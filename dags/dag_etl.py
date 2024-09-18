from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sodapy import Socrata
import psycopg2
import os
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Función para cargar variables de entorno
def load_env_variables(**kwargs):
    load_dotenv('/opt/airflow/dags/.env')
    env_variables = {
        'uri': os.getenv('URI'),
        'token': os.getenv('TOKEN'),
        'redshift_username': os.getenv('REDSHIFT_USERNAME'),
        'redshift_password': os.getenv('REDSHIFT_PASSWORD'),
        'redshift_host': os.getenv('REDSHIFT_HOST'),
        'redshift_port': os.getenv('REDSHIFT_PORT'),
        'redshift_database': os.getenv('REDSHIFT_DATABASE'),
        'redshift_schema': os.getenv('REDSHIFT_SCHEMA'),
        'sendgrid_api_key': os.getenv('TOKEN2'),
        'from_email': os.getenv('FROM_EMAIL'),
        'to_email': os.getenv('TO_EMAIL'),
        'min_api': int(os.getenv('MINAPI', 0)), 
        'min_db': int(os.getenv('MINDB', 0)),  
        'error_api': os.getenv('ERRORAPI', 'false').lower() == 'true',  
        'error_db': os.getenv('ERRORDB', 'false').lower() == 'true',
        'daysdata': int(os.getenv('DAYSDATA', 30)) 
    }
    # Empujar las variables al XCom
    kwargs['ti'].xcom_push(key='env_variables', value=env_variables)

def fetch_data_from_socrata(**kwargs):
    env_variables = kwargs['ti'].xcom_pull(key='env_variables')
    uri = env_variables['uri']
    token = env_variables['token']
    min_api = env_variables['min_api']
    daysdata = env_variables['daysdata']
    
    try:
        client = Socrata(uri, token)
        results = []
        offset = 0
        df = pd.DataFrame()
        
        # Obtener las fechas de inicio y fin de los últimos 'daysdata' días
        fecha_fin = datetime.now()
        fecha_inicio = fecha_fin - timedelta(days=daysdata)

        # Formato de las fechas para la consulta
        fecha_inicio_str = fecha_inicio.strftime("%Y-%m-%dT%H:%M:%S")
        fecha_fin_str = fecha_fin.strftime("%Y-%m-%dT%H:%M:%S")    

        while True:
            results = client.get("v8jr-kywh", limit=50000, offset=offset, where=f"fecha_venta between '{fecha_inicio_str}' and '{fecha_fin_str}'")
            if not results:
                break
            data = pd.DataFrame.from_records(results)
            df = pd.concat([df, data], ignore_index=True)
            offset += 50000

        # Verificar si los datos descargados son None o insuficientes
        if df is None or df.empty or df.shape[0] <= min_api:  # Cambiado a <= para incluir el caso de min_api+1
            # Establecer la necesidad de enviar un correo de alerta
            kwargs['ti'].xcom_push(key='send_email', value=True)
            kwargs['ti'].xcom_push(key='email_subject', value="Alerta: Datos descargados insuficientes")
            kwargs['ti'].xcom_push(key='email_body', value=f"Los datos descargados desde la API ({df.shape[0] if df is not None else 0}) son menores o iguales al mínimo requerido ({min_api}).")
            
            # Lanzar excepción para detener el DAG
            raise ValueError(f"Datos descargados insuficientes o nulos: {df.shape[0] if df is not None else 0} datos, se requieren más de {min_api}.")

        # Continuar con la ejecución normal si hay suficientes datos
        kwargs['ti'].xcom_push(key='fetched_data', value=df.to_json())

    except Exception as e:
        # Asegurar que cualquier error sea manejado y registrado
        kwargs['ti'].xcom_push(key='send_email', value=True)
        kwargs['ti'].xcom_push(key='email_subject', value="Error descargando datos")
        kwargs['ti'].xcom_push(key='email_body', value=f"Error al descargar datos de la API: {str(e)}")
        raise  # Asegurar que el error se propague para detener el DAG



def transform_data(**kwargs):
    df = pd.read_json(kwargs['ti'].xcom_pull(key='fetched_data'))
    # Transformación de datos
    df['fecha_venta'] = pd.to_datetime(df['fecha_venta']).dt.date
    df['anio_venta'] = df['anio_venta'].astype('int64')
    df['mes_venta'] = df['mes_venta'].astype('int64')
    df['dia_venta'] = df['dia_venta'].astype('int64')
    df['latitud'] = df['latitud'].astype('float')
    df['longitud'] = df['longitud'].astype('float')
    df['eds_activas'] = df['eds_activas'].astype('int64')
    df['numero_de_ventas'] = df['numero_de_ventas'].astype('int64')
    df['vehiculos_atendidos'] = df['vehiculos_atendidos'].astype('int64')
    df['cantidad_volumen_suministrado'] = df['cantidad_volumen_suministrado'].astype('float')
    df['date_update'] = pd.Timestamp(datetime.now())
    df['id_venta'] = (
        df['fecha_venta'].astype(str) + '_' +
        df['anio_venta'].astype(str) + '_' +
        df['mes_venta'].astype(str) + '_' +
        df['dia_venta'].astype(str) + '_' +
        df['latitud'].astype(str) + '_' +
        df['longitud'].astype(str)
    ).apply(hash)

    # Guardar el DataFrame transformado en el XCom
    kwargs['ti'].xcom_push(key='transformed_data', value=df.to_json())


def load_data_to_redshift(**kwargs):
    # Recuperar las variables de entorno del XCom
    env_variables = kwargs['ti'].xcom_pull(key='env_variables')
    
    # Recuperar el DataFrame transformado de XCom
    df = pd.read_json(kwargs['ti'].xcom_pull(key='transformed_data'))
    
    # Asegurarse de que 'fecha_venta' esté en el tipo correcto
    df['fecha_venta'] = pd.to_datetime(df['fecha_venta']).dt.date

    # Asegurarse de que 'date_update' esté en el tipo correcto
    df['date_update'] = pd.to_datetime(df['date_update']).dt.date
    
    # Obtener las variables de entorno cargadas
    keys = env_variables
    
    try:
        # Conexión a la base de datos Redshift
        conn = psycopg2.connect(
            dbname=keys['redshift_database'],
            user=keys['redshift_username'],
            password=keys['redshift_password'],
            host=keys['redshift_host'],
            port=keys['redshift_port']
        )
        conn.autocommit = True

        with conn.cursor() as cursor:
            # Verificar si la tabla existe
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = '{keys['redshift_schema']}'
                AND table_name = 'daily_sales_data';
            """)
            table_exists = cursor.fetchone()[0] == 1

            # Crear la tabla si no existe
            if not table_exists:
                cursor.execute(f"""
                    CREATE TABLE {keys['redshift_schema']}.daily_sales_data (
                        id_venta BIGINT PRIMARY KEY,
                        fecha_venta DATE,
                        anio_venta INT,
                        mes_venta INT,
                        dia_venta INT,
                        latitud FLOAT,
                        longitud FLOAT,
                        eds_activas INT,
                        numero_de_ventas INT,
                        vehiculos_atendidos INT,
                        cantidad_volumen_suministrado FLOAT,
                        date_update TIMESTAMP
                    );
                """)

            # Consultas preparadas para actualizar e insertar
            update_query = f"""
                UPDATE {keys['redshift_schema']}.daily_sales_data
                SET eds_activas = %s,
                    numero_de_ventas = %s,
                    vehiculos_atendidos = %s,
                    cantidad_volumen_suministrado = %s,
                    date_update = %s
                WHERE fecha_venta = %s
                AND anio_venta = %s
                AND mes_venta = %s
                AND dia_venta = %s
                AND latitud = %s
                AND longitud = %s;
            """

            insert_query = f"""
                INSERT INTO {keys['redshift_schema']}.daily_sales_data (
                    id_venta, fecha_venta, anio_venta, mes_venta, dia_venta, latitud, longitud,
                    eds_activas, numero_de_ventas, vehiculos_atendidos,
                    cantidad_volumen_suministrado, date_update
                ) VALUES %s;
            """

            # Contadores para actualizaciones e inserciones
            update_count = 0
            insert_count = 0

            # Preparar los valores para insertar/actualizar
            values_to_insert = []  # Para guardar los valores que se deben insertar
            for index, row in df.iterrows():
                # Comprobar si la fila existe para ser actualizada
                cursor.execute(update_query, (
                    row['eds_activas'],
                    row['numero_de_ventas'],
                    row['vehiculos_atendidos'],
                    row['cantidad_volumen_suministrado'],
                    row['date_update'],
                    row['fecha_venta'],
                    row['anio_venta'],
                    row['mes_venta'],
                    row['dia_venta'],
                    row['latitud'],
                    row['longitud']
                ))

                # Incrementar el contador de actualizaciones si se ha actualizado alguna fila
                if cursor.rowcount > 0:
                    update_count += 1
                else:
                    # Si no se actualizó ninguna fila, prepararla para inserción
                    values_to_insert.append((
                        row['id_venta'],
                        row['fecha_venta'],
                        row['anio_venta'],
                        row['mes_venta'],
                        row['dia_venta'],
                        row['latitud'],
                        row['longitud'],
                        row['eds_activas'],
                        row['numero_de_ventas'],
                        row['vehiculos_atendidos'],
                        row['cantidad_volumen_suministrado'],
                        row['date_update']
                    ))

            # Realizar inserciones solo para las filas que no se actualizaron
            if values_to_insert:
                from psycopg2.extras import execute_values
                execute_values(cursor, insert_query, values_to_insert)
                insert_count = len(values_to_insert)

            print(f"Datos cargados y actualizados exitosamente en Redshift. Actualizaciones: {update_count}, Inserciones: {insert_count}.")

            # Guardar los contadores en XCom para su uso en la tarea de envío de correo
            kwargs['ti'].xcom_push(key='update_count', value=update_count)
            kwargs['ti'].xcom_push(key='insert_count', value=insert_count)

    except psycopg2.Error as e:
        # Manejar errores y enviar un correo de alerta si hay problemas
        kwargs['ti'].xcom_push(key='send_email', value=True)
        kwargs['ti'].xcom_push(key='email_subject', value="Error al cargar datos en Redshift")
        kwargs['ti'].xcom_push(key='email_body', value=f"Error al conectar o cargar datos en Redshift: {e}")
        raise
    finally:
        if conn:
            conn.close()
    



def send_alert_email(**kwargs):
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    
    # Obtener el estado de todas las tareas del DAG, excluyendo 'send_alert_email'
    task_instances = dag_run.get_task_instances()
    tasks_status = "\n".join([f"Task {t.task_id}: {t.state}" for t in task_instances if t.task_id != 'send_alert_email'])
    
    # Comprobar si hay tareas fallidas
    failed_tasks = [t.task_id for t in task_instances if t.state == 'failed' and t.task_id != 'send_alert_email']
    send_email = len(failed_tasks) > 0 or ti.xcom_pull(key='send_email', default=False)
    
    # Obtener el número de datos actualizados e insertados
    update_count = ti.xcom_pull(key='update_count', default=0)
    insert_count = ti.xcom_pull(key='insert_count', default=0)
    
    # Configurar el asunto y el cuerpo del correo
    if send_email:
        subject = "DAG Execution Summary: Errors Detected" if failed_tasks else ti.xcom_pull(key='email_subject', default="DAG Execution Summary")
        body = ti.xcom_pull(key='email_body', default="No specific alerts. Here is the task summary:\n\n") + tasks_status
        # Añadir la información de actualización e inserción
        body += f"\nDatos actualizados: {update_count}, Datos insertados: {insert_count}."
    else:
        subject = "DAG Execution Summary"
        body = "All tasks executed successfully.\n\n" + tasks_status
        # Añadir la información de actualización e inserción
        body += f"\nDatos actualizados: {update_count}, Datos insertados: {insert_count}."

    # Preparar el mensaje de correo
    env_variables = ti.xcom_pull(key='env_variables')
    from_email = env_variables.get('from_email')
    to_email = env_variables.get('to_email')
    sendgrid_api_key = env_variables.get('sendgrid_api_key')

    # Crear el contenido HTML del correo
    html_content = """
        <html>
            <body style="font-family: Arial, sans-serif; margin: 20px;">
                <h2 style="color: #333;">{subject}</h2>
                <p style="font-size: 14px; color: #555;">{body}</p>
                <p style="font-size: 12px; color: #999;">Este mensaje fue generado automáticamente. Por favor, no responder.</p>
            </body>
        </html>
    """.format(subject=subject, body=body.replace('\n', '<br>'))
    
    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        html_content=html_content
    )
    
    # Enviar el correo usando SendGrid
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print("Correo de alerta enviado con éxito.")
        print(f"SendGrid response: {response.status_code}, {response.body}, {response.headers}")
    except Exception as e:
        print(f"Error enviando correo de alerta: {str(e)}")
    finally:
        print("Finalización de la tarea send_alert_email.")
        return "Tarea completada con éxito"



# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='daily_sales_data_ingestion',
    default_args=default_args,
    schedule_interval='15 0 * * *',  # Ejecutar todos los días a las 00:15 UTC
    catchup=True,  # Permitir mecanismo de backfill
    description='DAG for daily ingestion of sales data into Redshift'
)

# Definición de tareas

# Cargar variables de entorno
load_env_task = PythonOperator(
    task_id='load_env_variables',
    python_callable=load_env_variables,
    provide_context=True,
    dag=dag
)

# Descargar datos desde Socrata
fetch_data_task = PythonOperator(
    task_id='fetch_data_from_socrata',
    python_callable=fetch_data_from_socrata,
    provide_context=True,
    dag=dag
)

# Transformar datos
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

# Cargar datos a Redshift
load_data_task = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_data_to_redshift,
    provide_context=True,
    dag=dag
)

# Enviar correo de alerta si es necesario
send_email_task = PythonOperator(
    task_id='send_alert_email',
    python_callable=send_alert_email,
    provide_context=True,
    trigger_rule='all_done',
    dag=dag
)

# Definición de dependencias entre tareas
load_env_task >> fetch_data_task >> transform_data_task >> load_data_task >> send_email_task