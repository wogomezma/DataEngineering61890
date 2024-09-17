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
        'error_db': os.getenv('ERRORDB', 'false').lower() == 'true'  
    }
    # Empujar las variables al XCom
    kwargs['ti'].xcom_push(key='env_variables', value=env_variables)

def fetch_data_from_socrata(**kwargs):
    env_variables = kwargs['ti'].xcom_pull(key='env_variables')
    uri = env_variables['uri']
    token = env_variables['token']
    min_api = env_variables['min_api']
    error_api = env_variables['error_api']
    from_email = env_variables['from_email']
    to_email = env_variables['to_email']
    
    try:
        client = Socrata(uri, token)
        results = []
        offset = 0
        df = pd.DataFrame()
        
        # Obtener las fechas de inicio y fin de los últimos 30 días
        fecha_fin = datetime.now()
        fecha_inicio = fecha_fin - timedelta(days=30)

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

        if df.shape[0] < min_api:
            kwargs['ti'].xcom_push(key='send_email', value=True)
            kwargs['ti'].xcom_push(key='email_subject', value="Alerta: Datos descargados insuficientes")
            kwargs['ti'].xcom_push(key='email_body', value=f"Los datos descargados desde la API ({df.shape[0]}) son menores al mínimo requerido ({min_api}).")
        
        kwargs['ti'].xcom_push(key='fetched_data', value=df.to_json())
    
    except Exception as e:
        if error_api:
            kwargs['ti'].xcom_push(key='send_email', value=True)
            kwargs['ti'].xcom_push(key='email_subject', value="Error descargando datos")
            kwargs['ti'].xcom_push(key='email_body', value=f"Error al descargar datos de la API: {str(e)}")
        raise

def transform_data(**kwargs):
    df = pd.read_json(kwargs['ti'].xcom_pull(key='fetched_data'))
    # Transformación de datos
    df['fecha_venta'] = pd.to_datetime(df['fecha_venta'])
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

    kwargs['ti'].xcom_push(key='transformed_data', value=df.to_json())

def load_data_to_redshift(**kwargs):
    # Recuperar las variables de entorno del XCom
    env_variables = kwargs['ti'].xcom_pull(key='env_variables')
    
    # Recuperar el DataFrame transformado de XCom
    df = pd.read_json(kwargs['ti'].xcom_pull(key='transformed_data'))
    
    # Establecer date_update a la fecha y hora actuales
    date_update = datetime.now()

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

            # Preparar las consultas de actualización e inserción
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

            # Preparar los valores para insertar/actualizar
            values = [
                (
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
                    date_update
                )
                for _, row in df.iterrows()
            ]

            # Ejecutar actualización antes de insertar
            for value in values:
                cursor.execute(update_query, (
                    value[6],  # eds_activas
                    value[7],  # numero_de_ventas
                    value[8],  # vehiculos_atendidos
                    value[9],  # cantidad_volumen_suministrado
                    date_update,  # fecha de actualización actual
                    value[1],  # fecha_venta
                    value[2],  # anio_venta
                    value[3],  # mes_venta
                    value[4],  # dia_venta
                    value[5],  # latitud
                    value[6]   # longitud
                ))

            # Insertar nuevos datos
            from psycopg2.extras import execute_values
            execute_values(cursor, insert_query, values)
            print("Datos cargados y actualizados exitosamente en Redshift.")
    
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
    send_email = kwargs['ti'].xcom_pull(key='send_email', default=False)
    if send_email:
        subject = kwargs['ti'].xcom_pull(key='email_subject')
        body = kwargs['ti'].xcom_pull(key='email_body')
        message = Mail(
            from_email=kwargs['ti'].xcom_pull(key='env_variables')['from_email'],
            to_emails=kwargs['ti'].xcom_pull(key='env_variables')['to_email'],
            subject=subject,
            html_content=f"""
                <html>
                    <body style="font-family: Arial, sans-serif; margin: 20px;">
                        <h2 style="color: #333;">{subject}</h2>
                        <p style="font-size: 14px; color: #555;">{body}</p>
                        <p style="font-size: 12px; color: #999;">Este mensaje fue generado automáticamente. Por favor, no responder.</p>
                    </body>
                </html>
            """
        )
        try:
            sg = SendGridAPIClient(kwargs['ti'].xcom_pull(key='env_variables')['sendgrid_api_key'])
            sg.send(message)
            print("Correo de alerta enviado con éxito.")
        except Exception as e:
            print(f"Error enviando correo de alerta: {str(e)}")


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
    schedule_interval='@daily',
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
    trigger_rule='all_done',  # Ejecutar si todas las tareas anteriores han finalizado, sin importar si fallaron o tuvieron éxito
    dag=dag
)

# Definición de dependencias entre tareas
load_env_task >> fetch_data_task >> transform_data_task >> load_data_task
[fetch_data_task, transform_data_task, load_data_task] >> send_email_task