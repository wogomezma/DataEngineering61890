from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sodapy import Socrata
import psycopg2
import os
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def load_env_variables():
    load_dotenv('/opt/airflow/dags/.env')
    return {
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

def send_alert_email(subject, body, from_email, to_email):
    message = Mail(
        from_email=from_email,
        to_emails=to_email,
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
        sg = SendGridAPIClient(env_variables['sendgrid_api_key'])
        response = sg.send(message)
        print(f"Email enviado, Status Code: {response.status_code}")
    except Exception as e:
        print(f"Error enviando email: {str(e)}")

def fetch_data_from_socrata(uri, token, min_api, error_api, from_email, to_email):
    try:
        client = Socrata(uri, token)
        results = 0
        offset = 0
        df = pd.DataFrame()

        while results != []:
            results = client.get("v8jr-kywh", limit=50000, offset=offset)
            data = pd.DataFrame.from_records(results)
            offset += 50000

            if results != []:
                df = pd.concat([df, data], axis=0)

        if df.shape[0] < min_api:
            send_alert_email("Alerta: Datos descargados insuficientes", f"Los datos descargados desde la API ({df.shape[0]}) son menores al mínimo requerido ({min_api}).", from_email, to_email)
        return df

    except Exception as e:
        if error_api:
            send_alert_email("Error descargando datos", f"Error al descargar datos de la API: {str(e)}", from_email, to_email)
        raise

def transform_data(df):
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

    return df

def load_data_to_redshift(df, keys):
    try:
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

            update_query = f"""
                UPDATE {keys['redshift_schema']}.daily_sales_data
                SET eds_activas = %s,
                    numero_de_ventas = %s,
                    vehiculos_atendidos = %s,
                    cantidad_volumen_suministrado = %s
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
                    row['date_update']
                )
                for index, row in df.iterrows()
            ]

            from psycopg2.extras import execute_values

            # Ejecutar actualización
            for value in values:
                cursor.execute(update_query, (
                    value[6],  # eds_activas
                    value[7],  # numero_de_ventas
                    value[8],  # vehiculos_atendidos
                    value[9],  # cantidad_volumen_suministrado
                    value[1],  # fecha_venta
                    value[2],  # anio_venta
                    value[3],  # mes_venta
                    value[4],  # dia_venta
                    value[5],  # latitud
                    value[5]   # longitud
                ))

            # Ejecutar inserción
            execute_values(cursor, insert_query, values)
            print("Datos cargados y actualizados exitosamente en Redshift.")

            # Alerta si los valores insertados son cero
            if len(values) == 0:
                send_alert_email(
                    "Alerta: No se insertaron datos",
                    "No se insertaron datos en la tabla de Redshift.",
                    keys['from_email'],
                    keys['to_email']
                )

    except psycopg2.Error as e:
        print(f"Error al conectar o cargar datos en Redshift: {e}")
        # Enviar alerta si hay un error de conexión
        send_alert_email(
            "Error al cargar datos en Redshift",
            f"Error al conectar o cargar datos en Redshift: {e}",
            keys['from_email'],
            keys['to_email']
        )
    finally:
        if conn:
            conn.close()

def ingest_data_to_redshift():
    keys = load_env_variables()
    df = fetch_data_from_socrata(keys['uri'], keys['token'], keys['min_api'], keys['error_api'], keys['from_email'], keys['to_email'])
    df = transform_data(df)
    load_data_to_redshift(df, keys)

# Configuración del DAG
dag = DAG(
    dag_id='daily_sales_data_ingestion',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
    },
    schedule_interval='@daily',
    catchup=False,
    description='DAG for daily ingestion of sales data into Redshift'
)

# Operador de Python para ejecutar la función
ingest_data_task = PythonOperator(
    task_id='ingest_data_to_redshift',
    python_callable=ingest_data_to_redshift,
    dag=dag
)
