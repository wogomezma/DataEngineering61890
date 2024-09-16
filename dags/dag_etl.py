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
        sg = SendGridAPIClient(os.getenv('TOKEN2'))
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
    # Transformación de datos
    df['fecha_venta'] = pd.to_datetime(df['fecha_venta'])
    # Rest of the transformations
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
            # Check for table existence and create if needed

            # Update and insert logic
            from psycopg2.extras import execute_values

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
                    value[6]   # longitud
                ))

            execute_values(cursor, insert_query, values)
            print("Datos cargados y actualizados exitosamente en Redshift.")

            if len(values) < keys['min_db']:
                send_alert_email("Alerta: Datos insertados insuficientes", f"Los datos insertados en la base de datos ({len(values)}) son menores al mínimo requerido ({keys['min_db']}).", keys['from_email'], keys['to_email'])

    except psycopg2.Error as e:
        if keys['error_db']:
            send_alert_email("Error al cargar datos en Redshift", f"Error al conectar o cargar datos en Redshift: {str(e)}", keys['from_email'], keys['to_email'])
        raise
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
