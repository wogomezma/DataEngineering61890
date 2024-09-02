from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sodapy import Socrata
import json
import psycopg2

def load_data_to_redshift():
    with open('/opt/airflow/dags/keys.json') as f:
        keys = json.load(f)

    uri = keys['uri']
    token = keys['token']
    redshift_username = keys['redshift_username']
    redshift_password = keys['redshift_password']
    redshift_host = keys['redshift_host']
    redshift_port = keys['redshift_port']
    redshift_database = keys['redshift_database']
    redshift_schema = keys['redshift_schema']

    client = Socrata(uri, token)

    # Convert to pandas DataFrame
    results = 0
    offset = 0
    df = pd.DataFrame()

    while results != []:
        results = client.get("v8jr-kywh", limit=50000, offset=offset)

        data = pd.DataFrame.from_records(results)
        offset += 50000

        if results != []:
            df = pd.concat([df, data], axis=0)

    # Cambiar los tipos de datos
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

    # Crear un identificador único para cada venta
    df['id_venta'] = (
        df['fecha_venta'].astype(str) + '_' +
        df['anio_venta'].astype(str) + '_' +
        df['mes_venta'].astype(str) + '_' +
        df['dia_venta'].astype(str) + '_' +
        df['latitud'].astype(str) + '_' +
        df['longitud'].astype(str)
    ).apply(hash)

    # Conectar a Redshift usando psycopg2
    try:
        conn = psycopg2.connect(
            dbname=redshift_database,
            user=redshift_username,
            password=redshift_password,
            host=redshift_host,
            port=redshift_port
        )
        conn.autocommit = True

        with conn.cursor() as cursor:
            # Verificar si la tabla existe
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = '{redshift_schema}'
                AND table_name = 'daily_sales_data';
            """)

            table_exists = cursor.fetchone()[0] == 1

            if not table_exists:
                print("La tabla 'daily_sales_data' no existe en Redshift. Creando la tabla...")

                # Crear la tabla si no existe
                cursor.execute(f"""
                    CREATE TABLE {redshift_schema}.daily_sales_data (
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

            print("La tabla 'daily_sales_data' ahora existe.")

            # Preparar los datos para la inserción y actualización
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
                    row['cantidad_volumen_suministrado']
                )
                for index, row in df.iterrows()
            ]

            # Actualizar los registros que ya existen (sin date_update)
            update_query = f"""
                UPDATE {redshift_schema}.daily_sales_data
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

            for value in values:
                print(f"Updating record: {value}")
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

            # Insertar los registros nuevos
            insert_values = [
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

            insert_query = f"""
                INSERT INTO {redshift_schema}.daily_sales_data (
                    id_venta, fecha_venta, anio_venta, mes_venta, dia_venta, latitud, longitud,
                    eds_activas, numero_de_ventas, vehiculos_atendidos,
                    cantidad_volumen_suministrado, date_update
                ) VALUES %s;
            """

            from psycopg2.extras import execute_values

            # Insertar los datos que no existen
            execute_values(cursor, insert_query, insert_values)

            print("Datos cargados y actualizados exitosamente en Redshift.")

    except psycopg2.Error as e:
        print(f"Error al conectar o cargar datos en Redshift: {e}")
    finally:
        if conn:
            conn.close()

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
    description='DAG para la ingesta diaria de datos de ventas a Redshift'
)

# Operador de Python para ejecutar la función
ingest_data_task = PythonOperator(
    task_id='ingest_data_to_redshift',
    python_callable=load_data_to_redshift,
    dag=dag
)
