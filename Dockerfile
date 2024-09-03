# Utiliza una imagen base de Python con Airflow
FROM apache/airflow:2.6.3-python3.10

# Instala las dependencias necesarias
RUN pip install --no-cache-dir pandas sodapy psycopg2-binary

# Copia los archivos del DAG y keys.json
COPY entrega_3_data_engineering_walter_gomez_.py /opt/airflow/dags/entrega_3_data_engineering_walter_gomez_.py
COPY key.json /opt/airflow/dags/key.json

# Configura Airflow para que utilice la entrada predeterminada
CMD ["airflow", "webserver"]