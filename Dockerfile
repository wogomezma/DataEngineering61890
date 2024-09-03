# Usa la imagen oficial de Apache Airflow
FROM apache/airflow:2.6.3-python3.10

# Instala las dependencias necesarias
RUN pip install --no-cache-dir pandas sodapy psycopg2-binary

# Copia los archivos del DAG y la llave
COPY entrega_3_data_engineering_walter_gomez_.py /opt/airflow/dags/
COPY key.json /opt/airflow/dags/

# Ejecuta el webserver de Airflow
CMD ["airflow", "webserver"]