#!/bin/bash

# Inicia Airflow en modo standalone
airflow standalone

# Realiza las migraciones de la base de datos
airflow db migrate

# Crea el usuario administrador
airflow users create \
    --role Admin \
    --username admin \
    --email admin \
    --firstname admin \
    --lastname admin

# Inicia el servidor web de Airflow
airflow webserver --port 8080 &

# Inicia el scheduler de Airflow
airflow scheduler
