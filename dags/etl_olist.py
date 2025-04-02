from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Agregar la ruta de los scripts
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importar las funciones necesarias
from scripts.extract import extract_data
from scripts.load import load_data
from scripts.cleaning import cleaning_data
from scripts.transform import transform_data

# Definir los argumentos por defecto
default_args = {
    'owner': 'airflow',
}

# Definir el DAG
with DAG(
    dag_id='etl_airtravel',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@once',  # Periodicidad del DAG
    catchup=False,
    tags=['ETL', 'CSV', 'sqlite'],
) as dag:

    # Tarea de extracción
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    # Tarea de limpieza (Silver)
    cleaning = PythonOperator(
        task_id='cleaning_data',
        python_callable=cleaning_data
    )

    # Tarea de transformación (Gold)
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    # Tarea de carga
    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    # Definir el orden de ejecución
    extract >> cleaning >> transform >> load
