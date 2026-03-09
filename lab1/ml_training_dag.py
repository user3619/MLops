from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, '/home/stepan/airflow/')

from dags.pipeline import pipeline
from dags.predict import predict

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='ml_training_and_prediction',
    default_args=default_args,
    description='A simple ML training and prediction DAG',
    catchup=False,
    tags=['ml', 'example']
) as dag:

    task_pipeline = PythonOperator(
        task_id='run_ml_pipeline',
        python_callable=pipeline,
    )

    task_predict = PythonOperator(
        task_id='run_prediction',
        python_callable=predict,
    )


    task_pipeline >> task_predict
