import json
import logging
import os

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

logger = logging.getLogger(__name__)


def load_raw_data(**context):
    raw_path = "/data/cars/cars_full.json"
    
    if not os.path.exists(raw_path):
        logger.error(f"Raw data file not found: {raw_path}")
        raise FileNotFoundError(f"Raw data file not found: {raw_path}")
    
    logger.info(f"Loading raw data from {raw_path}")
    with open(raw_path, 'r') as f:
        cars_data = json.load(f)
    
    logger.info(f"Loaded {len(cars_data)} records")
    context['ti'].xcom_push(key='raw_data', value=cars_data)
    context['ti'].xcom_push(key='raw_path', value=raw_path)


def clean_cars_data(**context):
    ti = context['ti']
    cars_data = ti.xcom_pull(task_ids='load_raw_data', key='raw_data')
    
    if not cars_data:
        logger.error("No data received from previous task")
        return
    
    df = pd.DataFrame(cars_data)
    logger.info(f"Initial data shape: {df.shape}")
    
    # Статистика до очистки
    initial_rows = len(df)
    
    # 1. Удаление дубликатов
    df = df.drop_duplicates()
    logger.info(f"Removed {initial_rows - len(df)} duplicates")
    
    # 2. Удаление пропусков
    rows_before = len(df)
    df = df.dropna()
    logger.info(f"Removed {rows_before - len(df)} rows with missing values")
    
    # 3. Преобразование категориальных признаков
    fuel_type_mapping = {0: 0, 1: 1, 2: 2}
    transmission_mapping = {0: 0, 1: 1}
    
    if 'Fuel_type' in df.columns:
        df['Fuel_type'] = df['Fuel_type'].map(fuel_type_mapping).fillna(df['Fuel_type'])
        logger.info(f"Fuel_type unique values: {df['Fuel_type'].unique()}")
    
    if 'Transmission' in df.columns:
        df['Transmission'] = df['Transmission'].map(transmission_mapping).fillna(df['Transmission'])
        logger.info(f"Transmission unique values: {df['Transmission'].unique()}")
    
    # 4. Создание новых признаков
    if all(col in df.columns for col in ['x', 'y', 'z']):
        df['volume'] = df['x'] * df['y'] * df['z']
        logger.info("Added 'volume' feature")
    
    logger.info(f"Cleaned data shape: {df.shape}")
    
    # Сохраняем
    output_dir = "/data/cleaned"
    os.makedirs(output_dir, exist_ok=True)
    
    json_path = f"{output_dir}/cars_cleaned.json"
    csv_path = f"{output_dir}/cars_cleaned.csv"
    
    df.to_json(json_path, orient='records', indent=2)
    df.to_csv(csv_path, index=False)
    
    logger.info(f"Saved cleaned data to {json_path} and {csv_path}")
    
    # Передаем информацию дальше
    context['ti'].xcom_push(key='cleaned_path', value=json_path)
    context['ti'].xcom_push(key='cleaned_df_shape', value=list(df.shape))


def validate_cleaned_data(**context):
    """Проверяет качество очищенных данных."""
    ti = context['ti']
    cleaned_path = ti.xcom_pull(task_ids='clean_cars_data', key='cleaned_path')
    
    if not cleaned_path:
        logger.error("No cleaned data path found")
        return
    
    df = pd.read_json(cleaned_path)
    
    logger.info("=== Data Quality Report ===")
    logger.info(f"Total records: {len(df)}")
    logger.info(f"Total columns: {len(df.columns)}")
    logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
    
    # Проверка на пропуски
    missing = df.isnull().sum()
    if missing.sum() > 0:
        logger.warning(f"Missing values found:\n{missing[missing > 0]}")
    else:
        logger.info("No missing values found")
    
    # Проверка дубликатов
    duplicates = df.duplicated().sum()
    logger.info(f"Duplicates: {duplicates}")
    
    # Статистика по числовым колонкам
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        logger.info(f"Numeric columns summary:\n{df[numeric_cols].describe()}")
    
    logger.info("==========================")


with DAG(
    dag_id="03_data_cleaning",
    description="Cleans and validates car data",
    start_date=datetime(2026, 2, 3),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

    load_task = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_data,
    )

    clean_task = PythonOperator(
        task_id="clean_cars_data",
        python_callable=clean_cars_data,
    )

    validate_task = PythonOperator(
        task_id="validate_cleaned_data",
        python_callable=validate_cleaned_data,
    )

    load_task >> clean_task >> validate_task