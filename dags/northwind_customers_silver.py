from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

BRONZE_PATH = "/opt/airflow/data/bronze/customers.csv"
SILVER_PATH = "/opt/airflow/data/silver/customers_silver.csv"

def transform_customers():
    df = pd.read_csv(BRONZE_PATH)

    # Padronizar nomes de colunas
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # Remover linhas completamente vazias
    df = df.dropna(how="all")

    # Garantir que CustomerID não seja nulo
    df = df[df["customerid"].notna()]

    # Salvar na Silver
    os.makedirs(os.path.dirname(SILVER_PATH), exist_ok=True)
    df.to_csv(SILVER_PATH, index=False)

with DAG(
    dag_id="northwind_customers_silver",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["northwind", "silver", "customers"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_customers",
        python_callable=transform_customers
    )

    transform_task
