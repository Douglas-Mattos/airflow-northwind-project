from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

SILVER_PATH = "/opt/airflow/data/silver/customers_silver.csv"
GOLD_PATH = "/opt/airflow/data/gold/customers_gold.csv"

def build_customers_gold():
    df = pd.read_csv(SILVER_PATH)

    # Selecionar colunas relevantes para negócio
    df_gold = df[
        [
            "customerid",
            "companyname",
            "contactname",
            "country",
            "city"
        ]
    ]

    # Normalizar país
    df_gold["country"] = df_gold["country"].str.upper()

    # Criar flag simples de exemplo
    df_gold["is_usa"] = df_gold["country"] == "USA"

    os.makedirs(os.path.dirname(GOLD_PATH), exist_ok=True)
    df_gold.to_csv(GOLD_PATH, index=False)

with DAG(
    dag_id="northwind_customers_gold",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["northwind", "gold", "customers"],
) as dag:

    build_gold = PythonOperator(
        task_id="build_customers_gold",
        python_callable=build_customers_gold
    )

    build_gold
