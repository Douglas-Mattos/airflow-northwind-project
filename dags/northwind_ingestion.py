from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

RAW_PATH = "/opt/airflow/data/raw/northwind"

def extract_customers():
    os.makedirs(RAW_PATH, exist_ok=True)

    data = {
        "CustomerID": ["ALFKI", "ANATR", "ANTON"],
        "CompanyName": [
            "Alfreds Futterkiste",
            "Ana Trujillo Emparedados",
            "Antonio Moreno Taquería"
        ],
        "Country": ["Germany", "Mexico", "Mexico"]
    }

    df = pd.DataFrame(data)
    df.to_csv(f"{RAW_PATH}/customers.csv", index=False)

with DAG(
    dag_id="northwind_ingestion_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["northwind", "bronze", "ingestion"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers
    )

    extract_task
