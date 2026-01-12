from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pyodbc
import pandas as pd
import os

def extract_customers():
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=host.docker.internal,1433;"
        "DATABASE=Northwind;"
        "UID=airflow_user;"
        "PWD=Airflow@123;"
        "TrustServerCertificate=yes;"
    )

    query = "SELECT * FROM Customers"
    df = pd.read_sql(query, conn)

    os.makedirs("/opt/airflow/data/bronze", exist_ok=True)
    df.to_csv("/opt/airflow/data/bronze/customers.csv", index=False)

    conn.close()

with DAG(
    dag_id="northwind_bronze_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["northwind", "bronze"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers
    )
