from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pyodbc
import os

BRONZE_PATH = "/opt/airflow/data/bronze/customers.csv"
SILVER_PATH = "/opt/airflow/data/silver/customers_silver.csv"
GOLD_PATH = "/opt/airflow/data/gold/customers_gold.csv"

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=host.docker.internal,1433;"
    "DATABASE=Northwind;"
    "UID=sa;"
    "PWD=airflow;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
    "Timeout=60;"
)

def extract_customers():
    conn = pyodbc.connect(CONNECTION_STRING, autocommit=True)

    query = "SELECT * FROM Customers"
    df = pd.read_sql(query, conn)

    conn.close()

    os.makedirs(os.path.dirname(BRONZE_PATH), exist_ok=True)
    df.to_csv(BRONZE_PATH, index=False)

def transform_silver():
    df = pd.read_csv(BRONZE_PATH)

    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "", regex=False)
    )

    df = df.dropna(how="all")
    df = df[df["customerid"].notna()]

    os.makedirs(os.path.dirname(SILVER_PATH), exist_ok=True)
    df.to_csv(SILVER_PATH, index=False)

def build_gold():
    df = pd.read_csv(SILVER_PATH)

    df_gold = df[
        ["customerid", "companyname", "contactname", "country", "city"]
    ].copy()

    df_gold["country"] = df_gold["country"].str.upper()
    df_gold["is_usa"] = df_gold["country"] == "USA"

    os.makedirs(os.path.dirname(GOLD_PATH), exist_ok=True)
    df_gold.to_csv(GOLD_PATH, index=False)

with DAG(
    dag_id="northwind_customers_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["northwind", "bronze", "silver", "gold"],
) as dag:

    extract = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers
    )

    silver = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver
    )

    gold = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold
    )

    extract >> silver >> gold
