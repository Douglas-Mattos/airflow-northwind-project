from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json

BASE_PATH = "/opt/airflow/logs/northwind"

def ingest_northwind():
    os.makedirs(BASE_PATH, exist_ok=True)

    data = {
        "source": "northwind",
        "extracted_at": datetime.utcnow().isoformat(),
        "records": [
            {"order_id": 10248, "customer": "VINET", "total": 32.38},
            {"order_id": 10249, "customer": "TOMSP", "total": 11.61}
        ]
    }

    file_name = f"{BASE_PATH}/orders_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

    with open(file_name, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Arquivo gerado com sucesso: {file_name}")

with DAG(
    dag_id="northwind_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["northwind", "ingestion", "engineering"]
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_northwind_data",
        python_callable=ingest_northwind
    )

    ingest_task
