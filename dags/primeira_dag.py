from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="primeira_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # execução manual
    catchup=False,
    tags=["estudo", "fundamentos"],
) as dag:

    tarefa_inicio = BashOperator(
        task_id="inicio",
        bash_command="echo 'Airflow está rodando corretamente'",
    )

    tarefa_fim = BashOperator(
        task_id="fim",
        bash_command="echo 'Primeira DAG finalizada com sucesso'",
    )

    tarefa_inicio >> tarefa_fim
