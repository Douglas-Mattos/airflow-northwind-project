# Airflow Northwind Pipeline

Pipeline de dados utilizando Apache Airflow com arquitetura Bronze, Silver e Gold.

## Tecnologias
- Apache Airflow
- Docker / Docker Compose
- Python
- Pandas
- SQL Server (Northwind)
- pyodbc

## Pipeline
- **Extract**: leitura da tabela Customers (SQL Server)
- **Silver**: padronização e limpeza
- **Gold**: seleção de colunas e enriquecimento

## Estrutura
- `dags/`: DAGs do Airflow
- `data/bronze`: dados brutos
- `data/silver`: dados tratados
- `data/gold`: dados prontos para consumo

## Execução
```bash
docker-compose up -d
