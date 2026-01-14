# Airflow Northwind Pipeline (Bronze / Silver / Gold)

Pipeline de dados orquestrado com Apache Airflow (em Docker) para extrair dados do banco **Northwind (SQL Server)** e materializar datasets em camadas:
- **Bronze** (raw)
- **Silver** (padronização e limpeza)
- **Gold** (dataset pronto para consumo)

Este projeto foi desenvolvido como prática de Engenharia de Dados, cobrindo desde conectividade com SQL Server até orquestração e organização de camadas.

---

## Arquitetura

**Fonte (SQL Server / Northwind) → Airflow (DAG) → Arquivos em camadas**

- **Extract (Bronze):** extrai `Customers` do SQL Server e salva em CSV bruto
- **Transform (Silver):** padroniza nomes de colunas, remove linhas inválidas e aplica limpeza mínima
- **Build (Gold):** seleciona colunas relevantes e aplica enriquecimento simples (`is_usa`)

---

## Stack

- **Apache Airflow** (Docker / Docker Compose)
- **PostgreSQL** (metadados do Airflow)
- **SQL Server Express** (Northwind)
- **Python** + **Pandas**
- **pyodbc** (conector ODBC para SQL Server)

---

## Estrutura do repositório

```text
airflow_project/
├── dags/
│ └── northwind_customers_pipeline.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .gitignore
└── README.md


**Observação:**  
A pasta `data/` (camadas bronze, silver e gold) é criada em tempo de execução pelo Airflow e **não é versionada** no Git.

---

## Pré-requisitos

- Windows com **Docker Desktop** instalado e em execução
- **SQL Server Express** com o banco **Northwind**
- **SQL Server Management Studio (SSMS)**
- **Git**

---

## Como executar o projeto

1) Subir o Airflow com Docker

Na raiz do projeto:

```bash
docker compose up -d

Verificar se os containers estão em execução:

```bash
docker compose ps

2) Acessar a interface do Airflow

Este projeto utiliza mapeamento de porta externa 8082.

Acesse no navegador:
http://localhost:8082

Nota: internamente o Airflow utiliza a porta 8080, mas o Docker expõe a 8082 no host.

3) Executar a DAG

Na interface do Airflow:
1.Localize a DAG northwind_customers_pipeline
2.Ative a DAG (toggle ON)
3.Clique em Trigger DAG

```md
## Saídas geradas (camadas de dados)

Após a execução bem-sucedida da DAG, os arquivos são gerados dentro do container do Airflow:

* Bronze: /opt/airflow/data/bronze/customers.csv
* Silver: /opt/airflow/data/silver/customers_silver.csv
* Gold: /opt/airflow/data/gold/customers_gold.csv

Para listar os arquivos via terminal:
docker exec -it airflow_project-airflow-webserver-1 ls -R /opt/airflow/data


## Configuração do SQL Server (conectividade)

A DAG se conecta ao SQL Server via ODBC e TCP.

Configurações importantes:
* SQL Server acessível em: tcp:localhost,1433
* Autenticação SQL habilitada (ex.: usuário sa)
* Driver ODBC 18 configurado com:
	* Encrypt=no
	* TrustServerCertificate=yes


## Troubleshooting
Airflow não abre no navegador

* Confirme se os containers estão rodando:
docker compose ps

* Verifique se está acessando:
http://localhost:8082


Erro de conexão ODBC / timeout (HYT00)

* Verifique se o SQL Server responde no SSMS usando tcp:localhost,1433
* Confirme usuário e senha do login SQL
* Confirme que o Docker consegue acessar a porta 1433 do host

Alterações feitas no GitHub não aparecem localmente

Execute:
git pull


Próximos passos (Roadmap)

* Adicionar observabilidade nas DAGs (logs e métricas)
* Criar novas DAGs para outras tabelas do Northwind (Orders, Products)
* Persistir camada Gold em banco analítico
* Construir dashboard (Power BI) a partir da camada Gold



