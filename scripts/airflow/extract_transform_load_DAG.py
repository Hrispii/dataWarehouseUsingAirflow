"""
------------------------------------------------------------------------------------------
--                            Airflow DAG: extract_transform                            --
------------------------------------------------------------------------------------------
-- This Airflow DAG orchestrates the full data ingestion and transformation pipeline    --
-- used in the project. It is responsible for pulling raw JSON datasets from AWS S3,    --
-- loading them into the Bronze Layer of the Yandex PostgreSQL warehouse, and then      --
-- triggering dbt to process the Silver and Gold layers.                                --
------------------------------------------------------------------------------------------
--                                                                                      --
--                                Features                                              --
-- • Reads multiple raw JSON files directly from an S3 bucket                           --
-- • Loads each dataset into the Bronze Layer (full refresh)                            --
-- • Uses Pandas and SQLAlchemy under the hood for reliable ingestion                   --
-- • Executes the dbt project after ingestion to build Silver and Gold tables           --
-- • Clean, modular structure aligned with Medallion Architecture                       --
------------------------------------------------------------------------------------------
--                                                                                      --
--                                Layer Purpose                                         --
-- Bronze Layer:                                                                        --
--     Stores raw, unmodified data exactly as it exists in S3.                          --
--     Acts as the foundational landing zone for the warehouse.                         --
--                                                                                      --
-- Silver Layer (dbt):                                                                  --
--     Cleans, joins, and validates Bronze data.                                        --
--     Produces trusted, analytics-ready datasets.                                      --
--                                                                                      --
-- Gold Layer (dbt):                                                                    --
--     Creates final business-level data marts for analytics and ML models.             --
------------------------------------------------------------------------------------------
--                                                                                      --
--                                Execution Flow                                        --
-- 1. Airflow fetches all JSON files from S3                                            --
-- 2. Files are loaded as DataFrames into bronze_layer.* tables in PostgreSQL           --
-- 3. After loading finishes, dbt models are executed                                   --
-- 4. dbt transforms Bronze → Silver → Gold                                             --
-- 5. Warehouse becomes fully refreshed end-to-end                                      --
------------------------------------------------------------------------------------------
--                                                                                      --
--                                Technical Overview                                    --
-- Language: Python + Airflow Operators                                                 --
-- Integrations: AWS S3Hook, PostgreSQLHook, dbt (via BashOperator)                     --
-- Load Type: Full refresh to Bronze, incremental logic available if needed             --
-- Purpose: Production-grade ingestion & transformation pipeline                        --
------------------------------------------------------------------------------------------
--                                                                                      --
--                                How This DAG Fits the Project                         --
-- This DAG is the entry point of the entire warehouse.                                 --
-- It guarantees that each day fresh data is pulled from cloud storage and transformed  --
-- using consistent, version-controlled SQL models.                                     --
-- The combination of Airflow + dbt provides a robust, modern stack suitable for        --
-- enterprise analytics, data science, and reporting needs.                             --
------------------------------------------------------------------------------------------
"""


from airflow import DAG
from airflow.operators.python import PythonOperator #type:ignore 
from airflow.operators.bash import BashOperator #type:ignore 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import io

FILES = [
    "bronze_layer/payments.json",
    "bronze_layer/customers.json",
    "bronze_layer/order_items.json",
    "bronze_layer/inventory.json",
    "bronze_layer/reviews.json",
    "bronze_layer/warehouses.json",
    "bronze_layer/shipments.json",
    "bronze_layer/orders.json",
    "bronze_layer/products.json"
]

BUCKET = "airflow-project-for-portfolio"
S3_CONN_ID = "aws_default"   
PG_CONN_ID = "yandex_cloud"    
TARGET_SCHEMA = "bronze_layer" 


# full load in bronze_layer
def load_to_postgres_from_s3():
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    
    for path in FILES:
        obj = s3_hook.get_key(path, BUCKET)
        df = pd.read_json(obj.get()["Body"])

        table_name = path.split("/")[-1].replace(".json", "")

        conn = pg_hook.get_sqlalchemy_engine()
        df.to_sql(
            name=table_name,
            con=conn,
            schema=TARGET_SCHEMA,
            if_exists="replace",
            index=False
        )

        print(f"✅ Uploaded {table_name} to {TARGET_SCHEMA}.{table_name}")

    print("🎉 All files uploaded successfully to Yandex PostgreSQL!")


DEFAULT_ARGS = {
    'start_date':datetime.utcnow() - timedelta(minutes=2),
    'owner':'airflow',
}

with DAG(
    dag_id='extract_transform',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    catchup=False,
    tags=['S3', 'Load'],
) as dag:
    
    load_bronze = PythonOperator(
        task_id="load_bronze_from_s3",
        python_callable=load_to_postgres_from_s3,
    )

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            "cd /opt/airflow/airflowProjectPortfolioDBT " 
            "&& dbt run" ),
    )


    load_bronze >> run_dbt_models
