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

#incremental load in bronze_layer
def load_incremental_to_postgres_from_s3():
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    for file_path in FILES:
        obj = s3_hook.get_key(file_path, bucket_name=BUCKET)
        df = pd.read_json(io.BytesIO(obj.get()["Body"].read()))

        table_name = file_path.split("/")[-1].replace(".json", "")
        existing_df = pg_hook.get_pandas_df(f"SELECT * FROM silver_layer.{table_name};")

        if not existing_df.empty:
            new_data = df[~df["id"].isin(existing_df["id"])]
        else:
            new_data = df

        if not new_data.empty:
            pg_hook.insert_rows(
                table=f"silver_layer.{table_name}",
                rows=new_data.values.tolist(),
                target_fields=new_data.columns.tolist()
            )
            print(f"✅ Inserted {len(new_data)} new rows into {table_name}")
        else:
            print(f"ℹ️ No new data for {table_name}")

DEFAULT_ARGS = {
    'start_date':datetime.utcnow() - timedelta(minutes=2),
    'owner':'airflow',
}

with DAG(
    dag_id='extract_transform',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
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