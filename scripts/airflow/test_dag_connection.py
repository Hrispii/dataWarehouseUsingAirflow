from airflow import DAG
from airflow.operators.python import PythonOperator #type:ignore 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook #type:ignore
from datetime import datetime, timedelta
import pandas as pd

def read_s3_tables():
    hook = S3Hook(aws_conn_id="aws_default")
    bucket_name = "airflow-project-for-portfolio"

    files = [
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

    dataframes = {}

    for path in files:
        obj = hook.get_key(path, bucket_name)
        df = pd.read_json(obj.get()['Body'])
        table_name = path.split("/")[-1].replace(".json", "")
        dataframes[table_name] = df
        print(f"✅ Loaded {table_name}, rows: {len(df)}")

    return dataframes


DEFAULT_ARGS = {
    'start_date':datetime.utcnow() - timedelta(minutes=2),
    'owner':'airflow',
}

with DAG(
    dag_id='TEST_load_bronze',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=['S3', 'Test'],
) as dag:
    
    read_task = PythonOperator(
        task_id="read_json_from_s3",
        python_callable=read_s3_tables
    )
