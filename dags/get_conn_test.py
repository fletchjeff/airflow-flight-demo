from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks import GCSHook
from datetime import datetime
from airflow.operators.bash import BashOperator

from airflow.models import DAG
from airflow.decorators import task
from astro import sql as aql
import os

DB_CONN_ID = os.environ["DB_CONN_ID"]
BUCKET_NAME = os.environ["BUCKET_NAME"]

dag = DAG(
    dag_id="get_conn",
    start_date=datetime(2019, 1, 1),
    schedule="@once",
    catchup=False,
)

with dag:
    @task
    def get_conn():
        FILE_CONN_ID = os.environ["FILE_CONN_ID"]
        hook = S3Hook("minio_default")#"jf-xcom") #FILE_CONN_ID)
        #hook_info = hook.get_credentials
        #hook_info.
        #hook.get_bucket()
        #if not
        hook.create_bucket
        print(hook.check_for_bucket("jfletcher-datasets"))
        return hook.get_credentials().access_key

    get_conn()    
