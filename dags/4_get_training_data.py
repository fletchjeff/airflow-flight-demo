from datetime import datetime
from airflow import DAG, Dataset
from astro import sql as aql
from astro.sql.table import Table
import sqlalchemy
from astro.files import get_file_list
from astro.files import File
from pandas import DataFrame
from airflow.decorators import task
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
import scipy
from joblib import dump, load
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import mlflow
import os

DB_CONN_ID = os.environ["DB_CONN_ID"]
FILE_CONN_ID = os.environ["FILE_CONN_ID"]

flight_data_table_dataset = Dataset('postgresql://host.docker.internal/postgres/tmp_astro.flight_data')     
model_training_data_latest_dataset =  Dataset('postgresql://host.docker.internal/postgres/tmp_astro.model_training_data_latest')  

dag = DAG(
    dag_id="4_get_training_data",
    start_date=datetime(2022, 10, 30),
    schedule=[flight_data_table_dataset],
    catchup=False,
)

with dag:
    
    @aql.run_raw_sql
    def create_training_data_table_script(table: Table):
        return """
            CREATE TABLE IF NOT EXISTS {{table}}
            (
                "MONTH" numeric,
                "DAYOFMONTH" numeric,
                "DAYOFWEEK" numeric,
                "REPORTING_AIRLINE" character varying COLLATE pg_catalog."default",
                "TAIL_NUMBER" character varying COLLATE pg_catalog."default",
                "ORIGIN" character varying COLLATE pg_catalog."default",
                "ORIGINCITYNAME" character varying COLLATE pg_catalog."default",
                "ORIGINSTATE" character varying COLLATE pg_catalog."default",
                "DEST" character varying COLLATE pg_catalog."default",
                "DESTCITYNAME" character varying COLLATE pg_catalog."default",
                "DESTSTATE" character varying COLLATE pg_catalog."default",
                "CRSDEPTIME" numeric,
                "CRSARRTIME" numeric,
                "CRSELAPSEDTIME" numeric,
                "DISTANCE" numeric,
                "CANCELLED" numeric
            ) 
        """


    training_data_table = Table(name="model_training_data_latest", conn_id=DB_CONN_ID)
    create_training_data_table = create_training_data_table_script(training_data_table)

    @aql.transform(conn_id=DB_CONN_ID)
    def get_cancelled_flights(flight_data_table: Table):
        return """
        select "MONTH","DAYOFMONTH","DAYOFWEEK","REPORTING_AIRLINE","TAIL_NUMBER","ORIGIN","ORIGINCITYNAME","ORIGINSTATE","DEST","DESTCITYNAME","DESTSTATE",FLOOR("CRSDEPTIME"/100) AS "CRSDEPTIME",FLOOR("CRSARRTIME"/100) AS "CRSARRTIME","CRSELAPSEDTIME","DISTANCE","CANCELLED" from {{flight_data_table}}
        WHERE "CANCELLED" = 1
        """
    
    @aql.transform(conn_id=DB_CONN_ID)
    def get_normal_flights(cancelled_flight_count, flight_data_table: Table):
        return """
        select "MONTH","DAYOFMONTH","DAYOFWEEK","REPORTING_AIRLINE","TAIL_NUMBER","ORIGIN","ORIGINCITYNAME","ORIGINSTATE","DEST","DESTCITYNAME","DESTSTATE",FLOOR("CRSDEPTIME"/100) AS "CRSDEPTIME",FLOOR("CRSARRTIME"/100) AS "CRSARRTIME","CRSELAPSEDTIME","DISTANCE","CANCELLED" from {{flight_data_table}} WHERE "CANCELLED" = 0 ORDER BY RANDOM() limit {{cancelled_flight_count}}
        """

    get_count_sql =  """
        select count(*) from tmp_astro.flight_data WHERE "CANCELLED" = 1
        """

    flight_data_table = Table(
            "flight_data",
            conn_id=DB_CONN_ID
        )

    cancelled_flights = get_cancelled_flights(flight_data_table)
    normal_flights = get_normal_flights(cancelled_flights['output_table_row_count'],flight_data_table)


    append_normal = aql.append(
        task_id="append_normal",
        target_table=training_data_table,
        source_table=normal_flights,
    )

    append_cancelled = aql.append(
        task_id="append_cancelled",
        target_table=training_data_table,
        source_table=cancelled_flights,
    )

    @task(outlets = model_training_data_latest_dataset)
    def downstream_trigger():
        return None

    create_training_data_table >> cancelled_flights 

    append_normal >> downstream_trigger()


    
    aql.cleanup()