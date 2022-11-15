from airflow import DAG
from astro.sql.table import Table
from datetime import datetime
from astro import sql as aql
import mlflow
from airflow.decorators import dag, task
import pandas as pd
from smart_open import open
import boto3
from joblib import load
import os

DB_CONN_ID = os.environ["DB_CONN_ID"]
FILE_CONN_ID = os.environ["FILE_CONN_ID"]

dag = DAG(
    dag_id="6_deploy_model",
    start_date=datetime(2022, 10, 30),
    schedule_interval=None,
    catchup=False
    )

with dag:

    flight_table = Table(
            name="flight_data",
            conn_id=DB_CONN_ID,
        )

    @aql.transform
    def generate_new_predict_table(flight_table: Table):
        return """
        select "MONTH","DAYOFMONTH","DAYOFWEEK","REPORTING_AIRLINE","TAIL_NUMBER","ORIGIN","ORIGINCITYNAME","ORIGINSTATE","DEST","DESTCITYNAME","DESTSTATE",FLOOR("CRSDEPTIME"/100) AS "CRSDEPTIME",FLOOR("CRSARRTIME"/100) AS "CRSARRTIME","CRSELAPSEDTIME","DISTANCE" 
        from {{flight_table}} where "MONTH" = extract('month' from current_date) order by random() limit 100;
        """

    @task
    def get_lastest_model():
        mlflow.set_tracking_uri("http://host.docker.internal:5000")
        mlflow.set_experiment("train_model")
        client = mlflow.MlflowClient()
        latest_version = client.get_latest_versions(name='XGBClassifier')
        return (latest_version[0].tags['pipe'],latest_version[0].tags['ct'])

    @aql.dataframe(columns_names_capitalization="original")
    def predict_cancellations(new_predict_table: pd.DataFrame, paths: tuple):

        client = boto3.client('s3', 
            endpoint_url='http://host.docker.internal:9000/',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
        )
        bucketname = 'cosmicenergy-ml-public-datasets'
        with open (paths[0],'rb',transport_params={'client': client}) as f:
            pipe = load(f)
        with open (paths[1],'rb',transport_params={'client': client}) as f:
            ct = load(f)

        predict_data_clean = new_predict_table.dropna()

        predict_data_trans = ct.transform(predict_data_clean)
        predictions = pipe.predict(predict_data_trans)
        new_predict_table['CANCELLATION_PREDICTION'] = predictions        
        return new_predict_table        

    new_predict_table = generate_new_predict_table(flight_table)

    predict_cancellations(new_predict_table,get_lastest_model(),
        output_table=Table(
            name="CANCELLATION_PREDICTIONS",
            conn_id=DB_CONN_ID)
        )

    aql.cleanup()