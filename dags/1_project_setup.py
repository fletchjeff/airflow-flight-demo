from datetime import datetime
from airflow.operators.bash import BashOperator

from airflow.models import DAG
from airflow.decorators import task
from astro import sql as aql
import os

DB_CONN_ID = os.environ["DB_CONN_ID"]
#BUCKET_NAME = os.environ["BUCKET_NAME"]

dag = DAG(
    dag_id="1_project_setup",
    start_date=datetime(2019, 1, 1),
    schedule="@once",
    catchup=False,
)

with dag:
    @task
    def create_minio_buckets():
        from minio import Minio
        client = Minio("host.docker.internal:9000", "minioadmin", "minioadmin",secure=False)
        if not client.bucket_exists("local-xcom"):
            client.make_bucket("local-xcom")
        if not client.bucket_exists("cosmicenergy-ml-public-datasets"):
            client.make_bucket("cosmicenergy-ml-public-datasets")
            
    create_minio_buckets()

    @aql.run_raw_sql
    def create_schema():
        return """
        CREATE SCHEMA IF NOT EXISTS tmp_astro;
        """

    @aql.run_raw_sql
    def create_files_processed_table():
        return """
        DROP TABLE IF EXISTS tmp_astro.files_processed;
        CREATE TABLE tmp_astro.files_processed (file_name VARCHAR);        
        """

    @aql.run_raw_sql
    def create_flight_data_table():
        return """
            DROP TABLE IF EXISTS tmp_astro.flight_data;
            CREATE TABLE IF NOT EXISTS tmp_astro.flight_data
            (
                "YEAR" numeric,
                "QUARTER" numeric,
                "MONTH" numeric,
                "DAYOFMONTH" numeric,
                "DAYOFWEEK" numeric,
                "FLIGHTDATE" date,
                "REPORTING_AIRLINE" character varying COLLATE pg_catalog."default",
                "DOT_ID_REPORTING_AIRLINE" numeric,
                "IATA_CODE_REPORTING_AIRLINE" character varying COLLATE pg_catalog."default",
                "TAIL_NUMBER" character varying COLLATE pg_catalog."default",
                "FLIGHT_NUMBER_REPORTING_AIRLINE" character varying COLLATE pg_catalog."default",
                "ORIGINAIRPORTID" numeric,
                "ORIGINAIRPORTSEQID" numeric,
                "ORIGINCITYMARKETID" numeric,
                "ORIGIN" character varying COLLATE pg_catalog."default",
                "ORIGINCITYNAME" character varying COLLATE pg_catalog."default",
                "ORIGINSTATE" character varying COLLATE pg_catalog."default",
                "ORIGINSTATEFIPS" numeric,
                "ORIGINSTATENAME" character varying COLLATE pg_catalog."default",
                "ORIGINWAC" numeric,
                "DESTAIRPORTID" numeric,
                "DESTAIRPORTSEQID" numeric,
                "DESTCITYMARKETID" numeric,
                "DEST" character varying COLLATE pg_catalog."default",
                "DESTCITYNAME" character varying COLLATE pg_catalog."default",
                "DESTSTATE" character varying COLLATE pg_catalog."default",
                "DESTSTATEFIPS" numeric,
                "DESTSTATENAME" character varying COLLATE pg_catalog."default",
                "DESTWAC" numeric,
                "CRSDEPTIME" numeric,
                "DEPTIME" numeric,
                "DEPDELAY" numeric,
                "DEPDELAYMINUTES" numeric,
                "DEPDEL15" numeric,
                "DEPARTUREDELAYGROUPS" numeric,
                "DEPTIMEBLK" character varying COLLATE pg_catalog."default",
                "TAXIOUT" numeric,
                "WHEELSOFF" numeric,
                "WHEELSON" numeric,
                "TAXIIN" numeric,
                "CRSARRTIME" numeric,
                "ARRTIME" numeric,
                "ARRDELAY" numeric,
                "ARRDELAYMINUTES" numeric,
                "ARRDEL15" numeric,
                "ARRIVALDELAYGROUPS" numeric,
                "ARRTIMEBLK" character varying COLLATE pg_catalog."default",
                "CANCELLED" numeric,
                "CANCELLATIONCODE" character varying COLLATE pg_catalog."default",
                "DIVERTED" numeric,
                "CRSELAPSEDTIME" numeric,
                "ACTUALELAPSEDTIME" numeric,
                "AIRTIME" numeric,
                "FLIGHTS" numeric,
                "DISTANCE" numeric,
                "DISTANCEGROUP" numeric,
                "CARRIERDELAY" numeric,
                "WEATHERDELAY" numeric,
                "NASDELAY" numeric,
                "SECURITYDELAY" numeric,
                "LATEAIRCRAFTDELAY" numeric,
                "FIRSTDEPTIME" numeric,
                "TOTALADDGTIME" numeric,
                "LONGESTADDGTIME" numeric,
                "DIVAIRPORTLANDINGS" numeric,
                "DIVREACHEDDEST" numeric,
                "DIVACTUALELAPSEDTIME" numeric,
                "DIVARRDELAY" numeric,
                "DIVDISTANCE" numeric,
                "DIV1AIRPORT" character varying COLLATE pg_catalog."default",
                "DIV1AIRPORTID" numeric,
                "DIV1AIRPORTSEQID" numeric,
                "DIV1WHEELSON" numeric,
                "DIV1TOTALGTIME" numeric,
                "DIV1LONGESTGTIME" numeric,
                "DIV1WHEELSOFF" numeric,
                "DIV1TAILNUM" character varying COLLATE pg_catalog."default",
                "DIV2AIRPORT" character varying COLLATE pg_catalog."default",
                "DIV2AIRPORTID" numeric,
                "DIV2AIRPORTSEQID" numeric,
                "DIV2WHEELSON" numeric,
                "DIV2TOTALGTIME" numeric,
                "DIV2LONGESTGTIME" numeric,
                "DIV2WHEELSOFF" numeric,
                "DIV2TAILNUM" character varying COLLATE pg_catalog."default",
                "DIV3AIRPORT" character varying COLLATE pg_catalog."default",
                "DIV3AIRPORTID" numeric,
                "DIV3AIRPORTSEQID" numeric,
                "DIV3WHEELSON" numeric,
                "DIV3TOTALGTIME" numeric,
                "DIV3LONGESTGTIME" numeric,
                "DIV3WHEELSOFF" numeric,
                "DIV3TAILNUM" character varying COLLATE pg_catalog."default",
                "DIV4AIRPORT" character varying COLLATE pg_catalog."default",
                "DIV4AIRPORTID" numeric,
                "DIV4AIRPORTSEQID" numeric,
                "DIV4WHEELSON" numeric,
                "DIV4TOTALGTIME" numeric,
                "DIV4LONGESTGTIME" numeric,
                "DIV4WHEELSOFF" numeric,
                "DIV4TAILNUM" character varying COLLATE pg_catalog."default",
                "DIV5AIRPORT" character varying COLLATE pg_catalog."default",
                "DIV5AIRPORTID" numeric,
                "DIV5AIRPORTSEQID" numeric,
                "DIV5WHEELSON" numeric,
                "DIV5TOTALGTIME" numeric,
                "DIV5LONGESTGTIME" numeric,
                "DIV5WHEELSOFF" numeric,
                "DIV5TAILNUM" character varying COLLATE pg_catalog."default",
                "UNAMED" character varying COLLATE pg_catalog."default"
            ) 
        """

    delete_mlflow_backend = BashOperator(
        task_id='delete_mlflow_backend',
        bash_command='rm /usr/local/airflow/include/mlflow/mlflow_backend.db',
    )

    #TODO Clean up MLFlow
    # delete_mlflow_backend

    create_schema(conn_id=DB_CONN_ID) >> create_files_processed_table(conn_id=DB_CONN_ID) >> create_flight_data_table(conn_id=DB_CONN_ID)
