from datetime import datetime
from airflow import DAG, Dataset
from astro import sql as aql
from astro.sql.table import Table
from sqlalchemy import Column, Numeric, Date, String
from astro.files import get_file_list
from astro.files import File
from pandas import DataFrame
from airflow.decorators import task
import os

DB_CONN_ID = os.environ["DB_CONN_ID"]
FILE_CONN_ID = os.environ["FILE_CONN_ID"]


column_definitions = [
                Column("YEAR", Numeric),
                Column("QUARTER", Numeric),
                Column("MONTH", Numeric),
                Column("DAYOFMONTH", Numeric),
                Column("DAYOFWEEK", Numeric),
                Column("FLIGHTDATE", Date),
                Column("REPORTING_AIRLINE", String),
                Column("DOT_ID_REPORTING_AIRLINE", Numeric),
                Column("IATA_CODE_REPORTING_AIRLINE", String),
                Column("TAIL_NUMBER", String),
                Column("FLIGHT_NUMBER_REPORTING_AIRLINE", String),
                Column("ORIGINAIRPORTID", Numeric),
                Column("ORIGINAIRPORTSEQID", Numeric),
                Column("ORIGINCITYMARKETID", Numeric),
                Column("ORIGIN", String),
                Column("ORIGINCITYNAME", String),
                Column("ORIGINSTATE", String),
                Column("ORIGINSTATEFIPS", Numeric),
                Column("ORIGINSTATENAME", String),
                Column("ORIGINWAC", Numeric),
                Column("DESTAIRPORTID", Numeric),
                Column("DESTAIRPORTSEQID", Numeric),
                Column("DESTCITYMARKETID", Numeric),
                Column("DEST", String),
                Column("DESTCITYNAME", String),
                Column("DESTSTATE", String),
                Column("DESTSTATEFIPS", Numeric),
                Column("DESTSTATENAME", String),
                Column("DESTWAC", Numeric),
                Column("CRSDEPTIME", Numeric),
                Column("DEPTIME", Numeric),
                Column("DEPDELAY", Numeric),
                Column("DEPDELAYMINUTES", Numeric),
                Column("DEPDEL15", Numeric),
                Column("DEPARTUREDELAYGROUPS", Numeric),
                Column("DEPTIMEBLK", String),
                Column("TAXIOUT", Numeric),
                Column("WHEELSOFF", Numeric),
                Column("WHEELSON", Numeric),
                Column("TAXIIN", Numeric),
                Column("CRSARRTIME", Numeric),
                Column("ARRTIME", Numeric),
                Column("ARRDELAY", Numeric),
                Column("ARRDELAYMINUTES", Numeric),
                Column("ARRDEL15", Numeric),
                Column("ARRIVALDELAYGROUPS", Numeric),
                Column("ARRTIMEBLK", String),
                Column("CANCELLED", Numeric),
                Column("CANCELLATIONCODE", String),
                Column("DIVERTED", Numeric),
                Column("CRSELAPSEDTIME", Numeric),
                Column("ACTUALELAPSEDTIME", Numeric),
                Column("AIRTIME", Numeric),
                Column("FLIGHTS", Numeric),
                Column("DISTANCE", Numeric),
                Column("DISTANCEGROUP", Numeric),
                Column("CARRIERDELAY", Numeric),
                Column("WEATHERDELAY", Numeric),
                Column("NASDELAY", Numeric),
                Column("SECURITYDELAY", Numeric),
                Column("LATEAIRCRAFTDELAY", Numeric),
                Column("FIRSTDEPTIME", Numeric),
                Column("TOTALADDGTIME", Numeric),
                Column("LONGESTADDGTIME", Numeric),
                Column("DIVAIRPORTLANDINGS", Numeric),
                Column("DIVREACHEDDEST", Numeric),
                Column("DIVACTUALELAPSEDTIME", Numeric),
                Column("DIVARRDELAY", Numeric),
                Column("DIVDISTANCE", Numeric),
                Column("DIV1AIRPORT", String),
                Column("DIV1AIRPORTID", Numeric),
                Column("DIV1AIRPORTSEQID", Numeric),
                Column("DIV1WHEELSON", Numeric),
                Column("DIV1TOTALGTIME", Numeric),
                Column("DIV1LONGESTGTIME", Numeric),
                Column("DIV1WHEELSOFF", Numeric),
                Column("DIV1TAILNUM", String),
                Column("DIV2AIRPORT", String),
                Column("DIV2AIRPORTID", Numeric),
                Column("DIV2AIRPORTSEQID", Numeric),
                Column("DIV2WHEELSON", Numeric),
                Column("DIV2TOTALGTIME", Numeric),
                Column("DIV2LONGESTGTIME", Numeric),
                Column("DIV2WHEELSOFF", Numeric),
                Column("DIV2TAILNUM", String),
                Column("DIV3AIRPORT", String),
                Column("DIV3AIRPORTID", Numeric),
                Column("DIV3AIRPORTSEQID", Numeric),
                Column("DIV3WHEELSON", Numeric),
                Column("DIV3TOTALGTIME", Numeric),
                Column("DIV3LONGESTGTIME", Numeric),
                Column("DIV3WHEELSOFF", Numeric),
                Column("DIV3TAILNUM", String),
                Column("DIV4AIRPORT", String),
                Column("DIV4AIRPORTID", Numeric),
                Column("DIV4AIRPORTSEQID", Numeric),
                Column("DIV4WHEELSON", Numeric),
                Column("DIV4TOTALGTIME", Numeric),
                Column("DIV4LONGESTGTIME", Numeric),
                Column("DIV4WHEELSOFF", Numeric),
                Column("DIV4TAILNUM", String),
                Column("DIV5AIRPORT", String),
                Column("DIV5AIRPORTID", Numeric),
                Column("DIV5AIRPORTSEQID", Numeric),
                Column("DIV5WHEELSON", Numeric),
                Column("DIV5TOTALGTIME", Numeric),
                Column("DIV5LONGESTGTIME", Numeric),
                Column("DIV5WHEELSOFF", Numeric),
                Column("DIV5TAILNUM", String),
                Column("UNAMED", String)
            ]

flight_data_dataset = Dataset('s3://cosmicenergy-ml-public-datasets/flight_data')
flight_data_table_dataset = Dataset('postgresql://host.docker.internal/postgres/tmp_astro.flight_data')

dag = DAG(
    dag_id="3_load_files_to_table",
    start_date=datetime(2022, 10, 30),
    schedule=[flight_data_dataset],
    catchup=False,
)

with dag:

    @aql.transform(conn_id=DB_CONN_ID)
    def get_processed_files_list():
        return """
        SELECT * FROM tmp_astro.files_processed
        """

    file_list = get_file_list(path="s3://cosmicenergy-ml-public-datasets/flight_data/", conn_id=FILE_CONN_ID)

    @aql.dataframe
    def compare_lists(processed_files_list: DataFrame,all_files):
        all_files_list = []
        for file in all_files:
            all_files_list.append(file.path)
        difference = list(set(all_files_list).difference(set(processed_files_list['file_name'].values.tolist())))
        all_Files = []
        for file in difference:
            all_Files.append(File(path=file,conn_id=FILE_CONN_ID))
        print(all_Files)
        return all_Files

    unprocessed_files = compare_lists(get_processed_files_list(),file_list)

    load_new_files = aql.LoadFileOperator.partial(
        task_id="load_files",
        output_table=Table(
            "flight_data",
            conn_id=DB_CONN_ID,
            columns=column_definitions),
        max_active_tis_per_dag=4,
        if_exists='append',
        use_native_support=True,
    ).expand(input_file=unprocessed_files)

    @aql.dataframe
    def convert_file_list(file_list_in):
        file_list = []
        file_list.append(file_list_in.path)
        return file_list

    from psycopg2.extensions import adapt, register_adapter, AsIs
    def adapt_file(file):
        return AsIs(f"'{file.path}'")
    
    register_adapter(File,adapt_file)
    
    @aql.run_raw_sql(conn_id=DB_CONN_ID)
    def update_processed_file_list(file_name: File):
        return """
        INSERT INTO tmp_astro.files_processed (file_name) VALUES ({{file_name}});
        """
    
    @task(outlets = flight_data_table_dataset)
    def downstream_trigger():
        return None

    convert_file_list.expand(file_list_in = unprocessed_files) >> load_new_files >> update_processed_file_list.expand(file_name = unprocessed_files) >> downstream_trigger()

    #TODO - why does this break?
    #aql.cleanup()