from datetime import datetime
from airflow import DAG, Dataset
from astro import sql as aql
from astro.sql.table import Table
import sqlalchemy
from astro.files import get_file_list
from astro.files import File
from pandas import DataFrame
from airflow.decorators import task

FILE_CONN_ID = "minio_default"
DB_CONN_ID = "mypsql"

column_definitions = [
                sqlalchemy.Column("YEAR", sqlalchemy.Numeric),
                sqlalchemy.Column("QUARTER", sqlalchemy.Numeric),
                sqlalchemy.Column("MONTH", sqlalchemy.Numeric),
                sqlalchemy.Column("DAYOFMONTH", sqlalchemy.Numeric),
                sqlalchemy.Column("DAYOFWEEK", sqlalchemy.Numeric),
                sqlalchemy.Column("FLIGHTDATE", sqlalchemy.Date),
                sqlalchemy.Column("REPORTING_AIRLINE", sqlalchemy.String),
                sqlalchemy.Column("DOT_ID_REPORTING_AIRLINE", sqlalchemy.Numeric),
                sqlalchemy.Column("IATA_CODE_REPORTING_AIRLINE", sqlalchemy.String),
                sqlalchemy.Column("TAIL_NUMBER", sqlalchemy.String),
                sqlalchemy.Column("FLIGHT_NUMBER_REPORTING_AIRLINE", sqlalchemy.String),
                sqlalchemy.Column("ORIGINAIRPORTID", sqlalchemy.Numeric),
                sqlalchemy.Column("ORIGINAIRPORTSEQID", sqlalchemy.Numeric),
                sqlalchemy.Column("ORIGINCITYMARKETID", sqlalchemy.Numeric),
                sqlalchemy.Column("ORIGIN", sqlalchemy.String),
                sqlalchemy.Column("ORIGINCITYNAME", sqlalchemy.String),
                sqlalchemy.Column("ORIGINSTATE", sqlalchemy.String),
                sqlalchemy.Column("ORIGINSTATEFIPS", sqlalchemy.Numeric),
                sqlalchemy.Column("ORIGINSTATENAME", sqlalchemy.String),
                sqlalchemy.Column("ORIGINWAC", sqlalchemy.Numeric),
                sqlalchemy.Column("DESTAIRPORTID", sqlalchemy.Numeric),
                sqlalchemy.Column("DESTAIRPORTSEQID", sqlalchemy.Numeric),
                sqlalchemy.Column("DESTCITYMARKETID", sqlalchemy.Numeric),
                sqlalchemy.Column("DEST", sqlalchemy.String),
                sqlalchemy.Column("DESTCITYNAME", sqlalchemy.String),
                sqlalchemy.Column("DESTSTATE", sqlalchemy.String),
                sqlalchemy.Column("DESTSTATEFIPS", sqlalchemy.Numeric),
                sqlalchemy.Column("DESTSTATENAME", sqlalchemy.String),
                sqlalchemy.Column("DESTWAC", sqlalchemy.Numeric),
                sqlalchemy.Column("CRSDEPTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DEPTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DEPDELAY", sqlalchemy.Numeric),
                sqlalchemy.Column("DEPDELAYMINUTES", sqlalchemy.Numeric),
                sqlalchemy.Column("DEPDEL15", sqlalchemy.Numeric),
                sqlalchemy.Column("DEPARTUREDELAYGROUPS", sqlalchemy.Numeric),
                sqlalchemy.Column("DEPTIMEBLK", sqlalchemy.String),
                sqlalchemy.Column("TAXIOUT", sqlalchemy.Numeric),
                sqlalchemy.Column("WHEELSOFF", sqlalchemy.Numeric),
                sqlalchemy.Column("WHEELSON", sqlalchemy.Numeric),
                sqlalchemy.Column("TAXIIN", sqlalchemy.Numeric),
                sqlalchemy.Column("CRSARRTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("ARRTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("ARRDELAY", sqlalchemy.Numeric),
                sqlalchemy.Column("ARRDELAYMINUTES", sqlalchemy.Numeric),
                sqlalchemy.Column("ARRDEL15", sqlalchemy.Numeric),
                sqlalchemy.Column("ARRIVALDELAYGROUPS", sqlalchemy.Numeric),
                sqlalchemy.Column("ARRTIMEBLK", sqlalchemy.String),
                sqlalchemy.Column("CANCELLED", sqlalchemy.Numeric),
                sqlalchemy.Column("CANCELLATIONCODE", sqlalchemy.String),
                sqlalchemy.Column("DIVERTED", sqlalchemy.Numeric),
                sqlalchemy.Column("CRSELAPSEDTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("ACTUALELAPSEDTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("AIRTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("FLIGHTS", sqlalchemy.Numeric),
                sqlalchemy.Column("DISTANCE", sqlalchemy.Numeric),
                sqlalchemy.Column("DISTANCEGROUP", sqlalchemy.Numeric),
                sqlalchemy.Column("CARRIERDELAY", sqlalchemy.Numeric),
                sqlalchemy.Column("WEATHERDELAY", sqlalchemy.Numeric),
                sqlalchemy.Column("NASDELAY", sqlalchemy.Numeric),
                sqlalchemy.Column("SECURITYDELAY", sqlalchemy.Numeric),
                sqlalchemy.Column("LATEAIRCRAFTDELAY", sqlalchemy.Numeric),
                sqlalchemy.Column("FIRSTDEPTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("TOTALADDGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("LONGESTADDGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIVAIRPORTLANDINGS", sqlalchemy.Numeric),
                sqlalchemy.Column("DIVREACHEDDEST", sqlalchemy.Numeric),
                sqlalchemy.Column("DIVACTUALELAPSEDTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIVARRDELAY", sqlalchemy.Numeric),
                sqlalchemy.Column("DIVDISTANCE", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV1AIRPORT", sqlalchemy.String),
                sqlalchemy.Column("DIV1AIRPORTID", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV1AIRPORTSEQID", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV1WHEELSON", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV1TOTALGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV1LONGESTGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV1WHEELSOFF", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV1TAILNUM", sqlalchemy.String),
                sqlalchemy.Column("DIV2AIRPORT", sqlalchemy.String),
                sqlalchemy.Column("DIV2AIRPORTID", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV2AIRPORTSEQID", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV2WHEELSON", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV2TOTALGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV2LONGESTGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV2WHEELSOFF", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV2TAILNUM", sqlalchemy.String),
                sqlalchemy.Column("DIV3AIRPORT", sqlalchemy.String),
                sqlalchemy.Column("DIV3AIRPORTID", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV3AIRPORTSEQID", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV3WHEELSON", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV3TOTALGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV3LONGESTGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV3WHEELSOFF", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV3TAILNUM", sqlalchemy.String),
                sqlalchemy.Column("DIV4AIRPORT", sqlalchemy.String),
                sqlalchemy.Column("DIV4AIRPORTID", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV4AIRPORTSEQID", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV4WHEELSON", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV4TOTALGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV4LONGESTGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV4WHEELSOFF", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV4TAILNUM", sqlalchemy.String),
                sqlalchemy.Column("DIV5AIRPORT", sqlalchemy.String),
                sqlalchemy.Column("DIV5AIRPORTID", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV5AIRPORTSEQID", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV5WHEELSON", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV5TOTALGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV5LONGESTGTIME", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV5WHEELSOFF", sqlalchemy.Numeric),
                sqlalchemy.Column("DIV5TAILNUM", sqlalchemy.String),
                sqlalchemy.Column("UNAMED", sqlalchemy.String)
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