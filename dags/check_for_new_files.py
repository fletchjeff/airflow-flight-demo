from airflow import DAG
from airflow.decorators import task
import pendulum
import gzip
import json

from airflow.decorators import dag, task
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def update_files_in_s3():
    @task
    def get_files_in_s3():
        import boto3
        conn = boto3.client('s3',
            endpoint_url='http://host.docker.internal:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            aws_session_token=None,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False
        )
        s3_list = []
        if 'Contents' in conn.list_objects_v2(Bucket="cosmicenergy-ml-public-datasets",Prefix='flight_data'):
            for key in conn.list_objects_v2(Bucket="cosmicenergy-ml-public-datasets",Prefix='flight_data')['Contents']:
                if (len(key['Key'].split("/")) > 1) and ("On_Time_Reporting" in key['Key'].split("/")[1]):
                    s3_list.append(key['Key'].split("/")[1])
        return s3_list

    @task
    def get_files_in_transtats():
        from bs4 import BeautifulSoup
        import requests
        import datetime
        import re
        url = "http://transtats.bts.gov/PREZIP/"
        req = requests.get(url,verify=False)
        soup = BeautifulSoup(req.content, 'html.parser')
        live_list = []
        
        today = datetime.date.today()

        for year in range((today.year)-5,(today.year)+1):
            for files in soup.find_all(href=re.compile(f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}")):
                live_list.append(files.contents[0])
        return live_list

    @task
    def compare_lists(s3_list,live_list):
        difference = list(set(live_list).difference(set(s3_list)))
        return difference[0:8]

    @task(max_active_tis_per_dag=4,execution_timeout=pendulum.duration(seconds=600))
    def fetch_files(file_name):
        import requests
        import zipfile
        from io import BytesIO
        import boto3

        URL = f"https://transtats.bts.gov/PREZIP/{file_name}"
        response = requests.get(URL,verify=False)

        with zipfile.ZipFile(BytesIO(response.content)) as zip:
            for info in zip.infolist():
                if "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)" in info.filename:
                    file_bytes = zip.read(info.filename)


        conn = boto3.client('s3',
            endpoint_url='http://host.docker.internal:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            aws_session_token=None,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False
        )
        bucketname = 'cosmicenergy-ml-public-datasets'
        key = f'flight_data/{file_name.split(".zip")[0]}.csv.gz'
        conn.put_object(Body=gzip.compress(file_bytes),Bucket= bucketname,Key =key)

        return file_name

    @task
    def wait_for_files(files):
        return list(files)

    s3_list = get_files_in_s3() 
    live_list = get_files_in_transtats() 
    difference = compare_lists(s3_list,live_list)
    fetch_missing_files_complete = fetch_files.expand(file_name = difference)
    wait_for_files(fetch_missing_files_complete)

update_files_in_s3_dag = update_files_in_s3()