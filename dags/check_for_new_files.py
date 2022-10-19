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
        conn = boto3.client('s3')
        s3_list = []
        for key in conn.list_objects_v2(Bucket="cosmicenergy-ml-public-datasets",Prefix='flight_data/raw_files')['Contents']:
            if (len(key['Key'].split("/")) > 2) and ("On_Time_Reporting" in key['Key'].split("/")[2]):
                s3_list.append(key['Key'].split("/")[2])
        return json.dumps(s3_list)

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

        return json.dumps(live_list)

    @task
    def compare_lists(s3_list,live_list):
        difference = list(set(live_list).difference(set(s3_list)))
        print(difference)
        return difference

    s3_list = get_files_in_s3() 
    live_list = get_files_in_transtats() 
    difference = compare_lists(s3_list,live_list)

update_files_in_s3_dag = update_files_in_s3()