from airflow import DAG, Dataset
from airflow.decorators import task
import pendulum
from datetime import datetime
from airflow.providers.amazon.aws.hooks import s3
import os 
BUCKET_NAME = os.environ["BUCKET_NAME"]
IS_PROD=os.environ["IS_PROD"]

flight_data_dataset = Dataset(f's3://{BUCKET_NAME}/flight_data')

dag = DAG(
    dag_id="2_check_for_new_files",
    start_date=datetime(2019, 1, 1),
    schedule='@monthly',
    catchup=False,
)

with dag:
    @task
    def get_files_in_s3():
        if IS_PROD=='True':
            myhook = s3.S3Hook(aws_conn_id='my_aws_conn')
            s3_list = myhook.list_keys(bucket_name=BUCKET_NAME,prefix='flight_data')
            s3_list.remove('flight_data/')
            for index,item in enumerate(s3_list):
                s3_list[index]=item.split("/")[1].split(".")[0]
        else:                
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
            
            if 'Contents' in conn.list_objects_v2(Bucket=BUCKET_NAME,Prefix='flight_data'):
                for key in conn.list_objects_v2(Bucket=BUCKET_NAME,Prefix='flight_data')['Contents']:
                    if (len(key['Key'].split("/")) > 1) and ("On_Time_Reporting" in key['Key'].split("/")[1]):
                        s3_list.append(key['Key'].split("/")[1].split(".")[0])
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

        for year in range((today.year)-2,(today.year)+1):
            for files in soup.find_all(href=re.compile(f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}")):
                live_list.append(files.contents[0].split(".")[0])
        return live_list

    @task
    def compare_lists(s3_list,live_list):
        difference = list(set(live_list).difference(set(s3_list)))
        return difference #[0:8]

    @task(max_active_tis_per_dag=4,execution_timeout=pendulum.duration(seconds=600))
    def fetch_files(file_name):
        import requests
        import zipfile
        from io import BytesIO
        import boto3

        URL = f"https://transtats.bts.gov/PREZIP/{file_name}.zip"
        response = requests.get(URL,verify=False)

        with zipfile.ZipFile(BytesIO(response.content)) as zip:
            for info in zip.infolist():
                if "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)" in info.filename:
                    file_bytes = zip.read(info.filename)

        if IS_PROD=='True':
            myhook = s3.S3Hook(aws_conn_id='my_aws_conn')
            key = f'flight_data/{file_name.split(".zip")[0]}.csv'
            myhook.load_bytes(bytes_data=file_bytes,bucket_name=BUCKET_NAME,key=key)

        else:
            conn = boto3.client('s3',
                endpoint_url='http://host.docker.internal:9000',
                aws_access_key_id='minioadmin',
                aws_secret_access_key='minioadmin',
                aws_session_token=None,
                config=boto3.session.Config(signature_version='s3v4'),
                verify=False
            )
            
            key = f'flight_data/{file_name.split(".zip")[0]}.csv'
            conn.put_object(Body=file_bytes,Bucket=BUCKET_NAME,Key=key)

        return file_name

    @task(outlets=[flight_data_dataset])
    def wait_for_files(files):
        return list(files)

    s3_list = get_files_in_s3() 
    live_list = get_files_in_transtats() 
    difference = compare_lists(s3_list,live_list)
    fetch_missing_files_complete = fetch_files.expand(file_name = difference)
    wait_for_files(fetch_missing_files_complete)