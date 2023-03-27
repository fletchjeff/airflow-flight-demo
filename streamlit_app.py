import streamlit as st
import psycopg2
import pandas.io.sql as sqlio
import datetime
import boto3
import mlflow
from joblib import load
from smart_open import open

connection_params = """
dbname=postgres
user=postgres
password=postgres
host=localhost
port=5433
"""

def load_latest_model(model_name):
    mlflow.set_tracking_uri("http://localhost:5000/")
    client = mlflow.MlflowClient()
    latest_model = client.get_latest_versions(name=model_name) #'XGBClassifier'
    botoclient = boto3.client('s3', 
        endpoint_url='http://localhost:9000/',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
    )
    pipe_uri = latest_model[0].tags['pipe']
    ct_uri = latest_model[0].tags['ct']
    with open (pipe_uri,'rb',transport_params={'client': botoclient}) as f:
        pipe = load(f)
    with open (ct_uri,'rb',transport_params={'client': botoclient}) as f:
        ct = load(f)
    return (pipe,ct)

def make_prediction(input_data,pipe_ct_tuple):
    print(input_data)
    ct = pipe_ct_tuple[1]
    pipe = pipe_ct_tuple[0]
    transformed_data = ct.transform(input_data)
    output_prediction = pipe.predict(transformed_data)[0]
    cancelled_string = ['Still Scheduled','Cancelled']
    return cancelled_string[output_prediction]

def fetch_one_row(origin_city):
    conn=psycopg2.connect(
        connection_params
    )
    query = f"""
        select "MONTH","DAYOFMONTH","DAYOFWEEK","REPORTING_AIRLINE","TAIL_NUMBER","ORIGIN","ORIGINCITYNAME","ORIGINSTATE","DEST","DESTCITYNAME","DESTSTATE",FLOOR("CRSDEPTIME"/100) AS "CRSDEPTIME",FLOOR("CRSARRTIME"/100) AS "CRSARRTIME","CRSELAPSEDTIME","DISTANCE" 
        from tmp_astro.flight_data
        where "MONTH" = extract('month' from current_date) 
        and "DAYOFMONTH" = extract('day' from current_date)
        and "ORIGINCITYNAME" = '{origin_city}'
        order by random() limit 1;
        """    
    return_data = sqlio.read_sql_query(query, conn)
    return_data = return_data.rename(columns={'CANCELLATION_PREDICTION': 'PREDICTION'})
    return return_data

def get_cities_list():
    conn=psycopg2.connect(
        connection_params
    )
    query = '''select "ORIGINCITYNAME", count(*) as "count" from tmp_astro.flight_data group by "ORIGINCITYNAME" order by "count" desc limit 20'''
    return_data = sqlio.read_sql_query(query, conn)
    return return_data

st.title('Flight Cancellation Prediction')
st.subheader(f'For flights on: {datetime.datetime.now().strftime("%d %B")}')

@st.cache_data
def get_single_flight():
    return fetch_one_row('Atlanta, GA')

single_flight = get_single_flight()

origin_city = st.selectbox("Pick an origin city",get_cities_list())    
if st.button("Get New Flight"):
    single_flight = fetch_one_row(origin_city)      
st.write(f"Prediction: **{make_prediction(single_flight,load_latest_model('XGBClassifier'))}**")
st.dataframe(single_flight[['TAIL_NUMBER','ORIGINCITYNAME','DESTCITYNAME','CRSDEPTIME']].T)


