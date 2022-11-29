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

#datasets
model_training_data_latest_dataset =  Dataset('postgresql://host.docker.internal/postgres/tmp_astro.model_training_data_latest')  

dag = DAG(
    dag_id="5_train_model",
    start_date=datetime(2022, 10, 30),
    schedule=[model_training_data_latest_dataset],
    catchup=False,
)

with dag:
    model_directory = "model_training_data_{{dag_run.logical_date.strftime('%Y%m%d_%H%M%S')}}"

    training_data_table_latest = Table(name="model_training_data_latest", conn_id=DB_CONN_ID)
    
    training_data_table_copy = Table(name=model_directory, conn_id=DB_CONN_ID)

    @aql.run_raw_sql(conn_id=DB_CONN_ID)
    def clone_latest_training_data_table(latest_table: Table, copy_table: Table):
        return """
        CREATE TABLE {{copy_table}} AS 
        TABLE {{latest_table}};
        """

    @aql.transform
    def get_training_dataframe(training_data_table: Table):
        return """
        SELECT * FROM {{training_data_table}}
        """

    @aql.dataframe
    def train_model(model_data: DataFrame, model_directory):
        # import mlflow
        # import mlflow.xgboost
        #mlflow.xgboost.autolog()


        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(FILE_CONN_ID)
        print(conn)

        print(model_data.shape)
        model_data_clean = model_data.dropna()
        print(model_data_clean.columns)

        X = model_data_clean[['MONTH', 'DAYOFMONTH', 'DAYOFWEEK', 'REPORTING_AIRLINE', 'TAIL_NUMBER','ORIGIN', 'ORIGINCITYNAME', 'ORIGINSTATE', 'DEST', 'DESTCITYNAME','DESTSTATE', 'CRSDEPTIME', 'CRSARRTIME', 'CRSELAPSEDTIME', 'DISTANCE']]

        y = model_data_clean[['CANCELLED']]

        categorical_cols = ['MONTH', 'DAYOFMONTH', 'DAYOFWEEK', 'REPORTING_AIRLINE', 'TAIL_NUMBER','ORIGIN', 'ORIGINCITYNAME', 'ORIGINSTATE', 'DEST', 'DESTCITYNAME','DESTSTATE', 'CRSDEPTIME', 'CRSARRTIME']

        bucketname = 'cosmicenergy-ml-public-datasets'

        mlflow.set_tracking_uri("http://host.docker.internal:5000")
        mlflow.set_experiment("train_model")
        with mlflow.start_run(run_name=model_directory) as run:
            mlflow.log_param("model_directory",model_directory)

            ct = ColumnTransformer(
                [('le', OneHotEncoder(), categorical_cols)],
                remainder='passthrough'
            )

            X_trans = ct.fit_transform(X)
            print(type(X_trans))
            print(X_trans.shape)


            X_train, X_test, y_train, y_test = train_test_split(X_trans, y, random_state=42)
            xgbclf = xgb.XGBClassifier() 

            pipe = Pipeline([('scaler', StandardScaler(with_mean=False)),
                    ('xgbclf', xgbclf)])

            pipe.fit(X_train, y_train)

            model_uri = "runs:/{}".format(run.info.run_id)
            mlflow.register_model(model_uri=model_uri, name="XGBClassifier",tags={
                'pipe':f's3://{bucketname}/models/{model_directory}/pipe.joblib',
                'ct':f's3://{bucketname}/models/{model_directory}/ct.joblib',
                })

        from smart_open import open
        import boto3
        client = boto3.client('s3', 
            endpoint_url='http://host.docker.internal:9000/',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
        )
        
        # This needs an Astro SDK wrapper
        with open(f's3://{bucketname}/models/{model_directory}/X_train.npz','wb',transport_params={'client': client}) as f:
            scipy.sparse.save_npz(f, X_train)
        
        with open(f's3://{bucketname}/models/{model_directory}/X_test.npz','wb',transport_params={'client': client}) as f:
            scipy.sparse.save_npz(f, X_test)

        with open(f's3://{bucketname}/models/{model_directory}/y_train.parquet.gzip','wb',transport_params={'client': client}) as f:
            y_train.to_parquet(f,compression='gzip')

        with open(f's3://{bucketname}/models/{model_directory}/y_test.parquet.gzip','wb',transport_params={'client': client}) as f:
            y_test.to_parquet(f,compression='gzip')            

        with open(f's3://{bucketname}/models/{model_directory}/pipe.joblib','wb',transport_params={'client': client}) as f:
            dump(pipe,f)

        with open(f's3://{bucketname}/models/{model_directory}/ct.joblib','wb',transport_params={'client': client}) as f:
            dump(ct,f)

        # Note: return dict did not work
        return run.info.run_id

    @aql.dataframe
    def evaluate_model(run_id):
        from smart_open import open
        import boto3, os
        client = boto3.client('s3', 
            endpoint_url='http://host.docker.internal:9000/',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
        )

        mlflow.set_tracking_uri("http://host.docker.internal:5000")
        mlflow.set_experiment("train_model")
        model_path = mlflow.get_run(run_id=run_id).data.params['model_directory']
        bucketname = 'cosmicenergy-ml-public-datasets'

        with open (f's3://{bucketname}/models/{model_path}/X_test.npz','rb',transport_params={'client': client}) as f:
            X_test = scipy.sparse.load_npz(f)

        with open (f's3://{bucketname}/models/{model_path}/y_test.parquet.gzip','rb',transport_params={'client': client}) as f:
            y_test = pd.read_parquet(f)

        with open (f's3://{bucketname}/models/{model_path}/pipe.joblib','rb',transport_params={'client': client}) as f:
            pipe = load(f)              
       
        test_score = pipe.score(X_test, y_test)
        from sklearn.metrics import precision_recall_fscore_support
        from sklearn.metrics import classification_report
        print(classification_report(y_test['CANCELLED'].to_numpy(),pipe.predict(X_test)))
        prfs = precision_recall_fscore_support(y_test['CANCELLED'].to_numpy(),pipe.predict(X_test),average='binary')

        with mlflow.start_run(run_id=run_id) as run:
            mlflow.log_metric("test_score",test_score)
            mlflow.log_metric("precision",prfs[0])
            mlflow.log_metric("recall",prfs[1])
            mlflow.log_metric("fscore",prfs[2])

        print("test score", test_score)
        return run.info.run_id



    training_data = get_training_dataframe(training_data_table_latest)
    
    clone_latest_training_data_table(training_data_table_latest,training_data_table_copy) >> training_data
    
    train_model_directory = train_model(training_data,model_directory)
    


    evaluate_model(train_model_directory) 


    
    aql.cleanup()