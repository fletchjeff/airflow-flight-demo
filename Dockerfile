FROM quay.io/astronomer/astro-runtime:8.5.0

# Local dev ENV
ENV IS_PROD='False' \
DB_CONN_ID="postgres_local" \
FILE_CONN_ID="minio_local" \
BUCKET_NAME="raw-flight-data" \
MLFLOW_SERVER='http://host.docker.internal:5000'

# Astro dev ENV
# ENV IS_PROD='True' \
# DB_CONN_ID="my_postgres" \
# FILE_CONN_ID="my_aws_conn" \
# BUCKET_NAME="ce-ml-data" \
# MLFLOW_SERVER='http://add2f2378d8604988b8bd8fdcb5afa93-609300651.us-east-1.elb.amazonaws.com:5000'


ENV AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES='airflow\.* astro\.*'

ENV AIRFLOW_CONN_MINIO_LOCAL='{\
    "conn_type": "aws",\
    "description": "",\
    "login": "minioadmin",\
    "password": "minioadmin",\
    "host": "",\
    "port": null,\
    "schema": "",\
    "extra": "{\"aws_access_key\": \"minioadmin\", \"aws_secret_access_key\": \"minioadmin\", \"endpoint_url\": \"http://host.docker.internal:9000\"}"\
  }'

ENV AIRFLOW_CONN_POSTGRES_LOCAL='{\
    "conn_type": "postgres",\
    "description": "",\
    "login": "postgres",\
    "password": "postgres",\
    "host": "host.docker.internal",\
    "port": 5433,\
    "schema": "",\
    "extra": ""\
  }'