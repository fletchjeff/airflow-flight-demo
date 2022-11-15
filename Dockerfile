FROM quay.io/astronomer/astro-runtime:6.0.3

#ENV AIRFLOW__CORE__XCOM_BACKEND=include.modin_xcom_backend.ModinXComBackend
#ENV AIRFLOW__CORE__XCOM_BACKEND=astro.custom_backend.astro_custom_backend.AstroCustomXcomBackend
#ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL='s3://jf-xcom'
#ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL='s3://jfletcher-datasets/astro-backend/'

#ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
#ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL='s3://minioadmin:minioadmin@?host=http%3A%2F%2F192.168.1.101%3A9000'
#ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID='aws_default'
#ENV AIRFLOW_CONN_S3_CONN='s3://minioadmin:minioadmin@?host=http%3A%2F%2F192.168.1.101%3A9000'

# COPY tmp_sdk_fix/s3.py /usr/local/lib/python3.9/site-packages/astro/files/locations/amazon/s3.py

# Local dev ENV
ENV DB_CONN_ID="mypsql"
ENV FILE_CONN_ID="minio_default"
ENV BUCKET_NAME="cosmicenergy-ml-public-datasets"
#ENV BUCKET_NAME="jfletcher-datasets"

# Prod dev ENV
#ENV DB_CONN_ID="jf-snowflake"
#ENV FILE_CONN_ID="jf-xcom"


ENV AIRFLOW__CORE__XCOM_BACKEND=astro.custom_backend.astro_custom_backend.AstroCustomXcomBackend

# Local dev ENV
ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL='s3://local-xcom'
ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID='minio_default'

# Prod dev ENV
#ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL='s3://jf-xcom'
#ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID='jf-xcom'


#ENV AIRFLOW__ASTRO_SDK__AUTO_ADD_INLETS_OUTLETS = "false"

ENV AIRFLOW_CONN_MINIO_DEFAULT='{\
    "conn_type": "aws",\
    "description": "",\
    "login": "minioadmin",\
    "password": "minioadmin",\
    "host": "",\
    "port": null,\
    "schema": "",\
    "extra": "{\"aws_access_key\": \"minioadmin\", \"aws_secret_access_key\": \"minioadmin\", \"endpoint_url\": \"http://host.docker.internal:9000\"}"\
  }'

ENV AIRFLOW_CONN_MYPSQL='{\
    "conn_type": "postgres",\
    "description": "",\
    "login": "postgres",\
    "password": "postgres",\
    "host": "host.docker.internal",\
    "port": 5433,\
    "schema": "",\
    "extra": ""\
  }'

#  ENV AIRFLOW_CONN_ASTRO_ORDERS_SQLITE='{\
#     "conn_type": "sqlite",\
#     "description": "",\
#     "login": "",\
#     "password": "",\
#     "host": "/usr/local/airflow/include/astro_orders.db",\
#     "port": null,\
#     "schema": "",\
#     "extra": ""\
#   }' 

#ENV MLFLOW_SERVER='host.docker.internal:5000'