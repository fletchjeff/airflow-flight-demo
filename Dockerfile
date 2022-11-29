FROM quay.io/astronomer/astro-runtime:6.0.4

# Local dev ENV
ENV DB_CONN_ID="postgres_local" \
FILE_CONN_ID="minio_local" \
BUCKET_NAME="cosmicenergy-ml-public-datasets" \
XCOM_BUCKET_NAME="astro-xcom-backend"

# Prod dev ENV
#ENV DB_CONN_ID="jf-snowflake"
#ENV FILE_CONN_ID="jf-xcom"



# Prod dev ENV
ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL='s3://local-xcom'
ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID='minio_local'
ENV AIRFLOW__CORE__XCOM_BACKEND='astro.custom_backend.astro_custom_backend.AstroCustomXcomBackend'

# ENV AIRFLOW__ASTRO_SDK__AUTO_ADD_INLETS_OUTLETS = "false"

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

ENV MLFLOW_SERVER='host.docker.internal:5000'
