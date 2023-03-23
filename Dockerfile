# FROM quay.io/astronomer/astro-runtime:6.0.4
FROM quay.io/astronomer/astro-runtime:7.4.1

# Local dev ENV
ENV DB_CONN_ID="postgres_local" \
FILE_CONN_ID="minio_local" \
BUCKET_NAME="raw-flight-data" \
MLFLOW_SERVER='host.docker.internal:5000'
#XCOM_BUCKET_NAME="astro-xcom-backend" \
#MLFLOW_SERVER='host.docker.internal:5000'

# ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL='s3://local-xcom'
# ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID='minio_local'
# ENV AIRFLOW__CORE__XCOM_BACKEND='astro.custom_backend.astro_custom_backend.AstroCustomXcomBackend'
ENV AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES='airflow\.* astro\.*'

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

