FROM quay.io/astronomer/astro-runtime:6.0.2

#ENV AIRFLOW__CORE__XCOM_BACKEND=include.modin_xcom_backend.ModinXComBackend
ENV AIRFLOW__CORE__XCOM_BACKEND=astro.custom_backend.astro_custom_backend.AstroCustomXcomBackend
ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL='s3://jf-xcom'

#ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
#ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL='s3://minioadmin:minioadmin@?host=http%3A%2F%2F192.168.1.101%3A9000'
ENV AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID='aws_default'
#ENV AIRFLOW_CONN_S3_CONN='s3://minioadmin:minioadmin@?host=http%3A%2F%2F192.168.1.101%3A9000'
