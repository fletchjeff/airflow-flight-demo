FROM quay.io/astronomer/astro-runtime:6.0.2

#ENV AIRFLOW__CORE__XCOM_BACKEND=include.modin_xcom_backend.ModinXComBackend
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True