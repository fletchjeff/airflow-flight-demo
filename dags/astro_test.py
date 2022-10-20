from datetime import datetime

from airflow.models import DAG
#from pandas import DataFrame

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table
import pandas as pd

# Define constants for interacting with external systems
# S3_FILE_PATH = "s3://<aws-bucket-name>"
# S3_CONN_ID = "aws_default"
# SNOWFLAKE_CONN_ID = "snowflake_default"
# SNOWFLAKE_ORDERS = "orders_table"
# SNOWFLAKE_CUSTOMERS = "customers_table"
# SNOWFLAKE_REPORTING = "reporting_table"

# Define an SQL query for our transform step as a Python function using the SDK.
# This function filters out all rows with an amount value less than 150.
# @aql.transform
# def filter_orders(input_table: Table):
#     return "SELECT * FROM {{input_table}} WHERE amount > 150"

# # Define an SQL query for our transform step as a Python function using the SDK.
# # This function joins two tables into a new table.
# @aql.transform
# def join_orders_customers(filtered_orders_table: Table, customers_table: Table):
#     return """SELECT c.customer_id, customer_name, order_id, purchase_date, amount, type
#     FROM {{filtered_orders_table}} f JOIN {{customers_table}} c
#     ON f.customer_id = c.customer_id"""

# # Define a function for transforming tables to dataframes
# @aql.dataframe
# def transform_dataframe(df: DataFrame):
#     purchase_dates = df.loc[:, "purchase_date"]
#     print("purchase dates:", purchase_dates)
#     return purchase_dates

# Basic DAG definition. Run the DAG starting January 1st, 2019 on a daily schedule.
dag = DAG(
    dag_id="astro_orders",
    start_date=datetime(2019, 1, 1),
    schedule_interval=None, #"@daily",
    catchup=False,
)

with dag:
    @aql.dataframe()
    def read_data():
        import numpy as np
        frame_data = np.random.randint(0, 100, size=(2**10, 4))
        df = pd.DataFrame(frame_data,columns=list('ABCD'))
        return df

    @aql.dataframe()
    def df_slice(random_data: pd.DataFrame):
        print(random_data)
        return random_data[0:30]

    t1 = aql.load_file(
        task_id="load_from_github_to_bq",
        input_file=File(
            path="s3://test/smaller.csv",
            conn_id="aws_default"
        )
    )

    # @aql.transform()
    # def
    # t1 = aql.load_file(
    #     task_id="load_from_github_to_bq",
    #     input_file=File(
    #         path="s3://test.csv",
    #         conn_id="s3_conn"
    #     )

    #         #path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
    #     ),
    #     # output_table=Table(
    #     #     name="imdb_movies", conn_id="local_postgres"
    #     # ),
    # )

    # dataframe = aql.load_file(
    #     input_file=File(path="s3://test/smaller.csv"),
    # )

    # Extract a file with a header from S3 into a temporary Table, referenced by the
    # variable `orders_data`
    # orders_data = aql.load_file(
    #     # Data file needs to have a header row. The input and output table can be replaced with any
    #     # valid file and connection ID.
    #     input_file=File(
    #         path=S3_FILE_PATH + "/orders_data_header.csv", conn_id=S3_CONN_ID
    #     ),
    #     output_table=Table(conn_id=SNOWFLAKE_CONN_ID),
    # )

    # # Create a Table object for customer data in the Snowflake database
    # customers_table = Table(
    #     name=SNOWFLAKE_CUSTOMERS,
    #     conn_id=SNOWFLAKE_CONN_ID,
    # )

    # # Filter the orders data and then join with the customer table,
    # # saving the output into a temporary table referenced by the Table instance `joined_data`
    # joined_data = join_orders_customers(filter_orders(orders_data), customers_table)

    # # Merge the joined data into the reporting table based on the order_id.
    # # If there's a conflict in the customer_id or customer_name, then use the ones from
    # # the joined data
    # reporting_table = aql.merge(
    #     target_table=Table(
    #         name=SNOWFLAKE_REPORTING,
    #         conn_id=SNOWFLAKE_CONN_ID,
    #     ),
    #     source_table=joined_data,
    #     target_conflict_columns=["order_id"],
    #     columns=["customer_id", "customer_name"],
    #     if_conflicts="update",
    # )

    # # Transform the reporting table into a dataframe
    # purchase_dates = transform_dataframe(reporting_table)

    # Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    # both `orders_data` and `joined_data`
    aql.cleanup()
    read_data_df = read_data()
    final_slice = df_slice(read_data_df)
    #load_file_check = df_slice(dataframe)