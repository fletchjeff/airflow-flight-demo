import json

import pendulum
#import modin.pandas as pd
import pandas as pd


from airflow.decorators import dag, task
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def xcom_test():

    @task()
    def read_data():
        import numpy as np
        frame_data = np.random.randint(0, 100, size=(2**10, 4))
        df = pd.DataFrame(frame_data,columns=list('ABCD'))
        return df

    @task()
    def df_slice(random_data: pd.DataFrame):
        print(random_data)
        return random_data[0:30]

    read_data_df = read_data()
    final_slice = df_slice(read_data_df)

tutorial_etl_dag = xcom_test()