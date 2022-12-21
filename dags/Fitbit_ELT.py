import sys
import os
sys.path.append(os.getcwd())
sys.path.append(os.getcwd()+'/PythonClasses')

import config
from Fitbit import Fitbit
from DBConnector import DBConnector

from airflow.decorators import dag, task
import pendulum as pdl

@dag(
    schedule=None,
    start_date=pdl.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def fitbit_taskflow():
    @task()
    def extract_load():
        fb = Fitbit(config.fitbit_client_id, 
                    config.fitbit_access_token,
                    '2023-01-01')

        db_conn = DBConnector(config.db_user, config.db_password, config.db_name, config.db_host)
        db_conn.connect()

        types = ['sleep', 'steps', 'calories']
        records = { t: None for t in types }
        record_date = '2022-11-16'

        for type in types:
            res = fb.get_records(type, record_date)
            db_conn.execute_query("INSERT INTO LANDING.{} (load_date, load_json) VALUES ('{}', '{}')".format(type, pdl.today(), res.text))

        db_conn.close_connection()
    
    @task()
    def transform():
        pass

    extract_load()
    transform()

fitbit_taskflow()