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
                    config.fitbit_access_token_expires_on)

        db_conn = DBConnector(config.db_user, config.db_password, config.db_name, config.db_host)
        db_conn.connect()

        types = ['sleep', 'steps', 'calories']
        record_date = pdl.now('America/Los_Angeles').to_date_string()

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