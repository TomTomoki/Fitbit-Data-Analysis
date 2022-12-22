import helper_modules.env_config as env_config
from helper_modules.Fitbit import Fitbit
from helper_modules.DBConnector import DBConnector

from airflow.decorators import dag, task
import pendulum as pdl

@dag(
    dag_id='Fitbit',
    schedule=None,
    start_date=pdl.datetime(2022, 12, 1, tz="America/Los_Angeles"),
    default_args={"retries": 2}
)
def fitbit_taskflow():
    @task()
    def extract_load():
        fb = Fitbit(env_config.fitbit_client_id, 
                    env_config.fitbit_access_token,
                    env_config.fitbit_access_token_expires_on)

        db_conn = DBConnector(env_config.db_user, env_config.db_password, env_config.db_name, env_config.db_host)
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

    extract_load() >> transform()

fitbit_taskflow()