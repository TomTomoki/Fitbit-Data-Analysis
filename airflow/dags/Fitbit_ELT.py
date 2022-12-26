import helper_modules.env_config as env_config
from helper_modules.Fitbit import Fitbit
from helper_modules.DBConnector import DBConnector
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import pendulum as pdl
import os

@dag(
    dag_id='Fitbit',
    schedule=None,
    start_date=pdl.datetime(2022, 12, 1, tz="America/Los_Angeles"),
    #default_args={"retries": 2}
)
def fitbit_taskflow():
    @task
    def extract_load():
        fb = Fitbit(env_config.fitbit_client_id, 
                    env_config.fitbit_access_token,
                    env_config.fitbit_access_token_expires_on)

        db_conn = DBConnector(env_config.db_user, env_config.db_password, env_config.db_name, env_config.db_host)
        db_conn.connect()

        types = ['sleep', 'steps', 'calories']
        #record_date = pdl.now('America/Los_Angeles').to_date_string()
        record_date = pdl.from_format('2022-10-12', 'YYYY-MM-DD').to_date_string()

        for type in types:
            res = fb.get_records(type, record_date)
            db_conn.execute_query("INSERT INTO LANDING.{} (load_timestamp, load_json) VALUES ('{}', '{}')".format(type, pdl.now('America/Los_Angeles'), res.text))

        db_conn.close_connection()
    
    ### Transform task
    HOME = os.environ["HOME"]
    dbt_path = os.path.join(HOME, "Fitbit-Data-Analysis/dbt/fitbit_dbt")

    transform_stg = BashOperator(
        task_id = 'transform_stg',
        bash_command=f"source {HOME}/Fitbit-Data-Analysis/env/bin/activate && cd {dbt_path}" + " && dbt run --select staging.stg_sleep staging.stg_steps staging.stg_calories"
    )

    transform_prod = BashOperator(
        task_id = 'transform_prod',
        bash_command=f"cd {dbt_path}"
    )

    extract_load() >> transform_stg >> transform_prod


fitbit_taskflow()