import helper_modules.env_config as env_config
from helper_modules.Fitbit import Fitbit
from helper_modules.DBConnector import DBConnector
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from  airflow.exceptions import AirflowFailException
import pendulum as pdl
import os

@dag(
    dag_id='Fitbit',
    schedule='30 20 * * *',
    start_date=pdl.datetime(2023, 3, 1, tz="UTC")
)
def fitbit_taskflow():
    @task(
        provide_context=True
    )
    def extract_load(**kwargs):
        fb = Fitbit(env_config.fitbit_client_id, 
                    env_config.fitbit_access_token,
                    env_config.fitbit_access_token_expires_on)

        db_conn = DBConnector(env_config.db_user, env_config.db_password, env_config.db_name, env_config.db_host)
        db_conn.connect()

        if "manual_date_to_load" in kwargs["params"].keys():
            record_date = pdl.from_format(kwargs["params"]["manual_date_to_load"], 'YYYY-MM-DD').to_date_string()
        else:
            record_date = pdl.now('America/Los_Angeles').subtract(days=1).to_date_string()

        types = ['sleep', 'steps', 'calories', 'distance', 'sedentary', 'heartrate']

        for type in types:
            res = fb.get_records(type, record_date)

            if res == "access_token_expired":
                db_conn.close_connection()
                raise AirflowFailException("ERROR: Access token expired!")
            elif res == "invalid_record_type":
                db_conn.close_connection()
                raise AirflowFailException("ERROR: Invalid record type!")
            else:
                db_conn.execute_query("INSERT INTO LANDING.{} (load_timestamp, load_json) VALUES ('{}', '{}')".format(type, pdl.now('America/Los_Angeles'), res.text))

        db_conn.close_connection()
    
    ### Transform task
    HOME = os.environ["HOME"]
    dbt_path = os.path.join(HOME, "Fitbit-Data-Analysis/dbt/fitbit_dbt")

    transform = BashOperator(
        task_id = 'transform',
        bash_command=f"source {HOME}/Fitbit-Data-Analysis/env/bin/activate && cd {dbt_path}" + " && dbt run",
        trigger_rule='all_success'
    )

    extract_load() >> transform


fitbit_taskflow()