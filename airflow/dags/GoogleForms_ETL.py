from helper_modules.GoogleForms import GoogleForms
import helper_modules.env_config as env_config
from helper_modules.DBConnector import DBConnector
import pandas as pd
import pendulum as pdl
import re
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

@dag(
    dag_id='GoogleForms',
    schedule='0 21 * * *',
    start_date=pdl.datetime(2023, 2, 23, tz="UTC")
)
def GoogleForms_taskflow():
    @task
    def extract(**kwargs):
        google_forms_service = GoogleForms(env_config.google_forms_credential_file)
        responses = google_forms_service.get_responses(env_config.google_forms_id)

        ti = kwargs["ti"]
        ti.xcom_push("responses", responses)

    @task(
        trigger_rule='all_success'
    )
    def transform(**kwargs):
        ti = kwargs["ti"]
        responses = ti.xcom_pull(task_ids="extract", key="responses")

        df = pd.DataFrame.from_dict(responses['responses'])

        db_conn = DBConnector(env_config.db_user, env_config.db_password, env_config.db_name, env_config.db_host)
        db_conn.connect()
        db_conn.execute_query("select COALESCE(max(recorded_date), '2023-01-01') from PROD.Google_Forms_Responses")
        max_timestamp = db_conn.cur.fetchone()
        db_conn.close_connection()

        df_filtered = df[df.lastSubmittedTime > max_timestamp[0].strftime("%Y-%m-%dT%H:%M:%SZ")]

        if df_filtered.shape[0] == 0:
            print("LOGGING: No response to perform ETL on")
            raise AirflowSkipException

        ti.xcom_push("df_filtered", df_filtered.to_json())

    @task(
        trigger_rule='all_success'
    )
    def load(**kwargs):
        ti = kwargs["ti"]
        responses_filtered = ti.xcom_pull(task_ids="transform", key="df_filtered")
        df_filtered = pd.read_json(responses_filtered)

        db_conn = DBConnector(env_config.db_user, env_config.db_password, env_config.db_name, env_config.db_host)
        db_conn.connect()

        answers = {'motivated_level': None, 
                'happiness_level': None, 
                'tiredness_level': None, 
                'breakfast_yday': None, 
                'lunch_yday': None,
                'snack_amount_yday': None,
                'minutes_assignments_yday': None,
                'minutes_self_study_yday': None,
                'minutes_job_hunting_yday': None,
                'minutes_exercises_yday': None}

        for i in range(df_filtered.shape[0]):
            answer = df_filtered.loc[i, "answers"]
            lastSubmittedTime = df_filtered.loc[i, "lastSubmittedTime"]
            lastSubmittedDate = pdl.from_format(re.sub("\.[0-9]+Z", "", lastSubmittedTime), 'YYYY-MM-DDTHH:mm:ss', tz='UTC').in_timezone('America/Los_Angeles').to_date_string()

            for v in answer.values():
                match v['questionId']:
                    case '410109da':
                        answers['motivated_level'] = v['textAnswers']['answers'][0]['value']
                    case '361dcf07':
                        answers['happiness_level'] = v['textAnswers']['answers'][0]['value']
                    case '6a8fa3ad':
                        answers['tiredness_level'] = v['textAnswers']['answers'][0]['value']
                    case '3601998b':
                        if len(v['textAnswers']['answers']) == 2:
                            answers['breakfast_yday'] = 'TRUE'
                            answers['lunch_yday'] = 'TRUE'
                        else:
                            if v['textAnswers']['answers'][0]['value'] == 'Breakfast':
                                answers['breakfast_yday'] = 'TRUE'
                                answers['lunch_yday'] = 'FALSE'
                            else:
                                answers['breakfast_yday'] = 'FALSE'
                                answers['lunch_yday'] = 'TRUE'
                    case '1160410d':
                        answers['snack_amount_yday'] = v['textAnswers']['answers'][0]['value']
                    case '1ea6a5ab':
                        answers['minutes_assignments_yday'] = v['textAnswers']['answers'][0]['value']
                    case '1672a6b8':
                        answers['minutes_self_study_yday'] = v['textAnswers']['answers'][0]['value']
                    case '08c1b8d3':
                        answers['minutes_job_hunting_yday'] = v['textAnswers']['answers'][0]['value']
                    case '71e0aae8':
                        answers['minutes_exercises_yday'] = v['textAnswers']['answers'][0]['value']
                
            db_conn.execute_query("""
            INSERT INTO PROD.Google_Forms_Responses(load_timestamp, recorded_date, motivated_level, happiness_level, tiredness_level, breakfast_yday, lunch_yday, snack_amount_yday, minutes_assignments_yday, minutes_self_study_yday, minutes_job_hunting_yday, minutes_exercises_yday) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (pdl.now('America/Los_Angeles'), lastSubmittedDate, answers['motivated_level'], answers['happiness_level'], answers['tiredness_level'], answers['breakfast_yday'], answers['lunch_yday'], answers['snack_amount_yday'], answers['minutes_assignments_yday'], answers['minutes_self_study_yday'], answers['minutes_job_hunting_yday'], answers['minutes_exercises_yday']))

        db_conn.close_connection()


    extract() >> transform() >> load()

GoogleForms_taskflow()