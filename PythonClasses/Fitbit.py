import requests
from datetime import datetime, timedelta
import pytz

class Fitbit:
    def __init__(self, client_id, access_token, access_token_expires_on):
        self.client_id = client_id
        self.access_token = access_token
        self.headers = {'authorization': 'Bearer ' + self.access_token}
        self.access_token_expires_on = datetime.strptime(access_token_expires_on, '%Y-%m-%d').date()

    def get_records(self, type, date):
        if self.access_token_expired():
            print("Error: Access Token Expired")
        
        if type == 'sleep':
            return self.get_sleep(date)
        elif type == 'steps':
            return self.get_steps(date)
        elif type == 'calories':
            return self.get_calories(date)

    def get_sleep(self, date):
        return requests.get('https://api.fitbit.com/1.2/user/-/sleep/date/' + date + '.json', headers=self.headers)

    def get_steps(self, date):
        return requests.get('https://api.fitbit.com/1/user/-/activities/steps/date/2022-10-11/1d.json', headers=self.headers)

    def get_calories(self, date):
        return requests.get('https://api.fitbit.com/1/user/-/activities/calories/date/2022-10-11/1d.json', headers=self.headers)

    def access_token_expired(self):
        """_summary_

        Returns:
            bool: True if expired
        """
        pst = pytz.timezone('America/Los_Angeles')
        now = datetime.now(pst)
        
        return now > self.access_token_expires_on