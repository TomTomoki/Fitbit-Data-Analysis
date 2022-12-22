import requests
import pendulum as pdl

class Fitbit:
    def __init__(self, client_id, access_token, access_token_expires_on):
        self.client_id = client_id
        self.access_token = access_token
        self.headers = {'authorization': 'Bearer ' + self.access_token}
        self.access_token_expires_on = pdl.from_format(access_token_expires_on, 'YYYY-MM-DD').date()

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
        return requests.get('https://api.fitbit.com/1/user/-/activities/steps/date/' + date + '/1d.json', headers=self.headers)

    def get_calories(self, date):
        return requests.get('https://api.fitbit.com/1/user/-/activities/calories/date/' + date + '/1d.json', headers=self.headers)

    def access_token_expired(self):
        """_summary_

        Returns:
            bool: True if expired
        """
        now = pdl.now('America/Los_Angeles').date()
        
        return now > self.access_token_expires_on