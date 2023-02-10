from google.oauth2 import service_account
import googleapiclient.discovery

class GoogleForms:
    def __init__(self, service_account_file_path):
        self.service_account_file_path = service_account_file_path

    def get_responses(self, form_id):
        SCOPES = ['https://www.googleapis.com/auth/forms.responses.readonly']

        credentials = service_account.Credentials.from_service_account_file(self.service_account_file_path, scopes=SCOPES)

        forms_service = googleapiclient.discovery.build('forms', 'v1', credentials=credentials)

        responses = forms_service.forms().responses().list(formId=form_id).execute()

        return responses
