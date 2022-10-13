import requests

### Insert credentials
client_id = ''
authorization_code = ''
code_verifier = ''

body = {'client_id': client_id, 'code': authorization_code, 'code_verifier': code_verifier, 'grant_type': 'authorization_code'}

access_token_response = requests.post('https://api.fitbit.com/oauth2/token', data=body)

access_token_response = access_token_response.json()

headers = {'authorization': 'Bearer ' + access_token_response['access_token']}

res = requests.get('https://api.fitbit.com/1.2/user/-/sleep/date/2022-10-11.json', headers=headers)
print(res.text)
