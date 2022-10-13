import os
import hashlib
import base64
import re

code_verifier = base64.urlsafe_b64encode(os.urandom(40))
#print(code_verifier)
code_verifier = code_verifier.decode('utf-8')
#print(code_verifier)
code_verifier = re.sub('[^a-zA-Z0-9]+', '', code_verifier)
print(code_verifier)

code_challenge = hashlib.sha256(code_verifier.encode('utf-8'))
#print(code_challenge)
code_challenge = code_challenge.digest()
#print(code_challenge)
code_challenge = base64.urlsafe_b64encode(code_challenge)
#print(code_challenge)
code_challenge = code_challenge.decode('utf-8')
#print(code_challenge)
code_challenge = code_challenge.replace('=', '')
print(code_challenge)