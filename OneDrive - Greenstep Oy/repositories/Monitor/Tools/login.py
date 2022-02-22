import requests
import json
from urllib3.exceptions import InsecureRequestWarning
from . import functions


def get_session_id():
  url = "https://85.134.52.210:8001/sv/003_1.1/login"
  kvClient = functions.keyvault_connection('monitoretlkeyvault')
  Username = kvClient.get_secret('loginUserName').value
  Password = kvClient.get_secret('loginPW').value
  
  body = {
    "Username": f"{Username}",
    "Password": f"{Password}"
    }

  headers = {
    'Content-Type': 'application/json'
    }
  # Suppress only the single warning from urllib3 needed.
  requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
  response = requests.request("POST", url, headers=headers, data=json.dumps(body),verify=False)
  creds = {}
  creds["$id"] = json.loads(response.text)['$id']
  creds["SessionId"] = json.loads(response.text)['SessionId']
  response.close()
  return creds["SessionId"]

def save_new_creds(my_dict):
  with open('creds.json', 'w') as f:
    json.dump(my_dict, f)

def load_creds():
  with open('creds.json', 'r') as f:
    creds = json.load(f)
  return creds
         
save_new_creds(get_session_id())




