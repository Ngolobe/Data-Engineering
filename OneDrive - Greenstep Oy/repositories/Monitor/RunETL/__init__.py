# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
# fix absolute imports

import sys
from ..Tools import login

from os import path

sys.path.append(path.dirname(path.dirname(__file__)))

import logging
from ..Tools import run, functions
# this is an example function which starts the etl process with the provided credentials
def main(creds: dict):
    # take data to run from creds

    kvClient = functions.keyvault_connection('monitoretlkeyvault')
    SessionId = login.get_session_id()
    schema_name = kvClient.get_secret('shema').value
    # start the process
    run.run(SessionId, schema_name)
    return ["Done"]


