# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json
import azure.functions as func
import azure.durable_functions as df


# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# This function triggers the ETL process with creds provided in the "context" body
def orchestrator_function(context: df.DurableOrchestrationContext):
    # get creds from body
    creds = context.get_input()
    # start etl process with creds
    result1 = yield context.call_activity('RunETL', creds)
    return result1


main = df.Orchestrator.create(orchestrator_function)