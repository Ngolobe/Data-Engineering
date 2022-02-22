# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging
import json
import azure.functions as func
import azure.durable_functions as df


# This main function is triggered by an HTTP request
async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:

    # try retrieving the body from the request
    try:
        body = json.loads(req.get_body())
    except:
        # no body
        logging.info("No body provided in the request")
        body = {}

    # client for an orchestration
    client = df.DurableOrchestrationClient(starter)
    
    # call the orchestration function provided in the request with the provided body
    instance_id = await client.start_new(req.route_params["functionName"], None, body)

    # the function starts the orchestration process and continues here to log and return instanse ids
    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return client.create_check_status_response(req, instance_id)