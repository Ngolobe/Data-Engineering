import logging
import requests
import os
import json
from datetime import datetime
from ..Tools import database
import azure.functions as func

# this function triggers the nightly refresh of all Antell apis
def main(mytimer: func.TimerRequest) -> None:
    code = "" #os.environ["API_KEY"]
    url = f"https://moomincampaigns.azurewebsites.net/api/orchestrators/RunWithCreds?code={code}"
    logging.info("Fetching credits")
    # connect to the 'GreenstepSql' database and make a cursor
    connection = database.db_connection_one_click_wonder("GreenstepSql")
    cursor = connection.cursor()
    # fetch credits from 'Quickbooks_Companies'
    query = "SELECT id, schema_name, creds FROM [one_click_wonder].[Facebook_Ads_Companies]"
    ret = cursor.execute(query)
    data = ret.fetchall()
    cursor.close()
    connection.close()
    # data is in a list of tuples, since it is always in the same form, loop tuples and start processes
    for row in data:
        c = {
            "id": row[0],
            "schema_name": row[1],
            "credits": json.loads(row[2]),
            "database_name": "bibooktestintegrations"
        }
        logging.info(f"Got the credits for: {c['schema_name']}")
        # send a http post trigger to the url, which then orchestrates the ETL refresh
        r = requests.post(url=url, json=c) #c should be the sessionId
        if r.status_code in [202, 200]:
            logging.info(f"Nighly trigger succesfull for {c['schema_name']}")
            logging.info(r.content)
        else:
            logging.info("Something wrong with triggering: ")
            logging.info(r.status_code)
            logging.info(r.content)
            raise Exception(f"Triggering failed for {c['schema_name']}")