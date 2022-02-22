from .login import *
from . import functions, database, table
import logging

"""
This is an example script of the usage of this ETL framework. The framework
is used to query an APIs endpoints, and insert the data to a database. Some
manual configuration needs to be done in the functions.get_endpoint_data(), which
is provided as more of an example of the process.
"""

# meta information about the API

# base url for requests

def run(sessionId, schema_name):

    base_url = "https://85.134.52.210:8001/sv/003_1.1/api/v1"
                
    # session id for authentication
    sessionId = sessionId

    # different endpoints to fetch
    endpoints = [
        "Accounting/VoucherNumberSeries","Accounting/VoucherRows","Accounting/Vouchers", "Accounting/VoucherSeries","Accounting/Accounts",
        "Accounting/AccountsPayables","Accounting/AccountsReceivables","Accounting/BookingRows","Accounting/Bookings","Accounting/CodingDimensions",
        "Accounting/CodingElements","Accounting/CodingEntries","Accounting/CodingEntryElements","Accounting/CodingRows","Accounting/Codings","Common/Addresses",
        "Common/ApplicationUserPrinters"
        ]
    #for testing purposes
    #endpoints = ["Accounting/Accounts"] 

    limit = 10000

    params = {
        "$top": limit,  # how many rows to get
        "$skip": 0    # first row to get
    }

    print(params["$skip"] == 0)
  

    # next loop through the endpoints, get data, create tables, insert
    for endpoint in endpoints:
    
        endpoint_data = functions.get_batch_endpoint_data(base_url, endpoint, sessionId, params)
        i = 0
        while endpoint_data:
            i += 1
            print(f"batch {i} for endpoint {endpoint} fetched") 
            # after getting all the data, we can insert them to the db

            if params["$skip"] == 0: # first itearation only now create table
                # # use the table object to create a table to db
                # create the table object, specify as temp table and add random noise to the table name
                # first make a connection to a database
                # get the necessary information from a secure source
                kvClient = functions.keyvault_connection('monitoretlkeyvault')
                server = kvClient.get_secret('server').value
                database = kvClient.get_secret('database').value
                username = kvClient.get_secret('peterEtl').value
                password = kvClient.get_secret('peterEtlpw').value
            
                connection = functions.db_connection(username=username, password=password, database=database, server=server)

                # configure schema name
                schema_name = schema_name
                # table name
                table_name = endpoint.replace("/", "")#.split('?')[0]
                table_name = "monitor_" + table_name
                 
                # init sql parameters based on your data
                table_params = functions.init_table_parameters(endpoint_data)
                # # use the table object to create a table to db
                table2 = table.Table(table_name, schema_name, table_params, temp=True, randomize=True)
                table2.create_to_db(connection, drop_existing=True)

            # # insert all data (list of jsons), use default chunk size (100)
            print(f'Started inserting batch {i} data into table {table_name}')
            table2.insert_data(endpoint_data, table_params, connection)
            print(f'Finnished inserting batch {i} data into table {table_name}')
            #update parameters and fetch more data
            print('Updating parameters to fetch more data')
            params["$skip"] = params["$skip"] + limit

            endpoint_data = functions.get_batch_endpoint_data(base_url, endpoint, sessionId, params)
            

        # # replace temp table in db as just pure 'table_name' (no temp_ or random integers), drop possible existing table
        table2.rename(connection, table_name, drop_existing=True)
        print(f'Renamed table {table_name}')
        print('\n')
        endpoint_data = None

    return "Success"



