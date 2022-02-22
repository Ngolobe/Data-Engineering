import regex as re
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry  # type: ignore (ignore pylance: import could not be resolved)
from datetime import datetime, date, time
from time import sleep
import json
import pyodbc
import logging
from urllib3.exceptions import InsecureRequestWarning
#import config
#from ..Tools import table

def keyvault_connection(keyvault):

  from azure.identity import DefaultAzureCredential
  from azure.keyvault.secrets import SecretClient

  credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
  client = SecretClient(vault_url=f'https://{keyvault}.vault.azure.net', credential=credential)

  return client

# creates a pyodbc connection to a database
def db_connection(username, password, database, server):
    driver = "{ODBC Driver 17 for SQL Server}"

    # make a connection to the database
    connection = pyodbc.connect(
        f"DRIVER={driver};SERVER={server},1433;DATABASE={database};UID={username};\
                                  PWD={password}",
        autocommit=False,
    )

    return connection


def get_endpoint_data(base_url, endpoint, sessionId, extra_params=None):
    """Queries an endpoint of a given api for data. This is a skeleton version, so
       one must configure the paging, authentication etc. for themselves.

    Args:
        base_url (str): The common part of the url for endpoints
        endpoint (str): The endpoints url part of the full url to query
        auth (any): Anything you need to have for authentication for the API,
                    needs to be configured.
        extra_params (dict, optional): Possible extra query parameters for the requests. Give
                                       in the format of {"param": value, }. Defaults to None.
    Raises:
        Exception: On five consequent HTTP errors

    Returns:
        [list]: The endpoints data in a list of dicts.
    """

    # TODO: validate correct form for your integration
    # construct url for the api requests
    url = f"{base_url}/{endpoint}"

    # TODO: validate correct authentication for your integration
    headers = {
        "X-Monitor-SessionId": f"{sessionId}"
    }

    # TODO: validate correct param names
    # parameters for the request

    # amount of rows to fetch per request
    limit = 10000
    params = {
        "$top": limit,  # how many rows to get
        "$skip": 0    # first row to get
    }

    # update possible extra params to requests parameters
    if extra_params:
        params.update(extra_params)


    # gather data here
    ret = []

    while True:
        # Suppress only the single warning from urllib3 needed.
        #requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        # GET
        response = requests.get(url, headers=headers,params=params,verify=False)
        # if something wrong with request, try again a few times
        count = 1
        while not response.ok:
            # if error with the request itself, return
            if response.status_code in [404]:
                return response.status_code

            # if request fails 5 times in a row, raise exception
            if count == 5:
                raise Exception(f"Request failed 10 times in a row, last error: {response.status_code}")

            print(
                f"Request failed with code: {response.status_code}, sleeping and retrying"
            )
            # sleep
            sleep(5)

            # new request
            response = requests.get(url, headers=headers,params=params,verify=False)

            # increment
            count += 1

        # load data to json
        data = json.loads(response.content) #['Result']
        #print(type(data))
        # add data to list
        ret += data

        # if full page, increment paging and get more
        if len(data) == limit:
            # TODO: validate correct paging increment for your integration
            # else increment count, 'page' and continue
            count += 1
            params["$skip"] = params["$skip"] + limit

        # if not a full page, then no more data in endpoint
        else:
            break

    #logging.info(f"Endpoint data for endpoint {endpoint} fetched")
    print(f"Endpoint data for endpoint {endpoint} fetched")         
    # return fetched data in a list of dictionaries
    return ret

def get_batch_endpoint_data(base_url, endpoint, sessionId, params):
    """Queries an endpoint of a given api for data. This is a skeleton version, so
       one must configure the paging, authentication etc. for themselves.

    Args:
        base_url (str): The common part of the url for endpoints
        endpoint (str): The endpoints url part of the full url to query
        auth (any): Anything you need to have for authentication for the API,
                    needs to be configured.
        extra_params (dict, optional): Possible extra query parameters for the requests. Give
                                       in the format of {"param": value, }. Defaults to None.
    Raises:
        Exception: On five consequent HTTP errors

    Returns:
        [list]: The endpoints data in a list of dicts.
    """

    # TODO: validate correct form for your integration
    # construct url for the api requests
    url = f"{base_url}/{endpoint}"

    logging.info(f"{base_url}, {endpoint}, {sessionId}, {params}")

    # TODO: validate correct authentication for your integration
    headers = {
        "X-Monitor-SessionId": f"{sessionId}"
    }

    # TODO: validate correct param names
    # parameters for the request
    params = params
    
    # gather data here
    response = requests.get(url, headers=headers,params=params,verify=False)
    logging.info(response.content)
    logging.info(response.headers)
    logging.info(response.status_code)
    logging.info(type(sessionId))


    count = 1

    while not response.ok:
        # if error with the request itself, return
        if response.status_code in [404]:
            return response.status_code

        # if request fails 5 times in a row, raise exception
        if count == 5:
            raise Exception(f"Request failed 10 times in a row, last error: {response.status_code}")

        print(
            f"Request failed with code: {response.status_code}, sleeping and retrying"
        )
        # sleep
        sleep(5)

        # new request
        response = requests.get(url, headers=headers,params=params,verify=False)

        # increment
        count += 1

    data = json.loads(response.content)
        
    # return fetched data in a list of dictionaries
    return data


def init_table_parameters(data: list):
    """
    Loops through the elements of data and the rows of the elements identifying each value
    to a correct SQL datatype and keeping track of all unique keys present. Chooses the most
    frequently identified SQL datatype for each key in data.

    Args:
        data (list): List of dictionaries containing any data.

    Returns:
        (dict): A dictionary containing all the different keys of data with correct SQL
                datatypes as values.
    """

    # init empty dict for parameters
    parameters = {}

    # loop dictionaries
    for d in data:

        # loop keys and values of dictionary
        for key, value in d.items():

            # if key not already in parameters, init empty list
            if key not in parameters:
                parameters[key] = []

            # identify the values correct SQL datatype and append it to the list
            parameters[key].append(identify(value))

    # loop all different parameters found in the data
    for parameter, datatype_list in parameters.items():

        # sort list in decending order by item frequency
        temp = sorted(datatype_list, key=datatype_list.count, reverse=True)

        # drop all 'None' values
        filtered = list(filter(lambda x: x != "None", temp))

        # choose the head of the list (most frequent datatype) as the datatype for this parameter, varchar if empty
        if len(filtered) > 0:
            parameters[parameter] = filtered[0]
        else:
            parameters[parameter] = "Varchar (8000)"

    # return the parameters
    return parameters


def get_parameters_from_table(schema: str, table_name: str, connection):

    # query to get needed information from table
    datatype_q = f"""
            SELECT
                c.name 'Column Name',
                t.Name 'Data type',
                c.max_length 'Max Length',
                c.precision ,
                c.scale ,
                c.is_nullable,
                ISNULL(i.is_primary_key, 0) 'Primary Key'
            FROM
                sys.columns c
            INNER JOIN
                sys.types t ON c.user_type_id = t.user_type_id
            LEFT OUTER JOIN
                sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            LEFT OUTER JOIN
                sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            WHERE
                c.object_id = OBJECT_ID('{schema}.{table_name}')
                """

    # make cursor, execute, fetch data, close cursor
    cursor = connection.cursor()
    cursor.execute(datatype_q)
    data = cursor.fetchall()
    cursor.close()

    # loop data and extract relevant information to 'ret'
    ret = {}
    for row in data:
        # data always in same form
        column = row[0]
        datatype = row[1]
        # if varchar then get bit length also
        if datatype == 'varchar':
            datatype = f'varchar ({row[2]})'

        # capitalize datetime columns (fix this later)
        elif datatype == 'datetime':
            datatype = 'Datetime'
        elif datatype == 'date':
            datatype = 'Date'
        elif datatype == 'time':
            datatype = 'Time'

        # add to ret, 'column_name': 'datatype'
        ret[column] = datatype

    return ret


# identify a single element into a SQL data type
def identify(value):

    # identify type
    _type = type(value)

    # if no value
    if not value and _type is not bool:
        return "None"

    # if str, check for datetimes with regex
    elif _type is str:

        # datetime
        datetime_pattern = r"\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[1-2]\d|3[0-1])T(?:[0-1]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+|)(?:Z|(?:\+|\-)(?:\d{2}):?(?:\d{2}))"  # noqa: E501 (didn't find a way to split regex string to multiple lines)
        # date
        date_pattern = r"^\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[0-1])$"
        # time
        time_pattern = r"^[0-2][0-9][:][0-5][0-9][:][0-5][0-9]"

        # return correspoding type if matches, else varchar (8000)
        if re.match(datetime_pattern, value):
            return "Datetime"
        elif re.match(date_pattern, value) and len(value) < 12:
            return "Date"
        elif re.match(time_pattern, value) and len(value) < 15:
            return "Time"
        # regular string
        else:
            return "varchar (8000)"

    # map rest of the datatypes to corresponding SQL types
    datatypes = {bool: "bit", int: "int", float: "float", list: "varchar (8000)", dict: "varchar (8000)"}

    # return the correct type
    return datatypes[_type]


def convert_data(data: list, table_parameters: dict):
    """
    Converts a list of data to SQL friendly format

    Args:
        data (list): [description]
        table_parameters (dict): [description]

    Returns:
        [type]: [description]
    """

    # init empty list for converted data
    converted_data = []

    # loop through the dictionaries in data
    for d in data:
        # init an empty dict for the converted data
        temp = {}
        # loop through the all keys and datatypes identified for this table
        for key, datatype in table_parameters.items():
            # get value from row if present
            value = d.get(key)
            temp[key] = __convert(value, datatype)

        # append the converted dict to the main list
        converted_data.append(temp)

    # return the converted data
    return converted_data


def __convert(element, datatype):

    # get python datatype also
    _type = type(element)

    # if no value
    if _type is not bool and not element:
        return None

    # lists
    if _type is list:
        # wrap to str and return
        return ("{}".format(element),)

    # dict
    elif _type is dict:
        # wrap to square brackets
        return (("[{}]".format(element)),)

    # booleans
    elif _type is bool:
        if element:
            # True
            return (1,)
        else:
            # False
            return (0,)

    # convert datetimes, dates and times into python Datetime objects
    elif datatype in ["Datetime", "Date", "Time"]:
        return (__convert_datetime(element, datatype),)

    # else just return the element
    else:
        return (element,)


# converts a datetime, date or time string to Python datetime object
def __convert_datetime(element: str, form: str):

    # if element is None return None
    if not element:
        return None

    if form == "Datetime":
        # if element contains timezone information etc, strip them
        if len(element) != 19:
            element = element[:19]
        # replace possible 'T' with a space
        element = element.replace("T", " ")
        return datetime.strptime(element, "%Y-%m-%d %H:%M:%S")
    elif form == "Time":
        # strip milliseconds, timezone
        if len(element) != 8:
            element = element[:8]
        return time.fromisoformat(element)
    elif form == "Date":
        return date.fromisoformat(element)
    else:
        raise Exception(f"Invalid values passed to convert_datetime: {element, form}")


def get_schema_name_from_db(company_name: str, table_name: str, schema_name: str, connection):
    """Fetches the companies schema name from db.

    Args:
        company_name (str): The name of the company to fetch the schema name for
        table_name (str): The name of the table that holds the integration companies data
        schema_name (str): The name of the schema where this table lives
        connection (): PYODBC connection to the DB

    Returns:
        str: Schema name
    """

    # get schema name from db
    q = f"SELECT schema_name FROM [{schema_name}].[{table_name}] WHERE company_name = '{company_name}'"

    # connection, cursor
    cursor = connection.cursor()

    cursor.execute(q)
    res = cursor.fetchall()

    # schema name from first index of first row
    schema = res[0][0]

    # close cursor
    cursor.close()

    return schema


# creates a pyodbc connection to a database
def db_connection(server, database, username, password):
    driver = "{ODBC Driver 17 for SQL Server}"

    # make a connection to the database
    connection = pyodbc.connect(
        f"DRIVER={driver};SERVER={server},1433;DATABASE={database};UID={username};\
                                  PWD={password}",
        autocommit=False,
    )

    return connection
