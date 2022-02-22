import pyodbc
import os
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient


# creates a pyodbc connection to a databse
def db_connection(database: str):
    """
    makes a connection to the db where the data is destined
    """
    driver = '{ODBC Driver 17 for SQL Server}'

    # get server from enviroment variables
    server = os.environ["DB_SERVER_URL"]

    # Create a secret client for the key vault
    keyvault = os.environ["KEY_VAULT_NAME"]
    credential = ManagedIdentityCredential()
    client = SecretClient(vault_url=f'https://{keyvault}.vault.azure.net', credential=credential)

    # get username and pw from keyvault
    username = client.get_secret('peterETL').value
    password = client.get_secret('peterETLpw').value

    # make a connection to the database
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password, autocommit=False)
    return connection

def db_connection_one_click_wonder(database: str):
    """
    makes a connection to the db where we query api specific credentials
    """
    driver = '{ODBC Driver 17 for SQL Server}'

    # get server from enviroment variables
    server = os.environ["DB_SERVER_URL"]

    # Create a secret client for the key vault
    keyvault = os.environ["KEY_VAULT_NAME"]
    credential = ManagedIdentityCredential()
    client = SecretClient(vault_url=f'https://{keyvault}.vault.azure.net', credential=credential)
    
    # get username and pw from keyvault
    username = client.get_secret('GreenstepOCWUser').value
    password = client.get_secret('GreenstepOCWPassword').value
    
    # make a connection to the database
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE=' +
                                database+';UID='+username+';PWD=' + password, autocommit=False)
    
    return connection