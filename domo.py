 """A simple script that creates a Domo dataset, updates it, quierying the data, getting the Domo token""" 
import requests
import json
import os
import logging
from pydomo import Domo
from pydomo.datasets import UpdateMethod, DataSetRequest, Schema, Column, ColumnType, Policy

logger = logging.getLogger()
logging.basicConfig()
logger.setLevel(logging.INFO)

client_id = os.getenv('DOMO_CLIENT_ID')
secret = os.getenv('DOMO_CLIENT_SECRET')
dataset_id=os.getenv('TESTING_DATASET_ID')
api_host = "api.domo.com"

domo = Domo(client_id, secret, api_host )
datasets = domo.datasets
def create_dataset():
    domo.logger.info("\n**** Domo API - DataSet Examples ****\n")
    file_path='./FILEPATH.csv'
    # Define a DataSet Schema
    dsr = DataSetRequest()
    dsr.name = 'Testing dataset'
    dsr.description = 'dataset for testing purposes'
    dsr.schema = Schema([Column(ColumnType.STRING, 'Name'),
                        Column(ColumnType.STRING, 'Surmane'),
                        Column(ColumnType.STRING, 'Phone number')])

    # Create a DataSet with the given Schema
    dataset = datasets.create(dsr)
    domo.logger.info("Created DataSet " + dataset['id'])

    # Get a DataSets's metadata
    retrieved_dataset = datasets.get(dataset['id'])
    domo.logger.info("Retrieved DataSet " + retrieved_dataset['id'])

    # Import Data from a file
    datasets.data_import_from_file(dataset['id'], file_path,update_method= UpdateMethod.APPEND)
    domo.logger.info("Uploaded data from a file to DataSet {}".format(
                                                            dataset['id']))

def update_dataset():
    file_path='./DIFF_FILE_PATH.csv'
    datasets.data_import_from_file(dataset_id, file_path,update_method= UpdateMethod.APPEND)

def get_token():
    scopes = 'data workflow audit buzz user account dashboard'
    scopes_encoded = ' '.join(scopes.split()).replace(' ', '%20')
    payload = {
        'grant_type': 'client_credentials',
        'scope': scopes_encoded
    }
    url="https://api.domo.com/oauth/token?grant_type=client_credentials&scope=data"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(url, auth=(client_id, secret), data=payload, headers=headers)
    access_token = json.loads(response.text)["access_token"]
    return access_token

def query_api(access_token):
    url = f'https://api.domo.com/v1/datasets/query/execute/{dataset_id}'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'bearer {access_token}'
    }
    payload = {
        'sql': 'SELECT * FROM table'
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    print(response)

if __name__ == "__main__":
    try:
        access_token=get_token(client_id, secret)
        query_api(access_token,dataset_id)
    except Exception as e:
        logger.error(e)
