'''Logic of the script:
Extract all the credentials from JSON
Iterate the dictionary of those values, keep a count of it and start calling API's:
    Get the PIT
    Pass PIT in the search_after function
    Inside the search_after function:
    - call extract_fqdn funtion to extract fqdns and put them in list
    - call write_queries function and pass the list of fqdns to write it in csv file
    Call find_last_key_value to get the value of a sort key
    Check if the last_sort_value is None: if it's True, move to the next hostname if any
    If the above False: start an infinite loop, keep count of loops,check the condition of the last_sort_value
    and call looping_search_after function
    Inside the looping_search_after funtion:
    - call extract_fqdn funtion to extract fqdns and put them in list
    - call write_queries function and pass the list of fqdns to write it in csv file
    - call find_last_key_value to get the value of a sort key
    - increment the loop_number variabl
    If last_sort_value is None: stop calling the funtion
    The loop will be ended, if the count of left hostnames is less then 1
'''
import json
import csv
from datetime import date, timedelta
import datetime
import subprocess
import logging
import requests


logging.basicConfig(filename='example.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
today = date.today()
yesterday = today - timedelta(days=1)
FROM_DATE = yesterday.strftime("%Y-%m-%d")
TO_DATE = today.strftime("%Y-%m-%d")
original_csv_file = f'dns_result_original_{FROM_DATE}.csv'

with open('credentials.json', 'r') as json_file:
    credentials = json.load(json_file)

#extact all the values from json
password_hostname1 = credentials['password_hostname1']
# password_hostname2 = credentials['password_hostname2']
# password_hostname3 = credentials['password_hostname3']
# password_hostname4 = credentials['password_hostname4']
user = credentials['user']
url_hostname1 = credentials['url_hostname1']
# url_hostname2 = credentials['url_hostname2']
# url_hostname3 = credentials['url_hostname3']
# url_hostname4 = credentials['url_hostname4']


#for how long do we need the keep alive?
def get_pit():
    """
    Retrieve a PIT (Point-In-Time) value to be passed to the search_after function.

    This function sends a request to a specified URL to obtain a PIT value for a specific data query.
    The PIT is used for querying data in a Point-In-Time state.

    Returns:
        str or None: The PIT value retrieved from the API, or None if the request fails.
    """
    full_url = f"{url}/secured_eaas_stg-jpe2b_dns_queries-*/_pit?keep_alive=10m"
    auth = (user, password)
    headers = {"Content-Type": "application/json"}
    response = requests.post(full_url, auth=auth, headers=headers, timeout=100)
    if response.status_code == 200:
        logging.info('get_pit(): Request to get a PIT was successful.')
        api_response = response.text
        response_data_pit = json.loads(api_response)
        pit = response_data_pit.get("id")
    else:
        logging.error(f"get_pit(): Request to get a PIT failed with status code {response.status_code}.")
        return None
    return pit


def extract_fqdn(data):
    """
    Recursively extract FQDNs from nested dictionaries and lists.
    This function searches through a JSON-like data structure to find and collect FQDNs.

    Args:
        data (dict or list): The input data structure to search for FQDNs.

    Returns:
        list: A list of extracted FQDN values.

    Note:
        - FQDNs are identified based on the presence of the key "fqdn" in dictionaries.
        - This function uses recursion to traverse nested data structures.
    """
    def recursive_extract(data, results):
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "fqdn":
                    results.append(value)
                else:
                    recursive_extract(value, results)
        elif isinstance(data, list):
            for item in data:
                recursive_extract(item, results)
    fqdn_values = []
    recursive_extract(data, fqdn_values)
    logging.info('extract_fqdn(): FQDN extraction went successful.')
    return fqdn_values


def find_last_key_value(last_response_data):
    """
    Recursively find and extract the last occurrence of a specified key ('sort') and its associated value
    in a JSON-like data structure.

    Args:
        last_response_data (dict or list): The JSON-like data structure to search in.

    Returns:
        The value associated with the last occurrence of the specified key, or None if the key is not found.

    Note:
        - This function uses recursion to traverse nested data structures.
        - If the specified key is found in multiple places within the data structure, only the value from the last occurrence is returned.
    """
    try:
        search_key = 'sort'
        if isinstance(last_response_data, dict):
            for key, value in last_response_data.items():
                if key == search_key:
                    return value
                elif isinstance(value, (dict, list)):
                    result = find_last_key_value(value)
                    if result is not None:
                        logging.info(f"find_last_key_value(): Found and extracted sort values using dict {result}")
                        return result
        elif isinstance(last_response_data, list):
            for item in reversed(last_response_data):
                result = find_last_key_value(item)
                if result is not None:
                    logging.info(f"find_last_key_value(): Found and extracted sort values using list {result}")
                    return result
    except ValueError as e:
        logging.warn(f"find_last_sort_value(): {e}")
        return None


def write_queries(fqdn_values):
    """
    Write Fully Qualified Domain Names to a CSV file.
    This function takes a list of FQDNs and writes them to a specified CSV file.

    Args:
        fqdn_values (list): A list of FQDN values to be written to the CSV file.

    Returns:
        None

    Note:
        - If 'fqdn_values' is an empty list, no data is written to the CSV file.
        - The CSV file is opened in 'append' mode to add data to the existing file.
    """
    if fqdn_values:
        with open(original_csv_file, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for value in fqdn_values:
                writer.writerow([value])
        logging.info('write_queries(): Wrote the chunck of fqdns into the csv file')
    logging.info('write_queries(): The list of FQDN values is empty, skipping writing in down')


def search_after(pit):
    """
    Perform a search request with authentication and process the response.

    This function sends a POST request to a specified URL with authentication credentials and data.
    It handles the response, calls the function to extract FQDNs from the data,then calls the function to write them to a CSV file.

    Args:
        url_full_search (str): The URL to send the search request to.
        user (str): The username for authentication.
        password (str): The password for authentication.
        data (dict): The data to send in the request body as JSON.
        headers (dict): Headers to include in the request.

    Returns:
        tuple: A tuple containing the response data as a JSON object and loop number.

    Note:
        - This function expects a successful response with a status code of 200.
        - It extracts FQDNs from the response data using the 'extract_fqdn' function.
        - FQDNs are written to a CSV file using the 'write_queries' function.
    """
    url_full_search= f'{url}/_search?pretty'
    headers = {"Content-Type": "application/json"}
    data = {
    "size": 10000,
    "query": {
        "bool": {
        "must": [],
        "filter": [
            {
            "range": {
                "@timestamp": {
                "format": "strict_date_optional_time",
                "gte": "2023-09-27T00:00:00",
                "lte": "2023-09-27T00:00:10"
                }
            }
            }
        ],
        "should": [],
        "must_not": []
        }
    },
    "pit": {
        "id":pit,
        "keep_alive": "1m"
    },
    "sort": [
        {"@timestamp": {"order": "asc", "format": "strict_date_optional_time_nanos", "numeric_type" : "date_nanos" }}
    ]
    }
    auth = (user, password)
    timeout_seconds = 100
    response = requests.post(url = url_full_search, headers=headers, auth=auth, data=json.dumps(data), timeout=timeout_seconds)
    if response.status_code == 200:
        logging.info('search_after(): Response status code == 200')
        response_data = response.json()
        fqdn_values = extract_fqdn(response_data)
        write_queries(fqdn_values)
        loop_number = 0
        logging.info('search_after(): Completed the first data extraction request, moving to the search_after loop')
    else:
        logging.error('search_after(): Something went wrong with the first data extraction request')
    return response_data, loop_number


def looping_search_after(last_sort_value, pit, loop_number, hostname):
    """
    Perform a looping search request with authentication and process the response.

    This function sends a POST request to a specified URL with authentication credentials and data.
    It handles the response,calls the function to extract FQDNs from the data,
    and calls the function to write them to a CSV file. Additionally, it retrieves the last sort value from the response.

    Args:
        full_url (str): The full URL to send the search request to.
        user (str): The username for authentication.
        params (dict): Parameters including 'password' for authentication.
        data (dict): The data to send in the request body as JSON.
        headers (dict): Headers to include in the request.
        loop_number (int): The current loop number.

    Returns:
        tuple: A tuple containing the response data as a JSON object, the last sort value, and the updated loop number.

    Note:
        - This function expects a successful response with a status code of 200.
        - It extracts FQDNs from the response data using the 'extract_fqdn' function.
        - The last sort value is retrieved using the 'find_last_key_value' function.
        - FQDNs are written to a CSV file using the 'write_queries' function.
    """
    full_url= f'{url}/_search?pretty'
    first_value = str(last_sort_value[0])
    second_value = str(last_sort_value[1])

    headers = {"Content-Type": "application/json"}
    data = {
        "size": 10000,
        "query": {
            "bool": {
            "must": [],
            "filter": [
                {
                "range": {
                    "@timestamp": {
                    "format": "strict_date_optional_time",
                        "gte": "2023-09-27T00:00:00",
                        "lte": "2023-09-27T00:00:10"
                    }
                }
                }
            ],
            "should": [],
            "must_not": []
            }
        },
        "pit": {
            "id": pit,
            "keep_alive": "10m"
        },
        "sort": [
            {"@timestamp": {"order": "asc", "format": "strict_date_optional_time_nanos", "numeric_type" : "date_nanos" }}
        ],
        "search_after": [
            first_value,
            second_value
            ]
        }
    fqdn_values = []
    password = params["password"]
    auth = (user, password)
    timeout_seconds = 100
    response = requests.post(url = full_url, headers=headers, auth=auth, data=json.dumps(data), timeout=timeout_seconds)
    if response.status_code == 200:
        response_data = response.json()
        fqdn_values = extract_fqdn(response_data)
        last_sort_value = find_last_key_value(response_data)
        write_queries(fqdn_values)
        loop_number += 1
        logging.info(f'looping_search_after(): Completed the loop number {loop_number}')
    return response_data, last_sort_value, loop_number


if __name__ == "__main__":
    month_file_name='september_2023.csv'
    endpoints = {
    "hostname1": {
        "url": url_hostname1,
        "password": password_hostname1
    },
    "hostname2": {
        "url": url_hostname1,
        "password": password_hostname1
    },
    # "hostname3": {
    #     "url": url_hostname3,
    #     "password": password_hostname3
    # },
    # "hostname4": {
    #     "url": url_hostname4,
    #     "password": password_hostname4
    # }
                }
    total_endpoints = len(endpoints)
    for index, (hostname, params) in enumerate(endpoints.items()):
        logging.info('Started to collect the data for %s', hostname)
        url = params['url']
        password = params["password"]
        pit = get_pit()
        response_data, loop_number = search_after(pit)
        last_sort_value = find_last_key_value(response_data)
        if last_sort_value is None:
            break
        while True:
            if last_sort_value is None:
                break
            response_data, last_sort_value, loop_number = looping_search_after(last_sort_value, pit, loop_number, hostname)
        if index >= total_endpoints - 1:
            break  # Exit the for loop
         # Continue to the next iteration of the for loop
        continue
    logging.info('Completed the collection from all hosts')
