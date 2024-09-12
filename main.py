import functions_framework
import requests
import json
import logging
from google.cloud import bigquery
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

# SensorPush credentials
EMAIL = "poll.twelfth0k@icloud.com"
PASSWORD = "i9B9HiQRoT676odn"

# Default start date for the data fetch
START_DATE = "2024-07-19T00:00:00Z"

# Sensor ID to Name mapping
SENSOR_ID_NAME_MAPPING = {
    "469508.27177040722098928308": "Living Room Corner",
    "475020.27064303433454696280": "Front Sensor",
    "475030.1098946490148859685": "Bedroom Sensor"
}

# Room mapping based on sensor name
ROOM_MAPPING = {
    "Living Room Corner": "Living Room",
    "Front Sensor": "Entryway",
    "Bedroom Sensor": "Bedroom"
}

def get_authorization_token():
    url = 'https://api.sensorpush.com/api/v1/oauth/authorize'
    payload = {
        "email": EMAIL,
        "password": PASSWORD
    }
    headers = {"Content-Type": "application/json"}
    
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()  # Raises an error if the request fails
    return response.json()["authorization"]

def get_access_token(authorization_token):
    url = 'https://api.sensorpush.com/api/v1/oauth/accesstoken'
    payload = {"authorization": authorization_token}
    headers = {"Content-Type": "application/json"}
    
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()
    return response.json()["accesstoken"]

def get_last_timestamp(table_id):
    client = bigquery.Client(project="savvy-fountain-431023-t4")
    query = f"""
        SELECT MAX(observed) AS last_timestamp
        FROM `{table_id}`
    """
    result = client.query(query).result()
    for row in result:
        return row.last_timestamp

def fetch_data_from_sensorpush(access_token, start_time):
    url = "https://api.sensorpush.com/api/v1/samples"
    headers = {"Authorization": access_token, "accept": "application/json"}
    payload = {
        "sensors": list(SENSOR_ID_NAME_MAPPING.keys()),
        "limit": 100,
        "startTime": start_time,
        "stopTime": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()
    return response.json()

def process_and_store_data(data, table_id):
    client = bigquery.Client(project="savvy-fountain-431023-t4")
    rows_to_insert = []

    for sensor_id, readings in data['sensors'].items():
        sensor_name = SENSOR_ID_NAME_MAPPING.get(sensor_id, sensor_id)
        room = ROOM_MAPPING.get(sensor_name, "Unknown")

        for reading in readings:
            observed_time = datetime.strptime(reading["observed"], "%Y-%m-%dT%H:%M:%S.%fZ")
            
            # Data format to match the table schema
            row_data = {
                "observed": observed_time,
                "temperature": reading["temperature"],
                "humidity_percent": reading["humidity"] / 100,  # Convert to percentage
                "dewpoint": reading["dewpoint"],
                "vpd": reading["vpd"],
                "gateways": reading["gateways"],
                "sensor_name": sensor_name,
                "room": room,
                "datetime_field": observed_time  # Can be used for DATETIME-based queries
            }
            rows_to_insert.append(row_data)

    # Insert into BigQuery
    if rows_to_insert:
        client.insert_rows_json(table_id, rows_to_insert)
    else:
        logging.warning("No rows to insert into the table")

# Cloud Function entry point
@functions_framework.http
def gethumidityauto(request):
    table_id = "savvy-fountain-431023-t4.Humidity.all_data"

    try:
        # Step 1: Get authorization and access tokens
        authorization_token = get_authorization_token()
        access_token = get_access_token(authorization_token)

        # Step 2: Check for the last timestamp in the table
        last_timestamp = get_last_timestamp(table_id)
        start_time = last_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if last_timestamp else START_DATE
        logging.info(f"Start time for data fetch: {start_time}")

        # Step 3: Fetch data from SensorPush using hardcoded sensor IDs
        data = fetch_data_from_sensorpush(access_token, start_time)

        # Step 4: Process and store the data
        process_and_store_data(data, table_id)

        logging.info("Data sync complete")
        return "Data sync complete."

    except Exception as e:
        logging.error(f"Error in gethumidityauto function: {e}")
        return f"Error: {e}", 500
