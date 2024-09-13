import os
from google.cloud import bigquery
import axios
from datetime import datetime, timedelta

# SensorPush API credentials (retrieved from environment variables)
EMAIL = os.environ.get('EMAIL')
PASSWORD = os.environ.get('PASSWORD')

# Sensor IDs to fetch data from
SENSOR_IDS = [
    "469508.27177040722098928308",
    "475020.27064303433454696280",
    "475030.1098946490148859685",
]

# BigQuery table details
PROJECT_ID = "savvy-fountain-431023-t4"  # Replace with your project ID
DATASET_ID = "Humidity"
TABLE_ID = "all_data"

# Sensor and room mappings
SENSOR_ID_NAME_MAPPING = {
    "469508.27177040722098928308": "Living Room Corner",
    "475020.27064303433454696280": "Front Sensor",
    "475030.1098946490148859685": "Bedroom Sensor",
}
ROOM_MAPPING = {
    "Living Room Corner": "Living Room",
    "Front Sensor": "Entryway",
    "Bedroom Sensor": "Bedroom",
}


def get_access_token():
    """Authenticates with the SensorPush API and returns an access token."""
    try:
        # Step 1: Get authorization code
        response = axios.post(
            "https://api.sensorpush.com/api/v1/oauth/authorize",
            json={"email": EMAIL, "password": PASSWORD},
            headers={"Content-Type": "application/json"},
        )
        authorization = response.data.authorization

        # Step 2: Get access token
        response = axios.post(
            "https://api.sensorpush.com/api/v1/oauth/accesstoken",
            json={"authorization": authorization},
            headers={"Content-Type": "application/json"},
        )
        return response.data.accesstoken
    except axios.exceptions.RequestException as e:
        print(f"Error communicating with SensorPush API: {e}")
        return None
    except KeyError as e:
        print(f"Unexpected API response format: {e}")
        return None
    except Exception as e:
        print(f"Error getting access token: {e}")
        return None


def fetch_sensor_data(access_token, start_time=None):
    """Fetches sensor data from the SensorPush API."""
    try:
        url = "https://api.sensorpush.com/api/v1/samples"
        headers = {"Authorization": access_token, "Content-Type": "application/json"}
        data = {"sensors": SENSOR_IDS}

        # Add start_time parameter if provided (for fetching data since a specific time)
        if start_time:
            data["start"] = int(start_time.timestamp())

        response = axios.post(url, headers=headers, json=data)
        return response.data
    except Exception as e:
        print(f"Error fetching sensor data: {e}")
        return None


def process_sensor_data(sensor_data):
    """Processes the raw sensor data into the desired format."""
    processed_data = []
    for sensor_id, readings in sensor_data.get("sensors", {}).items():
        sensor_name = SENSOR_ID_NAME_MAPPING.get(sensor_id)
        room = ROOM_MAPPING.get(sensor_name)
        for reading in readings:
            processed_data.append(
                {
                    "observed": datetime.fromtimestamp(reading["observed"]),
                    "temperature": reading["temperature"],
                    "humidity_percent": reading["humidity"] / 100,
                    "dewpoint": reading["dewpoint"],
                    "vpd": reading["vpd"],
                    "gateways": str(reading["gateways"]),  # Convert list to string
                    "sensor_name": sensor_name,
                    "room": room,
                }
            )
    return processed_data


def insert_data_into_bigquery(data):
    """Inserts the processed data into BigQuery."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        table = client.get_table(table_ref)   

        errors = client.insert_rows_json(table,    data)
        if errors == []:
            print("New rows have been added.")
        else:
            print(f"Encountered errors while inserting rows: {errors}")
    except Exception as e:
        print(f"Error inserting data into BigQuery: {e}")


def humidity_data_to_bigquery(request):
    """Cloud Function entry point."""
    access_token = get_access_token()
    if not access_token:
        return "Failed to authenticate with SensorPush API"

    # Get the most recent "observed" timestamp from BigQuery (if any)
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
            SELECT MAX(observed) 
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        """
        query_job = client.query(query)
        result = query_job.result()
        latest_timestamp = list(result)[0][0]  # Get the first row, first column
    except Exception as e:
        print(f"Error querying BigQuery for latest timestamp: {e}")
        latest_timestamp = None

    # Determine the start time for fetching data
    if latest_timestamp:
        start_time = latest_timestamp
    else:
        start_time = datetime(2024, 7, 19)  # Initial load from July 19th, 2024

    # Fetch and process sensor data
    sensor_data = fetch_sensor_data(access_token, start_time)
    if sensor_data:
        processed_data = process_sensor_data(sensor_data)
        if processed_data:
            insert_data_into_bigquery(processed_data)
            return "Data inserted successfully"
        else:
            return "No new data to insert"
    else:
        return "Failed to fetch sensor data"
