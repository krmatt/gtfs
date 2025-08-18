import json
import pandas as pd
import requests


FILEPATH_STATIC_DATA = "../MBTA_GTFS_STATIC_DATA"
URL_SERVICE_ALERTS_REALTIME = "https://cdn.mbta.com/realtime/Alerts.json"
URL_TRIP_UPDATES_REALTIME = "https://cdn.mbta.com/realtime/TripUpdates.json"
URL_VEHICLE_POSITIONS_REALTIME = "https://cdn.mbta.com/realtime/VehiclePositions.json"
URL_SERVICE_ALERTS_REALTIME_ENHANCED = "https://cdn.mbta.com/realtime/Alerts_enhanced.json"
URL_TRIP_UPDATES_REALTIME_ENHANCED = "https://cdn.mbta.com/realtime/TripUpdates_enhanced.json"
URL_VEHICLE_POSITIONS_REALTIME_ENHANCED = "https://cdn.mbta.com/realtime/VehiclePositions_enhanced.json"

URL_MBTA_API_V3 = "https://api-v3.mbta.com"


def get_credentials(key_only: bool = False):
    with open("mbta_api_creds") as cred_file:
        for line in cred_file.readlines():
            if line.startswith("key"):
                key = line[4:]
            if line.startswith("user"):
                user = line[5:]

    if key_only:
        return key
    return key, user


def get_gtfs_realtime_data(url: str):
    response = requests.get(url)
    return json.loads(response.text)


def get_gtfs_static_data(data_type: str):
    return pd.read_csv(f"{FILEPATH_STATIC_DATA}/{data_type}.txt")


if __name__ == "__main__":
    pass
