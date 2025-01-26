import json
import pandas as pd
import requests


FILEPATH_STATIC_DATA = "api/MBTA_GTFS"
URL_SERVICE_ALERTS_REALTIME = "https://cdn.mbta.com/realtime/Alerts.json"
URL_TRIP_UPDATES_REALTIME = "https://cdn.mbta.com/realtime/TripUpdates.json"
URL_VEHICLE_POSITIONS_REALTIME = "https://cdn.mbta.com/realtime/VehiclePositions.json"
URL_SERVICE_ALERTS_REALTIME_ENHANCED = "https://cdn.mbta.com/realtime/Alerts_enhanced.json"
URL_TRIP_UPDATES_REALTIME_ENHANCED = "https://cdn.mbta.com/realtime/TripUpdates_enhanced.json"
URL_VEHICLE_POSITIONS_REALTIME_ENHANCED = "https://cdn.mbta.com/realtime/VehiclePositions_enhanced.json"

URL_MBTA_API_V3 = "https://api-v3.mbta.com"


def get_credentials():
    with open("api/mbta_api_creds") as cred_file:
        for line in cred_file.readlines():
            if line.startswith("key"):
                key = line[4:]
            if line.startswith("user"):
                user = line[5:]

    return key, user


# def get_gtfs_api_data(data_type: str, fields: list = None):


def get_gtfs_realtime_data(url: str):
    response = requests.get(url)
    return json.loads(response.text)


def get_gtfs_static_data(data_type: str):
    return pd.read_csv(f"{FILEPATH_STATIC_DATA}/{data_type}.txt")


def main():
    trip_updates = get_gtfs_realtime_data(URL_TRIP_UPDATES_REALTIME)
    trip_update = trip_updates["entity"][0]
    print(trip_update)
    df_stops = get_gtfs_static_data("stops")
    print(trip_update["trip_update"]["trip"])
    for stop_time_update in trip_update["trip_update"]["stop_time_update"]:
        stop_name = df_stops.loc[df_stops['stop_id'] == stop_time_update['stop_id'], 'stop_name'].iloc[0]
        print(f"{stop_time_update}\t{stop_name}")


if __name__ == "__main__":
    main()