import json
import pandas as pd
import requests


FILEPATH_STATIC_DATA = "../MBTA_GTFS_STATIC_DATA"
URL_STATIC_DATA = "https://cdn.mbta.com/MBTA_GTFS.zip"
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


def get_first_and_last_stop_ids(route_ids: list[str]) -> dict:
    # NOTE: We actually want to get the first and second-to-last stop_id.
    # I'm calculating headways by measuring the times between departures at a stop,
    # and departures are indicated by a vehicle's current stop_id changing.
    # The vehicle's stop_id won't change for a long time after departing the last stop,
    # so the second-to-last stop_id is the next best thing.
    headers = {
        "accept": "application/vnd.api+json",
        "x-api-key": get_credentials(key_only=True)
    }

    route_first_last_stop_ids = {}

    for route_id in route_ids:
        direction_ids = [0, 1]  # TODO get all possible directions from route patterns
        route_first_last_stop_ids[route_id] = {direction_id: {} for direction_id in direction_ids}

        for direction_id in direction_ids:
            response_schedules = requests.get(
                url=f"{URL_MBTA_API_V3}/schedules",
                headers=headers,
                params={
                    "filter[route]": route_id,
                    "filter[stop_sequence]": "first,last",
                    "filter[direction_id]": direction_id,
                    "page[limit]": 2
                }
            )
            response_schedules.raise_for_status()
            schedules = response_schedules.json()["data"]

            for stop in schedules:
                stop_id = stop["relationships"]["stop"]["data"]["id"]
                stop_sequence = stop["attributes"]["stop_sequence"]
                if stop_sequence == 1:
                    route_first_last_stop_ids[route_id][direction_id]["first"] = stop_id
                elif stop_sequence > 1:
                    # Get the stop_id for the second-to-last stop instead
                    response_second_to_last_stop = requests.get(
                        url=f"{URL_MBTA_API_V3}/schedules",
                        headers=headers,
                        params={
                            "filter[route]": route_id,
                            "filter[stop_sequence]": stop_sequence - 1,
                            "filter[direction_id]": direction_id,
                            "page[limit]": 1
                        }
                    )
                    response_second_to_last_stop.raise_for_status()
                    route_first_last_stop_ids[route_id][direction_id]["last"] = response_second_to_last_stop.json()["data"][0]["relationships"]["stop"]["data"]["id"]
                else:
                    raise ValueError(f"Stop sequence out of expected range:\nstop_sequence: {stop_sequence}\nstop_id: {stop_id}\nroute_id: {route_id}")

    return route_first_last_stop_ids


if __name__ == "__main__":
    FREQUENT_BUS_ROUTES = ["1", "15", "22", "23", "28", "32", "39", "57", "66", "71", "73", "77", "104", "109", "110", "111", "116"]
    first_and_last_stops = get_first_and_last_stop_ids(FREQUENT_BUS_ROUTES)
    for route_id in FREQUENT_BUS_ROUTES:
        print(f"{route_id}: {first_and_last_stops[route_id]}")
