import json
import sqlite3

import mbta_gtfs as gtfs

FILEPATH_STOP_ARRIVAL_EVENTS = "gtfs/mbta/data/stop_arrival_events.json"
FILEPATH_ERRORS = "gtfs/mbta/data/frequency_monitor_errors.txt"

FREQUENT_BUS_ROUTES = [
    "1",
    "15",
    "22",
    "23",
    "28",
    "32",
    "39",
    "57",
    "66",
    "71",
    "73",
    "77",
    "104",
    "109",
    "110",
    "111",
    "116",
]


# Using JSON TODO remove
def update_stop_arrival_events(tracked_route_ids: list[str]) -> None:
    vehicle_positions = gtfs.get_gtfs_realtime_data(gtfs.URL_VEHICLE_POSITIONS_REALTIME_ENHANCED)["entity"]
    try:
        with open(FILEPATH_STOP_ARRIVAL_EVENTS, "r") as f:
            stop_arrival_events = json.load(f)
    except FileNotFoundError:
        # If the arrival events file does not exist, start from scratch
        stop_arrival_events = {}

    for vehicle in vehicle_positions:
        try:
            route_id = vehicle["vehicle"]["trip"]["route_id"]
            trip_id = vehicle["vehicle"]["trip"]["trip_id"]
            stop_id = vehicle["vehicle"]["stop_id"]
            date_code = vehicle["vehicle"]["trip"]["start_date"]
            arrival_timestamp = vehicle["vehicle"]["timestamp"]
        except KeyError as e:
            with open(FILEPATH_ERRORS, "w") as f:  # Overwrite because this file could get very big
                f.write(f"Vehicle missing attribute.\n\t{vehicle}\n\t{e}\n")
            continue

        if (
                vehicle["vehicle"]["trip"]["route_id"] in tracked_route_ids and
                vehicle["vehicle"]["current_status"] == "STOPPED_AT"
        ):
            if stop_id not in stop_arrival_events.keys():
                stop_arrival_events[stop_id] = {}

            if route_id not in stop_arrival_events[stop_id].keys():
                stop_arrival_events[stop_id][route_id] = {}

            if date_code not in stop_arrival_events[stop_id][route_id].keys():
                stop_arrival_events[stop_id][route_id][date_code] = {}

            if trip_id in stop_arrival_events[stop_id][route_id][date_code].keys():
                continue
            else:
                stop_arrival_events[stop_id][route_id][date_code][trip_id] = arrival_timestamp

    with open(FILEPATH_STOP_ARRIVAL_EVENTS, "w") as f:
        json.dump(stop_arrival_events, f)


# Using SQLite
def update_stop_events(tracked_route_ids: list[str]) -> None:
    vehicle_positions = gtfs.get_gtfs_realtime_data(gtfs.URL_VEHICLE_POSITIONS_REALTIME_ENHANCED)["entity"]
    stop_events = []

    for vehicle in vehicle_positions:
        try:
            route_id = vehicle["vehicle"]["trip"]["route_id"]
            trip_id = vehicle["vehicle"]["trip"]["trip_id"]
            stop_id = vehicle["vehicle"]["stop_id"]
            stop_timestamp = vehicle["vehicle"]["timestamp"]
        except KeyError as e:
            with open(FILEPATH_ERRORS, "w") as f:  # Overwrite because this file could get very big
                f.write(f"Vehicle missing attribute.\n\t{vehicle}\n\t{e}\n")
            continue

        if (
                vehicle["vehicle"]["trip"]["route_id"] in tracked_route_ids and
                vehicle["vehicle"]["current_status"] == "STOPPED_AT"
        ):
            stop_events.append((stop_id, route_id, trip_id, stop_timestamp))

    conn = sqlite3.connect("stop_events.db")
    cursor = conn.cursor()

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS stop_events (
                stop_id TEXT,
                route_id TEXT,
                trip_id TEXT,
                stop_timestamp INTEGER,
                unique (stop_id, route_id, trip_id)
            );
        """)

    cursor.executemany("""
        INSERT OR IGNORE INTO stop_events
        VALUES (?, ?, ?, ?)
    """, stop_events)

    conn.commit()
    cursor.close()
    conn.close()


def read_db(route_id: str) -> None:
    conn = sqlite3.connect("stop_events.db")
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT * FROM stop_events
        WHERE route_id = '{route_id}'
        ORDER BY stop_timestamp DESC
        LIMIT 100
    """)

    rows = cursor.fetchall()
    for row in rows:
        print(row)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    # update_stop_events(FREQUENT_BUS_ROUTES)
    read_db("77")
