import aiosqlite
import asyncio
from datetime import datetime
import httpx
import httpx_sse
import json
import logging
import random
# import sqlite3  # TODO remove

import mbta_gtfs as gtfs

FILEPATH_STOP_ARRIVAL_EVENTS = "gtfs/mbta/data/stop_arrival_events.json"
FILEPATH_ERRORS = "gtfs/mbta/data/frequency_monitor_errors.txt"
DATABASE = "stop_events.db"

RETRY_BASE_SECONDS = 5
RETRY_MAX_SECONDS = 60

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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("stop_frequency_monitor.log"),
        logging.StreamHandler()
    ]
)

previous_vehicle_stops = {}  # vehicle_id: stop_id


### Polling for Data ###
# def update_stop_events(tracked_route_ids: list[str]) -> None:
#     vehicle_positions = gtfs.get_gtfs_realtime_data(gtfs.URL_VEHICLE_POSITIONS_REALTIME_ENHANCED)["entity"]
#     stop_events = []
#
#     for vehicle in vehicle_positions:
#         try:
#             route_id = vehicle["vehicle"]["trip"]["route_id"]
#             trip_id = vehicle["vehicle"]["trip"]["trip_id"]
#             stop_id = vehicle["vehicle"]["stop_id"]
#             stop_timestamp = vehicle["vehicle"]["timestamp"]
#         except KeyError as e:
#             with open(FILEPATH_ERRORS, "w") as f:  # Overwrite because this file could get very big
#                 f.write(f"Vehicle missing attribute.\n\t{vehicle}\n\t{e}\n")
#             continue
#
#         if (
#                 vehicle["vehicle"]["trip"]["route_id"] in tracked_route_ids and
#                 vehicle["vehicle"]["current_status"] == "STOPPED_AT"
#         ):
#             stop_events.append((stop_id, route_id, trip_id, stop_timestamp))
#
#     conn = sqlite3.connect("stop_events.db")
#     cursor = conn.cursor()
#
#     cursor.execute("""
#             CREATE TABLE IF NOT EXISTS stop_events (
#                 stop_id TEXT,
#                 route_id TEXT,
#                 trip_id TEXT,
#                 stop_timestamp INTEGER,
#                 unique (stop_id, route_id, trip_id)
#             );
#         """)
#
#     cursor.executemany("""
#         INSERT OR IGNORE INTO stop_events
#         VALUES (?, ?, ?, ?)
#     """, stop_events)
#
#     conn.commit()
#     cursor.close()
#     conn.close()


# def read_db(route_id: str) -> None:
#     conn = sqlite3.connect("stop_events.db")
#     cursor = conn.cursor()
#
#     cursor.execute(f"""
#         SELECT * FROM stop_events
#         WHERE route_id = '{route_id}'
#         ORDER BY stop_timestamp DESC
#         LIMIT 100
#     """)
#
#     rows = cursor.fetchall()
#     for row in rows:
#         print(row)
#
#     cursor.close()
#     conn.close()


### Streaming Data ###
async def setup_db():
    async with aiosqlite.connect(DATABASE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stop_events (
                stop_id TEXT,
                route_id TEXT,
                trip_id TEXT,
                direction_id INTEGER,
                stop_timestamp INTEGER,
                unique (stop_id, route_id, trip_id)
            );
        """)
        await db.commit()


async def log_stop_event(db: aiosqlite.Connection, stop_id: str, route_id: str, trip_id: str, direction_id: int, stop_timestamp: int):
    await db.execute("""
        INSERT OR IGNORE INTO stop_events (stop_id, route_id, trip_id, direction_id, stop_timestamp)
        VALUES (?, ?, ?, ?, ?)
    """, (stop_id, route_id, trip_id, direction_id, stop_timestamp))
    await db.commit()


async def stream_vehicle_data():
    await setup_db()

    url = f"{gtfs.URL_MBTA_API_V3}/vehicles?filter[route]=77&filter[revenue]=REVENUE"  # TODO make filter a variable
    headers = {
        "Accept": "text/event-stream",
        "x-api-key": gtfs.get_credentials(True)
    }

    attempt = 0
    while True:
        try:
            logging.info(f"Connecting to MBTA SSE stream (attempt {attempt + 1})...")
            async with httpx.AsyncClient(timeout=None) as client:
                async with httpx_sse.aconnect_sse(client=client, method="GET", url=url, headers=headers) as sse:
                    async with aiosqlite.connect(DATABASE) as db:
                        async for event in sse.aiter_sse():
                            if event.event == "update":  # TODO handle other event types (reset, add, remove)
                                try:
                                    data = json.loads(event.data)
                                    await handle_vehicle_data(data, db)
                                except Exception as e:
                                    logging.error(f"Error parsing stream data: {e}")
            raise RuntimeError("SSE stream ended unexpectedly")

        except (httpx.HTTPError, RuntimeError, ConnectionError, asyncio.CancelledError) as e:
            wait_time = min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS * (2 ** attempt)) * random.uniform(0.5, 1.5)
            attempt += 1
            logging.warning(f"Disconnected or failed to connect: {e}")
            logging.info(f"Retrying in {wait_time:.1f} seconds...")
            await asyncio.sleep(wait_time)


async def handle_vehicle_data(data: dict, db: aiosqlite.Connection):
    vehicle_id = data["id"]
    stop_timestamp = data["attributes"]["updated_at"]
    direction_id = data["attributes"]["direction_id"]
    stop_id = data["relationships"]["stop"]["data"]["id"]
    route_id = data["relationships"]["route"]["data"]["id"]
    trip_id = data["relationships"]["trip"]["data"]["id"] + (datetime.fromisoformat(stop_timestamp).strftime("%Y%m%d"))  # Trip IDs are only unique within a service day, so we append the date to guarantee universal uniqueness within the db

    previous_stop_id = previous_vehicle_stops.get(vehicle_id)

    if (
        stop_id != previous_stop_id and  # stop_id changes when a bus departs from a station
        route_id in FREQUENT_BUS_ROUTES and
        data["attributes"]["current_status"] == "STOPPED_AT"
    ):
        await log_stop_event(db, stop_id, route_id, trip_id, direction_id, stop_timestamp)
        previous_vehicle_stops[vehicle_id] = stop_id


if __name__ == "__main__":
    asyncio.run(stream_vehicle_data())
