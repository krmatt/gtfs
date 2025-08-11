import sqlite3

import pandas as pd


def calculate_headways(db: str, table: str) -> None:
    query = f"""
        SELECT * FROM {table}
        ORDER BY stop_id, route_id, stop_timestamp
    """
    df = pd.read_sql(query, sqlite3.connect(db))
    df["stop_timestamp"] = pd.to_datetime(df["stop_timestamp"], unit="s") - pd.Timedelta("04:00:00")

    df["headway"] = df.groupby(["stop_id", "route_id"])["stop_timestamp"].diff()
    print(df.head(), end="\n\n")

    long_headways = df[df["headway"] > pd.Timedelta(minutes=15)]
    print(long_headways.head())

    avg_headways = df.groupby(["stop_id", "route_id"])["headway"].mean().reset_index()
    print(avg_headways)

if __name__ == "__main__":
    calculate_headways("stop_events.db", "stop_events")
