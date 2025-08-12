import sqlite3

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")


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

    df_cleaned = df.groupby(["stop_id", "route_id"], group_keys=False).apply(remove_outliers, include_groups=False)
    df_cleaned["headway_minutes"] = df_cleaned["headway"].dt.total_seconds() / 60
    df_cleaned["headway_minutes"].hist(bins=80)
    plt.axvline(x=15, color="r", lw=1)
    plt.xlabel("Headway (minutes)")
    plt.ylabel("Occurrences")
    plt.title("Headways of Frequent Bus Routes")
    plt.savefig("stop_hist.pdf")


def remove_outliers(group):
    return group[group["headway"] <= group["headway"].quantile(0.95)]


if __name__ == "__main__":
    calculate_headways("stop_events.db", "stop_events")
