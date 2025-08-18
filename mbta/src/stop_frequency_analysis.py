import sqlite3

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

matplotlib.use("Agg")

UTC_OFFSET_EDT = "04:00:00"
MAX_HEADWAY_MINUTES = 120


def load_stop_data_from_sqlite(db: str, table: str) -> pd.DataFrame:
    query = f"""
        SELECT * FROM {table}
        ORDER BY stop_id, route_id, stop_timestamp
    """
    df = pd.read_sql(query, sqlite3.connect(db))
    # df["stop_timestamp"] = pd.to_datetime(df["stop_timestamp"], unit="s") - pd.Timedelta(UTC_OFFSET_EDT)
    df["stop_timestamp"] = pd.to_datetime(df["stop_timestamp"])
    df["headway"] = df.groupby(["stop_id", "route_id"])["stop_timestamp"].diff()
    df["headway_minutes"] = df["headway"].dt.total_seconds() / 60
    return df


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

    # df_cleaned = df.groupby(["stop_id", "route_id"], group_keys=False).apply(remove_outliers, include_groups=False)
    # df_cleaned["headway_minutes"] = df_cleaned["headway"].dt.total_seconds() / 60
    # df_cleaned["headway_minutes"].hist(bins=100)

    df["headway_minutes"] = df["headway"].dt.total_seconds() / 60
    df_cleaned = df.groupby(["stop_id", "route_id"], group_keys=False).apply(remove_outliers, include_groups=False)
    df_cleaned["headway_minutes"].hist(bins=100)

    plt.axvline(x=15, color="r", lw=1)
    plt.xlabel("Headway (minutes)")
    plt.ylabel("Occurrences")
    plt.title("Headways of Frequent Bus Routes")
    plt.savefig("stop_hist.pdf")


def remove_outliers(group):
    # return group[group["headway"] <= group["headway"].quantile(0.95)]
    return group[group["headway_minutes"] <= MAX_HEADWAY_MINUTES]


def plot_headway_over_time(stop_id: str, route_id: str) -> None:
    df = load_stop_data_from_sqlite("stop_events.db", "stop_events")
    df_plot = df[
        (df["stop_id"] == stop_id) &
        (df["route_id"] == route_id)
    ]
    df_plot.loc[df_plot["headway_minutes"] > MAX_HEADWAY_MINUTES, "headway_minutes"] = pd.NA

    sns.lineplot(data=df_plot, x="stop_timestamp", y="headway_minutes", marker="o")

    plt.axhline(y=15, color="r", lw=1)
    plt.xticks(rotation=45)
    plt.tight_layout(rect=(0, 0, 1, 0.95))
    plt.xlabel("Stop Timestamp")
    plt.ylabel("Headway (minutes)")
    plt.title(f"Headways of Route {route_id} at Stop {stop_id}")
    plt.savefig(f"../data/headways_{stop_id}_{route_id}.pdf")


if __name__ == "__main__":
    plot_headway_over_time("2305", "77")
    # plot_headway_over_time("2567", "109")
