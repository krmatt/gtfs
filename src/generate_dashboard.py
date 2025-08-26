from datetime import datetime, time, timedelta
import jinja2
import os
import pandas as pd
import plotly.express as px
import re
import sqlite3

import mbta_gtfs


DB_PATH = "stop_events.db"
DB_TABLE_NAME = "stop_events"
TEMPLATE_DIR = "../templates"
OUTPUT_DIR = ".."
DASHBOARD_HTML_FILENAME = "dashboard.html"
INDEX_HTML_FILENAME = "index.html"
STOPS_FILENAME = "../MBTA_GTFS_STATIC_DATA/stops.txt"

FREQUENT_HEADWAY_MINUTES = 15
MAX_HEADWAY_MINUTES = 120
LOOKBACK_DAYS = 7
TIME_FREQUENT_SERVICE_START = time(5, 0)  # 5:00am ET
TIME_FREQUENT_SERVICE_STOP = time(1, 0)  # 1:00am ET (next day)

FREQUENT_BUS_ROUTES = [
    "1",
    "15",
    "22",
    "23",
    "28",
    "31",
    # "32",
    # "39",
    # "57",
    # "66",
    # "71",
    # "73",
    # "77",
    "104",
    "109",
    "110",
    # "111",
    "116",
]  # TODO include routes as they become frequent bus routes


os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_data(lookback_days: int) -> pd.DataFrame:
    # Load data from SQLite into DataFrame
    conn = sqlite3.Connection(DB_PATH)
    df = pd.read_sql(f"""
        SELECT * FROM {DB_TABLE_NAME}
        ORDER BY stop_id, route_id, stop_timestamp
    """, conn)
    conn.close()

    # Calculate headways
    df["stop_timestamp"] = pd.to_datetime(df["stop_timestamp"])
    df_lookback = df[
        df["stop_timestamp"] >= pd.Timestamp.now(tz=df["stop_timestamp"].dt.tz) - timedelta(days=lookback_days)
    ]
    df_service_hours = df_lookback[
        (df_lookback["stop_timestamp"].dt.time >= TIME_FREQUENT_SERVICE_START) |
        (df_lookback["stop_timestamp"].dt.time < TIME_FREQUENT_SERVICE_STOP)
    ]

    df_service_hours["headway"] = df_service_hours.groupby(["stop_id", "route_id"])["stop_timestamp"].diff()
    df_service_hours["headway_minutes"] = df_service_hours["headway"].dt.total_seconds() / 60
    df_service_hours.loc[df_service_hours["headway_minutes"] > MAX_HEADWAY_MINUTES, "headway_minutes"] = pd.NA

    return df_service_hours


def sort_key(s) -> tuple:
    # Sort series in the scatter plot by route, inbound/outbound, and stop number
    num_match = re.match(r"(\d+)", s)
    num = int(num_match.group(1)) if num_match else float("inf")

    return num, s


def make_scatter_plot_headways_at_first_and_last_stops(df: pd.DataFrame) -> str:
    first_last_stop_ids = mbta_gtfs.get_first_and_last_stop_ids(FREQUENT_BUS_ROUTES)
    df_stops_info = pd.read_csv(STOPS_FILENAME)

    # Prepare data
    stop_ids = []
    for route_id in FREQUENT_BUS_ROUTES:
        stop_ids.extend([
            first_last_stop_ids[route_id][0]["first"],
            first_last_stop_ids[route_id][0]["last"],
            first_last_stop_ids[route_id][1]["first"],
            first_last_stop_ids[route_id][1]["last"]
        ])

    df = df.merge(df_stops_info[["stop_id", "stop_name"]], on="stop_id", how="left")

    df["series"] = (df["route_id"] + " " +
                    df["direction_id"].map({0: "Outbound", 1: "Inbound"}) + ": " +
                    df["stop_name"])

    df_plot = df[
        (df["route_id"].isin(FREQUENT_BUS_ROUTES)) &
        (df["stop_id"].isin(stop_ids))
    ]

    # Create plot
    fig = px.scatter(
        data_frame=df_plot,
        x="stop_timestamp",
        y="headway_minutes",
        color="series",
        category_orders={"series": sorted(df_plot["series"].unique(), key=sort_key)},
        labels={
            "stop_timestamp": "Stop Timestamp",
            "headway_minutes": "Headway (minutes)",
            "series": "Route, Direction, and Stop"
        },
        title=f"Headways at First and Last Stops"
    )
    fig.add_hline(
        y=FREQUENT_HEADWAY_MINUTES,
        line_color="red"
    )
    fig.update_traces(visible="legendonly")
    fig.for_each_trace(lambda t: t.update(visible=True) if re.match(r"^1 ", t.name) else None)

    return fig.to_html(full_html=False, include_plotlyjs="cdn", config={"responsive": True})


def make_bar_chart_headways_frequency_threshold(df: pd.DataFrame) -> str:
    # Prepare data
    df_freq = df[df["route_id"].isin(FREQUENT_BUS_ROUTES)].dropna(subset="headway_minutes")
    df_freq["meets_headway_target"] = df_freq["headway_minutes"] <= FREQUENT_HEADWAY_MINUTES

    headways_meeting_target_by_route = {}
    for route_id in FREQUENT_BUS_ROUTES:
        df_route = df_freq[df_freq["route_id"] == route_id]
        headways_meeting_target_by_route[route_id] = df_route["meets_headway_target"].values.sum() / len(df_route["headway_minutes"].values)

    df_plot = pd.DataFrame({
        "Route": FREQUENT_BUS_ROUTES,
        "Proportion of Headways Meeting Target": [headways_meeting_target_by_route[route_id] for route_id in FREQUENT_BUS_ROUTES]
    })
    df_plot["label"] = df_plot["Proportion of Headways Meeting Target"].apply(lambda x: f"{x * 100:.1f}%")

    # Create plot
    fig = px.bar(
        data_frame=df_plot,
        x="Proportion of Headways Meeting Target",
        y="Route",
        orientation="h",
        title=f"How Often do Frequent Buses<br>Meet the 15 Minute Headway Target?",
        color="Route",
        range_x=[0,1],
        text="label"
    )
    fig.update_traces(
        textposition='outside',
        textfont_size=14,
        cliponaxis=False
    )
    fig.update_layout(
        autosize=True,
        margin=dict(l=50, r=50, t=50, b=50),
        showlegend=False
    )

    return fig.to_html(full_html=False, include_plotlyjs=False, config={"responsive": True})


def make_histogram_headways_distribution(df: pd.DataFrame) -> str:
    # Prepare data
    df_plot = df[df["route_id"].isin(FREQUENT_BUS_ROUTES)].dropna(subset="headway_minutes")

    # Create Plot
    fig = px.histogram(
        data_frame=df_plot,
        x="headway_minutes",
        color="route_id",
        category_orders={"route_id": sorted(df_plot["route_id"].unique(), key=sort_key)},
        opacity=0.7,
        labels={"headway_minutes": "Headway (minutes)", "route_id": "Route"},
        title=f"Distribution of Headways"
    )
    fig.add_vline(
        x=FREQUENT_HEADWAY_MINUTES,
        line_color="red"
    )
    fig.update_layout(
        barmode="overlay",
        yaxis_title="Count"
    )
    fig.update_traces(visible="legendonly")
    fig.for_each_trace(lambda t: t.update(visible=True) if t.name == "1" else None)

    return fig.to_html(full_html=False, include_plotlyjs=False, config={"responsive": True})


def render_dashboard(scatter_plot_headways_first_last: str,
                     bar_chart_headways_frequency_threshold: str,
                     histogram_headways_distribution: str) -> None:
    template=jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_DIR)).get_template(DASHBOARD_HTML_FILENAME)
    rendered = template.render(
        updated=datetime.now().strftime("%Y-%m-%d %H:%M"),
        routes=FREQUENT_BUS_ROUTES,
        scatter_plot_headways_first_last=scatter_plot_headways_first_last,
        bar_chart_headways_frequency_threshold=bar_chart_headways_frequency_threshold,
        histogram_headways_distribution=histogram_headways_distribution
    )
    with open(os.path.join(OUTPUT_DIR, INDEX_HTML_FILENAME), "w") as f:
        f.write(rendered)


def main() -> None:
    df = load_data(LOOKBACK_DAYS)
    scatter_plot_headways_first_last = make_scatter_plot_headways_at_first_and_last_stops(df)
    bar_chart_headways_frequency_threshold = make_bar_chart_headways_frequency_threshold(df)
    histogram_headways_distribution = make_histogram_headways_distribution(df)
    render_dashboard(scatter_plot_headways_first_last, bar_chart_headways_frequency_threshold, histogram_headways_distribution)


if __name__ == "__main__":
    main()