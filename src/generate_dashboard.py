from datetime import datetime
import jinja2
import os
import pandas as pd
import plotly.express as px
import sqlite3


DB_PATH = "stop_events.db"
DB_TABLE_NAME = "stop_events"
TEMPLATE_DIR = "../templates"
OUTPUT_DIR = "../output"
DASHBOARD_HTML_FILENAME = "dashboard.html"
INDEX_HTML_FILENAME = "index.html"

FREQUENT_HEADWAY_MINUTES = 15
MAX_HEADWAY_MINUTES = 120

STOP_ID = "2305"  # TODO make these dynamic (so a user can select which combo to view?)
ROUTE_ID = "77"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_data() -> pd.DataFrame:
    # Load data from SQLite into DataFrame
    conn = sqlite3.Connection(DB_PATH)
    df = pd.read_sql(f"""
        SELECT * FROM {DB_TABLE_NAME}
        ORDER BY stop_id, route_id, stop_timestamp
    """, conn)
    conn.close()

    # Calculate headways
    df["stop_timestamp"] = pd.to_datetime(df["stop_timestamp"])
    df["headway"] = df.groupby(["stop_id", "route_id"])["stop_timestamp"].diff()
    df["headway_minutes"] = df["headway"].dt.total_seconds() / 60

    # Filter to a specific stop and route, remove very long headways (likely service breaks)
    df_plot = df[
        (df["stop_id"] == STOP_ID) &
        (df["route_id"] == ROUTE_ID)
        ]
    df_plot.loc[df_plot["headway_minutes"] > MAX_HEADWAY_MINUTES, "headway_minutes"] = pd.NA

    return df_plot


def make_scatter_plot_headways_over_time(df: pd.DataFrame) -> str:
    fig = px.scatter(
        data_frame=df,
        x="stop_timestamp",
        y="headway_minutes",
        labels={"x": "Stop Timestamp", "y": "Headway (minutes)"},
        title=f"Headways of Route {ROUTE_ID} at Stop {STOP_ID}"
    )
    fig.add_hline(
        y=FREQUENT_HEADWAY_MINUTES,
        line_color="red"
    )

    return fig.to_html(full_html=False, include_plotlyjs="cdn")  # TODO check if cdn or False works


def make_histogram_headways_occurrences(df: pd.DataFrame) -> str:
    fig = px.histogram(
        data_frame=df,
        x="headway_minutes",
        nbins=60,
        labels={"x": "Headway (minutes)", "y": "Occurrences"},
        title=f"Headway Occurrences of Route {ROUTE_ID} at Stop {STOP_ID}"
    )
    fig.add_vline(
        x=FREQUENT_HEADWAY_MINUTES,
        line_color="red"
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)  # TODO check if cdn or False works


def render_dashboard(headways_scatter_plot_html: str, headways_histogram_html: str) -> None:
    env=jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_DIR))
    template = env.get_template(DASHBOARD_HTML_FILENAME)
    rendered = template.render(
        updated=datetime.now().strftime("%Y-%m-%d %H:%M"),
        headways_scatter_plot=headways_scatter_plot_html,
        headways_histogram=headways_histogram_html
    )
    with open(os.path.join(OUTPUT_DIR, INDEX_HTML_FILENAME), "w") as f:
        f.write(rendered)


def main() -> None:
    df = load_data()
    headways_scatter_plot_html = make_scatter_plot_headways_over_time(df)
    headways_histogram_html = make_histogram_headways_occurrences(df)
    render_dashboard(headways_scatter_plot_html, headways_histogram_html)


if __name__ == "__main__":
    main()
