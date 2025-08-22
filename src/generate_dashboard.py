from datetime import datetime
import jinja2
import os
import pandas as pd
import plotly.express as px
import sqlite3

import mbta_gtfs


DB_PATH = "stop_events.db"
DB_TABLE_NAME = "stop_events"
TEMPLATE_DIR = "../templates"
OUTPUT_DIR = ".."
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
    df.loc[df["headway_minutes"] > MAX_HEADWAY_MINUTES, "headway_minutes"] = pd.NA

    return df

    # Filter to a specific stop and route, remove very long headways (likely service breaks)
    # TODO do filtering in specific plot functions?
    # df_plot = df[
    #     (df["stop_id"] == STOP_ID) &
    #     (df["route_id"] == ROUTE_ID) # TODO use direction to disambiguate shared stops between inbound/outbound
    #     ]
    # df_plot.loc[df_plot["headway_minutes"] > MAX_HEADWAY_MINUTES, "headway_minutes"] = pd.NA
    #
    # return df_plot


def make_scatter_plot_headways_at_first_and_last_stops(df: pd.DataFrame, route_id: str, first_last_stop_ids: dict) -> str:
    # Prepare data
    stop_ids = [
        first_last_stop_ids[route_id][0]["first"],
        first_last_stop_ids[route_id][0]["last"],
        first_last_stop_ids[route_id][1]["first"],
        first_last_stop_ids[route_id][1]["last"]
    ]

    df_plot = df[
        (df["route_id"] == route_id) &
        (df["stop_id"].isin(stop_ids))
    ]

    df_plot["series"] = df['direction_id'].astype(str) + '_' + df['stop_id'].astype(str)

    # Create plot
    fig = px.scatter(
        data_frame=df_plot,
        x="stop_timestamp",
        y="headway_minutes",
        color="series",
        labels={"x": "Stop Timestamp", "y": "Headway (minutes)"},
        title=f"Headways at the First and Last Stops of Route {ROUTE_ID}"
    )
    fig.add_hline(
        y=FREQUENT_HEADWAY_MINUTES,
        line_color="red"
    )

    return fig.to_html(full_html=False, include_plotlyjs="cdn")


def make_bar_chart_headways_frequency_threshold(df: pd.DataFrame, route_id: str) -> str:
    # Prepare data
    df_route = df[df["route_id"] == route_id].dropna(subset="headway_minutes")
    total_values = len(df_route["headway_minutes"].values)

    df_plot = pd.DataFrame({
        "Headway": ["Over 15 min", "At or Under 15 min"],
        "Proportion of Headways": [
            (df_route["headway_minutes"].values > FREQUENT_HEADWAY_MINUTES).sum() / total_values,
            (df_route["headway_minutes"].values <= FREQUENT_HEADWAY_MINUTES).sum() / total_values
        ]
    })
    df_plot["label"] = df_plot["Proportion of Headways"].apply(lambda x: f"{x * 100:.1f}%")

    # Create plot
    fig = px.bar(
        data_frame=df_plot,
        x="Proportion of Headways",
        y="Headway",
        orientation="h",
        title=f"How Often do {route_id} Buses Meet the 15 Minute Target?",
        color="Headway",
        color_discrete_map={
            "Over 15 min": "orange",
            "At or Under 15 min": "blue"
        },
        range_x=[0,1],
        text="label"
    )
    fig.update_traces(
        textposition='outside',
        textfont_size=24,
        cliponaxis=False
    )
    fig.update_layout(
        showlegend=False
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def make_histogram_headways_distribution(df: pd.DataFrame, route_id: str) -> str:
    # Prepare data
    df_plot = df[df["route_id"] == route_id].dropna(subset="headway_minutes")

    # Create Plot
    fig = px.histogram(
        data_frame=df_plot,
        x="headway_minutes",
        labels={"x": "Headway (minutes)", "y": "Occurrences"},
        title=f"Distribution of Headways on Route {route_id}"
    )
    fig.add_vline(
        x=FREQUENT_HEADWAY_MINUTES,
        line_color="red"
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def render_dashboard(scatter_plot_headways_first_last_html: str,
                     bar_chart_headways_frequency_threshold_html: str,
                     histogram_headways_distribution_html: str) -> None:
    env=jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_DIR))
    template = env.get_template(DASHBOARD_HTML_FILENAME)
    rendered = template.render(
        updated=datetime.now().strftime("%Y-%m-%d %H:%M"),
        scatter_plot_headways_first_last=scatter_plot_headways_first_last_html,
        bar_chart_headways_frequency_threshold=bar_chart_headways_frequency_threshold_html,
        histogram_headways_distribution=histogram_headways_distribution_html
    )
    with open(os.path.join(OUTPUT_DIR, INDEX_HTML_FILENAME), "w") as f:
        f.write(rendered)


def main() -> None:
    route_first_last_stop_ids = mbta_gtfs.get_first_and_last_stop_ids([ROUTE_ID])
    df = load_data()
    scatter_plot_headways_first_last_html = make_scatter_plot_headways_at_first_and_last_stops(df, ROUTE_ID, route_first_last_stop_ids)
    bar_chart_headways_frequency_threshold_html = make_bar_chart_headways_frequency_threshold(df, ROUTE_ID)
    histogram_headways_distribution_html = make_histogram_headways_distribution(df, ROUTE_ID)
    render_dashboard(scatter_plot_headways_first_last_html, bar_chart_headways_frequency_threshold_html, histogram_headways_distribution_html)


if __name__ == "__main__":
    main()
