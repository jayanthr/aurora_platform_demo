"""
Weather Dashboard Application
----------------------------
This Dash app visualizes weather station data using Plotly and Dash components.

Key Data Flow:
1. The app always fetches the latest weather data from the 'weather' Kafka topic using the Kafka REST Proxy (see get_kafka_data).
2. For historical temperature trends (last 30 minutes), the app fetches from the 'weather_history' Kafka topic (see get_kafka_history).
3. The dashboard updates every 10 seconds, showing:
    - Current temperature gauges for each station (from Kafka).
    - Temperature trends, humidity, wind speed, and precipitation (using both Kafka topics).

If the Kafka REST Proxy is unavailable or returns no data, the dashboard will display empty plots.
"""
def get_kafka_history():
    import uuid
    try:
        group_name = "weather-history-group"
        consumer_name = "weather-history-consumer"
        consumer_response = requests.post(
            f"http://kafka-rest-proxy:8082/consumers/{group_name}",
            headers={"Content-Type": "application/vnd.kafka.json.v2+json",
                     "Accept": "application/vnd.kafka.json.v2+json"},
            json={
                "name": consumer_name,
                "format": "json",
                "auto.offset.reset": "earliest"
            },
            timeout=5
        )
        if consumer_response.status_code != 200:
            return None
        consumer_data = consumer_response.json()
        base_uri = consumer_data["base_uri"]
        subscribe_response = requests.post(
            f"{base_uri}/subscription",
            headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
            json={"topics": ["weather_history"]},
            timeout=5
        )
        if subscribe_response.status_code != 204:
            requests.delete(base_uri)
            return None
        import time
        messages = []
        for _ in range(5):
            records_response = requests.get(
                f"{base_uri}/records",
                headers={"Accept": "application/vnd.kafka.json.v2+json"},
                timeout=5
            )
            if records_response.status_code == 200:
                batch = records_response.json()
                if batch:
                    messages.extend(batch)
                    break
            time.sleep(0.5)
        requests.delete(base_uri)
        print("Received historical weather data from Kafka:", messages)
        if messages:
            data = []
            for msg in messages:
                value = msg['value']
                data.append({
                    'city_id': value['city_id'],
                    'city_name': value['city_name'],
                    'end_time': value['timestamp'],
                    'avg_temperature': value['temperature_celsius']
                })
            df = pd.DataFrame(data)
            print("Parsed DataFrame (history):\n", df)
            # Convert end_time to datetime for sorting/plotting
            df['end_time'] = pd.to_datetime(df['end_time'])
            # Only keep last 30 minutes of data
            cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=30)
            df = df[df['end_time'] >= cutoff]
            df = df.sort_values(['city_id', 'end_time'])
            return df
    except Exception as e:
        print(f"Kafka history consumption failed: {e}")
    return None
"""
Weather Dashboard Application
----------------------------
This Dash app visualizes weather station data using Plotly and Dash components.

Key Data Flow:
1. The app always fetches the latest weather data from the 'weather' Kafka topic using the Kafka REST Proxy. This is done via the get_kafka_data() function, which:
    - Creates a consumer instance via the REST Proxy.
    - Subscribes to the 'weather' topic.
    - Fetches the most recent records (weather metrics for each city/station).
    - Cleans up the consumer instance after use.
    - Returns a pandas DataFrame with the latest weather data for all stations.
2. For historical temperature trends (e.g., last 30 minutes), the app still queries the PostgreSQL database. This is because Kafka only provides recent data, while the database stores historical records.
3. The dashboard updates every 10 seconds, showing:
    - Current temperature gauges for each station (from Kafka).
    - Temperature trends, humidity, wind speed, and precipitation (using both Kafka and DB data).

If the Kafka REST Proxy is unavailable or returns no data, the dashboard will display empty plots.

This approach ensures the dashboard is always up-to-date with the latest weather data from Kafka, while still leveraging the database for historical analysis.
"""
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
import sqlalchemy
from dash_bootstrap_templates import load_figure_template
import requests
import json  

# Load the dark theme for the entire app
load_figure_template("bootstrap")

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div(
    [
        dbc.NavbarSimple(
            brand="Weather Station Analysis",
            brand_href="#",
            color="white",
            dark=False,
        ),
        dbc.Container(
            [
                dbc.Row(
                    [
                        dbc.Col(dcc.Graph(id="station-1-gauge"), width=4),
                        dbc.Col(dcc.Graph(id="station-2-gauge"), width=4),
                        dbc.Col(dcc.Graph(id="station-3-gauge"), width=4),
                    ],
                    align="center",
                ),
                dbc.Row(
                    [
                        dbc.Col(dcc.Graph(id="station-4-gauge"), width=4),
                        dbc.Col(dcc.Graph(id="station-5-gauge"), width=4),
                    ],
                    align="center",
                ),
                dbc.Row([dbc.Col(dcc.Graph(id="temperature-trends"), width=12)]),
                dbc.Row(
                    [
                        dbc.Col(dcc.Graph(id="humidity-wind"), width=6),
                        dbc.Col(dcc.Graph(id="precipitation"), width=6),
                    ]
                ),
                dcc.Interval(
                    id="interval-component", interval=10 * 1000, n_intervals=0
                ),
            ],
            fluid=True,
        ),
    ]
)


def get_kafka_data():
    """
    Fetch the latest weather data for all stations from the 'weather' Kafka topic via the REST Proxy.

    Returns:
        pd.DataFrame: DataFrame with the latest weather metrics for each city/station, or None if unavailable.
    """
    # Get latest weather data from Kafka topic via REST proxy
    import uuid
    try:
        group_name = "weather-group"
        consumer_name = "weather-consumer"
        consumer_response = requests.post(
            f"http://kafka-rest-proxy:8082/consumers/{group_name}",
            headers={"Content-Type": "application/vnd.kafka.json.v2+json",
                     "Accept": "application/vnd.kafka.json.v2+json"},
            json={
                "name": consumer_name,
                "format": "json",
                "auto.offset.reset": "earliest"
            },
            timeout=5
        )

        if consumer_response.status_code != 200:
            return None
            
        consumer_data = consumer_response.json()
        base_uri = consumer_data["base_uri"]
        subscribe_response = requests.post(
            f"{base_uri}/subscription",
            headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
            json={"topics": ["weather"]},
            timeout=5
        )
        
        if subscribe_response.status_code != 204:
            requests.delete(base_uri) 
            return None
        
        import time
        messages = []
        for _ in range(5):
            records_response = requests.get(
                f"{base_uri}/records",
                headers={"Accept": "application/vnd.kafka.json.v2+json"},
                timeout=5
            )
            if records_response.status_code == 200:
                batch = records_response.json()
                if batch:
                    messages.extend(batch)
                    break
            time.sleep(0.5)
        requests.delete(base_uri)
        print("Received latest weather data from Kafka:", messages)
        if messages:
            data = []
            for msg in messages[-5:]: 
                value = msg['value']  
                data.append({
                    'city_id': value['city_id'],
                    'city_name': value['city_name'],
                    'avg_temperature': value['temperature_celsius'],
                    'avg_humidity': value['humidity_percent'],
                    'avg_wind_speed': value['wind_speed_kmh'],
                    'avg_precipitation': value['precipitation_mm'],
                    'end_time': value['timestamp']
                })
            df = pd.DataFrame(data)
            print("Parsed DataFrame (latest):\n", df)
            return df
                
    except Exception as e:
        print(f"Kafka consumption failed: {e}")
    
    return None

@app.callback(
    [
        Output("station-1-gauge", "figure"),
        Output("station-2-gauge", "figure"),
        Output("station-3-gauge", "figure"),
        Output("station-4-gauge", "figure"),
        Output("station-5-gauge", "figure"),
        Output("temperature-trends", "figure"),
        Output("humidity-wind", "figure"),
        Output("precipitation", "figure"),
    ],
    Input("interval-component", "n_intervals"),
)
def update_metrics(n):
    """
    Dash callback to update all dashboard plots every 10 seconds.
    Fetches latest and historical weather data from Kafka topics and updates:
      - Temperature gauges
      - Temperature trends
      - Humidity and wind speed
      - Precipitation

    Args:
        n: Number of intervals elapsed (unused, triggers update)

    Returns:
        List of Plotly figures for each dashboard component.
    """
    # Always get latest data from Kafka REST Proxy
    df = get_kafka_data()
    historical_data = get_kafka_history()
    if df is None or df.empty or historical_data is None or historical_data.empty:
        # If Kafka fails, return empty plots (or you can show a message)
        empty_fig = go.Figure()
        empty_fig.update_layout(template="seaborn")
        return [empty_fig]*5 + [empty_fig]*3

    # Create gauges for the latest temperatures
    gauges = []
    for i in range(1, 6):
        station_data = df[df["city_id"] == f"city_{i}"]
        temperature = station_data["avg_temperature"].values[0] if not station_data.empty else 0
        station_name = station_data["city_name"].values[0] if not station_data.empty else f"Station {i}"
        gauge = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=temperature,
                domain={"x": [0, 1], "y": [0, 1]},
                title={"text": f"{station_name} Temperature", "align": "center"},
                gauge={"axis": {"range": [-20, 50]}},
            )
        )
        gauge.update_layout(template="seaborn")
        gauges.append(gauge)
    # Temperature Trends Plot
    temp_trends = go.Figure()
    colors = ['#FF0000', '#00FF00', '#0000FF', '#FFA500', '#800080']  # Red, Green, Blue, Orange, Purple
    for i in range(1, 6):  # Loop through all 5 stations
        city_data = historical_data[historical_data["city_id"] == f"city_{i}"]
        if not city_data.empty:
            city_name = city_data["city_name"].iloc[0]
            temp_trends.add_trace(
                go.Scatter(
                    x=city_data["end_time"],
                    y=city_data["avg_temperature"],
                    mode="lines+markers",
                    name=f"{city_name}",
                    line=dict(color=colors[i-1], width=2),
                    marker=dict(color=colors[i-1], size=6)
                )
            )

    temp_trends.update_layout(
        title="Temperature Trends Over Time",
        xaxis_title="Time",
        yaxis_title="Temperature (°C)", 
        legend_title="Station",
        template="seaborn",
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99
        )
    )

    # Humidity and Wind Speed Plot
    humidity_wind = go.Figure()
    humidity_wind.add_trace(
        go.Bar(
            x=df["city_name"],
            y=df["avg_humidity"],
            name="Humidity (%)",
        )
    )
    humidity_wind.add_trace(
        go.Bar(
            x=df["city_name"],
            y=df["avg_wind_speed"],
            name="Wind Speed (m/s)",
        )
    )
    humidity_wind.update_layout(
        barmode="group",
        title="Humidity and Wind Speed by Station",
        xaxis_title="Station",
        yaxis_title="Value",
        template="seaborn",
    )

    # Precipitation Plot
    precipitation = go.Figure(
        go.Bar(
            x=df["city_name"],
            y=df["avg_precipitation"],
            text=df["avg_precipitation"],
            textposition="auto",
        )
    )
    precipitation.update_layout(
        title="Average Precipitation by Station",
        xaxis_title="Station",
        yaxis_title="Precipitation (mm)",
        template="seaborn",
    )

    return gauges + [temp_trends, humidity_wind, precipitation]


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")