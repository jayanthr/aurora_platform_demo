# Aurora Weather Dashboard

This project is a real-time weather dashboard that visualizes simulated weather data for multiple cities using Apache Kafka, a Kafka REST Proxy, and a Dash web application. It is designed for demonstration, analytics, and streaming data engineering use cases.

## Project Structure

```
docker-compose.yml

dash_app/
    app.py                # Dash web application for visualization
    Dockerfile
    requirements.txt
kafka_producer/
    produce_weather_data.py  # Simulates and produces weather data to Kafka
    Dockerfile
    requirements.txt
postgres/
    init/
        init_db.sql
spark_jobs/
    Dockerfile
    requirements.txt
    src/
        spark_streaming_job.py
```

## Components

### 1. Kafka Producer (`kafka_producer/produce_weather_data.py`)
- Simulates weather data for 5 cities.
- Produces messages to two Kafka topics:
  - `weather`: latest weather data for each city (used for dashboard gauges)
  - `weather_history`: historical weather data (used for dashboard trends)

### 2. Dash App (`dash_app/app.py`)
- Fetches latest and historical weather data from Kafka topics via the Kafka REST Proxy.
- Visualizes:
  - Current temperature gauges for each city
  - Temperature trends over the last 30 minutes
  - Humidity, wind speed, and precipitation
- Updates every 10 seconds.

### 3. Kafka REST Proxy
- Allows the Dash app to consume Kafka topics over HTTP.

### 4. (Optional) PostgreSQL & Spark
- The original design included a PostgreSQL database and Spark jobs for historical analytics. The current dashboard fetches all data from Kafka topics.

## How It Works

1. The producer script simulates and sends weather data to Kafka topics.
2. The Dash app consumes the latest and historical data from Kafka via the REST Proxy.
3. The dashboard displays real-time and trend analytics for all cities.

## Running the Project

1. **Start all services** (Kafka, REST Proxy, Dash app, Producer, etc.) using Docker Compose:
   ```sh
   docker-compose up --build
   ```
2. **Access the dashboard** at [http://localhost:8050](http://localhost:8050)

## Requirements
- Docker & Docker Compose
- Python 3.8+
- See `requirements.txt` in each component for Python dependencies

## Customization
- To change the number of cities or weather simulation logic, edit `produce_weather_data.py`.
- To modify dashboard visuals, edit `dash_app/app.py`.

## Documentation
- All functions and files are documented with docstrings.
- See code comments for further details.

## License
MIT License
