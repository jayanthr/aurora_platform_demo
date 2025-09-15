"""
Kafka Weather Data Producer
--------------------------
This script simulates weather data for multiple cities and produces messages to two Kafka topics:
    - 'weather': for the latest weather data (used by the dashboard for current readings)
    - 'weather_history': for historical weather data (used by the dashboard for trend analysis)

Each message contains weather metrics for a city, including temperature, humidity, wind speed, precipitation, and a timestamp.
"""
import json
import random
from datetime import datetime
from time import sleep

import numpy as np
from confluent_kafka import Producer


# Configure Kafka Producer
conf = {
    "bootstrap.servers": "kafka:9092",
    "client.id": "weather-producer",
    "api.version.request": True,
    "security.protocol": "PLAINTEXT",
}

producer = Producer(conf)


def delivery_report(err, msg) -> None:
    """
    Callback for reporting the delivery status of a message to Kafka.

    Args:
        err: Error information if the message failed to deliver.
        msg: Message object if the delivery was successful.
    """
    """
    Callback for reporting the delivery status of a message.

    Args:
        err: Error information if the message failed to deliver.
        msg: Message object if the delivery was successful.

    Returns:
        None
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")



def simulate_weather_data(city_id: int) -> str:
    """
    Simulate weather data for a given city ID, generating random weather attributes.

    Args:
        city_id: Integer representing the city ID.

    Returns:
        JSON string containing the simulated weather data.
    """
    """
    Simulate weather data for a given city ID, generating random weather attributes.

    Args:
        city_id: Integer representing the city ID.

    Returns:
        JSON string containing the simulated weather data.
    """
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    conditions = ["Sunny", "Cloudy", "Rainy", "Snowy", "Windy"]
    temperatures = [random.uniform(-10, 35) for _ in range(len(cities))]
    humidities = [random.uniform(20, 90) for _ in range(len(cities))]
    wind_speeds = [random.uniform(0, 20) for _ in range(len(cities))]
    precipitations = [random.uniform(0, 10) for _ in range(len(cities))]

    city_index = city_id % len(cities)
    
    weather_data = {
        "timestamp": datetime.now().isoformat(),
        "city_id": f"city_{city_id}",
        "city_name": cities[city_index],
        "temperature_celsius": temperatures[city_index],
        "humidity_percent": humidities[city_index],
        "wind_speed_kmh": wind_speeds[city_index],
        "precipitation_mm": precipitations[city_index],
        "condition": random.choice(conditions)
    }
    
    return json.dumps(weather_data)


def produce_data() -> None:
    """
    Continuously produce simulated weather data for multiple cities.

    Sends the data to both 'weather' and 'weather_history' Kafka topics with a callback for delivery reports.
    """
    """
    Continuously produce simulated weather data for multiple cities.

    Sends the data to a Kafka topic with a callback for delivery reports.
    """
    city_count = 5  # Number of different cities
    while True:
        for city_id in range(1, city_count + 1):
            data = simulate_weather_data(city_id)
            producer.poll(0)
            # Send to both 'weather' and 'weather_history' topics
            producer.produce("weather", value=data, callback=delivery_report)
            producer.produce("weather_history", value=data, callback=delivery_report)
            print(f"Sent data: {data}")
        producer.flush()
        sleep(0.1)  # Simulate data production delay




if __name__ == "__main__":
    produce_data()
