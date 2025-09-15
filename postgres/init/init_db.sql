DROP TABLE IF EXISTS weather_metrics;

CREATE TABLE weather_metrics (
    city_id TEXT,
    city_name TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    avg_temperature DECIMAL(5, 2),
    avg_humidity DECIMAL(5, 2),
    avg_wind_speed DECIMAL(5, 2),
    avg_precipitation DECIMAL(5, 2)
);
