from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    sum,
    avg,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


def write_to_postgres(df, epoch_id, table_name: str) -> None:
    """
    Writes a DataFrame to PostgreSQL using JDBC.

    Args:
        df: DataFrame to write.
        epoch_id: Epoch ID of the streaming batch.
        table_name: Name of the PostgreSQL table.

    This function uses JDBC to append data to a PostgreSQL table, ensuring that
    data is persisted in a relational database for later analysis and reporting.
    """
    jdbc_url = "jdbc:postgresql://postgres:5432/weather"
    properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver",
    }
    df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=properties)


def main() -> None:
    """
    Main function to initialize Spark session and process streaming data.
    """
    # Initialize Spark session for processing
    spark = SparkSession.builder.appName("WeatherStreamExample").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Kafka configuration details
    kafka_topic_name = "weather"
    kafka_bootstrap_servers = "kafka:9092"

    schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("city_id", StringType(), True),
            StructField("city_name", StringType(), True),
            StructField("temperature_celsius", FloatType(), True),
            StructField("humidity_percent", FloatType(), True),
            StructField("wind_speed_kmh", FloatType(), True),
            StructField("precipitation_mm", FloatType(), True),
            StructField("condition", StringType(), True),
        ]
    )

    # Read data from Kafka using structured streaming
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse the JSON data and apply the schema
    json_df = df.selectExpr("CAST(value AS STRING) as json").select(
        from_json(col("json"), schema).alias("data")
    )

  # Extract relevant fields from the JSON data
    exploded_df = json_df.select(
        to_timestamp(col("data.timestamp")).alias("timestamp"),
        "data.city_id",
        "data.city_name",
        "data.temperature_celsius",
        "data.humidity_percent",
        "data.wind_speed_kmh",
        "data.precipitation_mm",
        "data.condition",
    )
 # Aggregate data based on city and a sliding window
    weather_metrics_df = (
        exploded_df.groupBy("city_id", "city_name", window("timestamp", "10 seconds"))
        .agg(
            avg("temperature_celsius").alias("avg_temperature"),
            avg("humidity_percent").alias("avg_humidity"),
            avg("wind_speed_kmh").alias("avg_wind_speed"),
            avg("precipitation_mm").alias("avg_precipitation"),
        )
        .select(
            "city_id",
            "city_name",
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            "avg_temperature",
            "avg_humidity",
            "avg_wind_speed",
            "avg_precipitation",
        )
    )
    

    # Setup the streaming query to write the aggregated results to PostgreSQL
    app_query = (
        weather_metrics_df.writeStream.outputMode("update")
        .foreachBatch(
            lambda df, epoch_id: write_to_postgres(df, epoch_id, "weather_metrics")
        )
        .start()
    )

    # Wait for all processing to be done
    app_query.awaitTermination()


if __name__ == "__main__":
    main()
