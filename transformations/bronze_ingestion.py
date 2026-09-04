from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


BASE_PATH = (
    "/Volumes/workspace/airline_ops/"
    "landing_volume"
)


def read_json_stream(source_path):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn(
            "_source_file",
            col("_metadata.file_path")
        )
        .withColumn(
            "_ingested_at",
            current_timestamp()
        )
    )


@dp.table(
    name="flight_events",
    comment="Raw flight events incrementally ingested with Auto Loader",
    table_properties={
        "quality": "bronze",
        "source_system": "flight_operations"
    }
)
def bronze_flight_events():
    return read_json_stream(
        f"{BASE_PATH}/flights"
    )


@dp.table(
    name="weather_events",
    comment="Raw airport weather observations ingested with Auto Loader",
    table_properties={
        "quality": "bronze",
        "source_system": "airport_weather_service"
    }
)
def bronze_weather_events():
    return read_json_stream(
        f"{BASE_PATH}/weather"
    )


@dp.table(
    name="aircraft_cdc_events",
    comment="Raw aircraft change-data-capture events",
    table_properties={
        "quality": "bronze",
        "source_system": "aircraft_management"
    }
)
def bronze_aircraft_cdc_events():
    return read_json_stream(
        f"{BASE_PATH}/aircraft_cdc"
    )


@dp.table(
    name="baggage_events",
    comment="Raw baggage tracking events ingested with Auto Loader",
    table_properties={
        "quality": "bronze",
        "source_system": "baggage_tracking"
    }
)
def bronze_baggage_events():
    return read_json_stream(
        f"{BASE_PATH}/baggage"
    )


@dp.table(
    name="airports",
    comment="Raw airport reference data",
    table_properties={
        "quality": "bronze",
        "source_system": "reference_data"
    }
)
def bronze_airports():
    return read_json_stream(
        f"{BASE_PATH}/reference/airports"
    )


@dp.table(
    name="airlines",
    comment="Raw airline reference data",
    table_properties={
        "quality": "bronze",
        "source_system": "reference_data"
    }
)
def bronze_airlines():
    return read_json_stream(
        f"{BASE_PATH}/reference/airlines"
    )


@dp.table(
    name="aircraft",
    comment="Raw aircraft reference snapshot",
    table_properties={
        "quality": "bronze",
        "source_system": "reference_data"
    }
)
def bronze_aircraft():
    return read_json_stream(
        f"{BASE_PATH}/reference/aircraft"
    )


@dp.table(
    name="routes",
    comment="Raw airline route reference data",
    table_properties={
        "quality": "bronze",
        "source_system": "reference_data"
    }
)
def bronze_routes():
    return read_json_stream(
        f"{BASE_PATH}/reference/routes"
    )