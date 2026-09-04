from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =========================================================
# GOLD CONFIGURATION
# =========================================================

CATALOG = "workspace"
SILVER_SCHEMA = "airline_silver"
BRONZE_SCHEMA = "airline_bronze"
GOLD_SCHEMA = "airline_gold"

FLIGHT_SILVER = (
    f"{CATALOG}.{SILVER_SCHEMA}.flight_events_clean"
)

WEATHER_SILVER = (
    f"{CATALOG}.{SILVER_SCHEMA}.weather_events_clean"
)

ROUTES_BRONZE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.routes"
)

FLIGHT_SUMMARY = (
    f"{CATALOG}.{GOLD_SCHEMA}.flight_operations_summary"
)

FLIGHT_FEATURES = (
    f"{CATALOG}.{GOLD_SCHEMA}.flight_delay_features"
)


# =========================================================
# 1. GOLD FLIGHT OPERATIONS SUMMARY
# =========================================================

print("Building flight_operations_summary...")

flights_df = spark.table(FLIGHT_SILVER)

flight_operations_summary_df = (
    flights_df
    .withColumn(
        "flight_date",
        F.to_date(
            F.col("scheduled_departure_utc")
        )
    )
    .groupBy(
        "flight_date",
        "airline_code",
        "origin_airport",
        "destination_airport",
        "status"
    )
    .agg(
        F.count("*").alias(
            "flight_count"
        ),
        F.sum(
            F.when(
                F.col("delay_minutes") > 15,
                1
            ).otherwise(0)
        ).alias(
            "delayed_flight_count"
        ),
        F.sum(
            F.when(
                F.col("delay_minutes") <= 15,
                1
            ).otherwise(0)
        ).alias(
            "on_time_flight_count"
        ),
        F.round(
            F.avg("delay_minutes"),
            2
        ).alias(
            "average_delay_minutes"
        ),
        F.max(
            "delay_minutes"
        ).alias(
            "maximum_delay_minutes"
        )
    )
)

flight_operations_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FLIGHT_SUMMARY)

summary_row_count = (
    spark.table(FLIGHT_SUMMARY)
    .count()
)

summary_flight_count = (
    spark.table(FLIGHT_SUMMARY)
    .agg(
        F.sum("flight_count").alias("total")
    )
    .collect()[0]["total"]
)

print(
    f"flight_operations_summary rows: "
    f"{summary_row_count}"
)

print(
    f"Flights represented in summary: "
    f"{summary_flight_count}"
)


# =========================================================
# 2. LATEST WEATHER BY AIRPORT
# =========================================================

print("Preparing latest weather observations...")

latest_weather_window = (
    Window
    .partitionBy("airport_code")
    .orderBy(
        F.col("observation_time_utc").desc()
    )
)

latest_weather_df = (
    spark.table(WEATHER_SILVER)
    .withColumn(
        "weather_rank",
        F.row_number().over(
            latest_weather_window
        )
    )
    .filter(
        F.col("weather_rank") == 1
    )
    .drop(
        "weather_rank"
    )
)


# =========================================================
# 3. GOLD FLIGHT DELAY FEATURES
# =========================================================

print("Building flight_delay_features...")

flights = (
    spark.table(FLIGHT_SILVER)
    .alias("flights")
)

routes = (
    spark.table(ROUTES_BRONZE)
    .alias("routes")
)

weather = (
    latest_weather_df
    .alias("weather")
)

flight_delay_features_df = (
    flights
    .join(
        routes,
        F.col("flights.route_id")
        == F.col("routes.route_id"),
        "left"
    )
    .join(
        weather,
        F.col("flights.origin_airport")
        == F.col("weather.airport_code"),
        "left"
    )
    .select(
        F.col("flights.event_id")
        .alias("event_id"),

        F.col("flights.flight_id")
        .alias("flight_id"),

        F.col("flights.airline_code")
        .alias("airline_code"),

        F.col("flights.origin_airport")
        .alias("origin_airport"),

        F.col("flights.destination_airport")
        .alias("destination_airport"),

        F.col("flights.aircraft_id")
        .alias("aircraft_id"),

        F.col("flights.status")
        .alias("status"),

        F.col("flights.scheduled_departure_utc")
        .alias("scheduled_departure_utc"),

        F.hour(
            F.col("flights.scheduled_departure_utc")
        ).alias(
            "departure_hour"
        ),

        F.dayofweek(
            F.col("flights.scheduled_departure_utc")
        ).alias(
            "departure_day_of_week"
        ),

        F.col("routes.distance_miles")
        .cast("integer")
        .alias(
            "distance_miles"
        ),

        F.col("routes.scheduled_duration_minutes")
        .cast("integer")
        .alias(
            "scheduled_duration_minutes"
        ),

        F.col("weather.temperature_f")
        .alias(
            "temperature_f"
        ),

        F.col("weather.wind_speed_mph")
        .alias(
            "wind_speed_mph"
        ),

        F.col("weather.visibility_miles")
        .alias(
            "visibility_miles"
        ),

        F.col("weather.precipitation_inches")
        .alias(
            "precipitation_inches"
        ),

        F.col("weather.weather_severity")
        .alias(
            "weather_severity"
        ),

        F.col("flights.delay_minutes")
        .alias(
            "delay_minutes"
        ),

        F.when(
            F.col("flights.delay_minutes") > 15,
            1
        )
        .otherwise(0)
        .alias(
            "is_delayed"
        )
    )
)

flight_delay_features_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FLIGHT_FEATURES)

feature_row_count = (
    spark.table(FLIGHT_FEATURES)
    .count()
)

delayed_flight_count = (
    spark.table(FLIGHT_FEATURES)
    .agg(
        F.sum("is_delayed").alias("total")
    )
    .collect()[0]["total"]
)

on_time_flight_count = (
    feature_row_count
    - delayed_flight_count
)


# =========================================================
# FINAL VALIDATION
# =========================================================

print("")
print("=" * 60)
print("GOLD TRANSFORMATION COMPLETE")
print("=" * 60)

print(
    f"flight_operations_summary rows: "
    f"{summary_row_count}"
)

print(
    f"summarized flights: "
    f"{summary_flight_count}"
)

print(
    f"flight_delay_features rows: "
    f"{feature_row_count}"
)

print(
    f"delayed flights: "
    f"{delayed_flight_count}"
)

print(
    f"on-time flights: "
    f"{on_time_flight_count}"
)

print("=" * 60)

