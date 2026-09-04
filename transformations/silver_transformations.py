from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =========================================================
# CONFIGURATION
# =========================================================

CATALOG = "workspace"

BRONZE_SCHEMA = "airline_bronze"
SILVER_SCHEMA = "airline_silver"
MONITORING_SCHEMA = "airline_monitoring"

FLIGHT_BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}.flight_events"
WEATHER_BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}.weather_events"
BAGGAGE_BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}.baggage_events"
AIRCRAFT_BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}.aircraft_cdc_events"

FLIGHT_SILVER = f"{CATALOG}.{SILVER_SCHEMA}.flight_events_clean"
WEATHER_SILVER = f"{CATALOG}.{SILVER_SCHEMA}.weather_events_clean"
BAGGAGE_SILVER = f"{CATALOG}.{SILVER_SCHEMA}.baggage_events_clean"
AIRCRAFT_SILVER = f"{CATALOG}.{SILVER_SCHEMA}.aircraft_history"

FLIGHT_QUARANTINE = (
    f"{CATALOG}.{MONITORING_SCHEMA}.flight_events_quarantine"
)


# =========================================================
# CREATE REQUIRED SCHEMAS
# =========================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{CATALOG}.{MONITORING_SCHEMA}"
)

print("Required schemas verified.")


# =========================================================
# 1. SILVER FLIGHT EVENTS
# =========================================================

print("Processing flight events...")

flight_bronze_df = spark.table(FLIGHT_BRONZE)

flight_standardized_df = (
    flight_bronze_df
    .select(
        F.trim(F.col("event_id")).alias("event_id"),
        F.trim(F.col("flight_id")).alias("flight_id"),

        F.upper(
            F.trim(F.col("airline_code"))
        ).alias("airline_code"),

        F.trim(
            F.col("flight_number")
        ).alias("flight_number"),

        F.trim(
            F.col("route_id")
        ).alias("route_id"),

        F.upper(
            F.trim(F.col("origin_airport"))
        ).alias("origin_airport"),

        F.upper(
            F.trim(F.col("destination_airport"))
        ).alias("destination_airport"),

        F.trim(
            F.col("aircraft_id")
        ).alias("aircraft_id"),

        F.to_timestamp(
            F.col("scheduled_departure_utc")
        ).alias("scheduled_departure_utc"),

        F.to_timestamp(
            F.col("scheduled_arrival_utc")
        ).alias("scheduled_arrival_utc"),

        F.upper(
            F.trim(F.col("gate"))
        ).alias("gate"),

        F.upper(
            F.trim(F.col("status"))
        ).alias("status"),

        F.col("delay_minutes")
        .cast("integer")
        .alias("delay_minutes"),

        F.to_timestamp(
            F.col("event_time_utc")
        ).alias("event_time_utc"),

        F.col("batch_id")
        .cast("long")
        .alias("batch_id"),

        F.col("source_system"),
        F.col("_source_file"),
        F.col("_ingested_at")
    )
)


VALID_FLIGHT_STATUSES = [
    "SCHEDULED",
    "BOARDING",
    "DELAYED",
    "DEPARTED",
    "ARRIVED",
    "CANCELLED"
]


flight_valid_condition = (
    F.col("event_id").isNotNull()
    & F.col("flight_id").isNotNull()
    & F.col("origin_airport").isNotNull()
    & F.col("destination_airport").isNotNull()
    & (
        F.col("origin_airport")
        != F.col("destination_airport")
    )
    & F.col("status").isin(VALID_FLIGHT_STATUSES)
    & F.col("delay_minutes").isNotNull()
    & (F.col("delay_minutes") >= 0)
)


flight_clean_df = (
    flight_standardized_df
    .filter(flight_valid_condition)
    .dropDuplicates(["event_id"])
)


flight_quarantine_df = (
    flight_standardized_df
    .filter(~flight_valid_condition)
    .withColumn(
        "quarantine_reason",

        F.when(
            F.col("event_id").isNull(),
            F.lit("MISSING_EVENT_ID")
        )

        .when(
            F.col("flight_id").isNull(),
            F.lit("MISSING_FLIGHT_ID")
        )

        .when(
            F.col("origin_airport").isNull()
            | F.col("destination_airport").isNull(),
            F.lit("MISSING_AIRPORT")
        )

        .when(
            F.col("origin_airport")
            == F.col("destination_airport"),
            F.lit("INVALID_ROUTE")
        )

        .when(
            F.col("status").isNull()
            | ~F.col("status").isin(
                VALID_FLIGHT_STATUSES
            ),
            F.lit("INVALID_STATUS")
        )

        .when(
            F.col("delay_minutes").isNull()
            | (F.col("delay_minutes") < 0),
            F.lit("INVALID_DELAY")
        )

        .otherwise(
            F.lit("UNKNOWN_QUALITY_ERROR")
        )
    )
)


flight_clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FLIGHT_SILVER)


flight_quarantine_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FLIGHT_QUARANTINE)


flight_clean_count = spark.table(
    FLIGHT_SILVER
).count()

flight_quarantine_count = spark.table(
    FLIGHT_QUARANTINE
).count()


print(
    f"Silver flight rows: {flight_clean_count}"
)

print(
    f"Quarantined flight rows: "
    f"{flight_quarantine_count}"
)


# =========================================================
# 2. SILVER WEATHER EVENTS
# =========================================================

print("Processing weather events...")

weather_df = (
    spark.table(WEATHER_BRONZE)
    .select(
        F.trim(
            F.col("weather_event_id")
        ).alias("weather_event_id"),

        F.upper(
            F.trim(F.col("airport_code"))
        ).alias("airport_code"),

        F.to_timestamp(
            F.col("observation_time_utc")
        ).alias("observation_time_utc"),

        F.col("temperature_f")
        .cast("integer")
        .alias("temperature_f"),

        F.col("wind_speed_mph")
        .cast("integer")
        .alias("wind_speed_mph"),

        F.col("visibility_miles")
        .cast("integer")
        .alias("visibility_miles"),

        F.col("precipitation_inches")
        .cast("double")
        .alias("precipitation_inches"),

        F.upper(
            F.trim(F.col("weather_condition"))
        ).alias("weather_condition"),

        F.col("weather_severity")
        .cast("integer")
        .alias("weather_severity"),

        F.col("batch_id")
        .cast("long")
        .alias("batch_id"),

        F.col("source_system"),
        F.col("_source_file"),
        F.col("_ingested_at")
    )
)


weather_clean_df = (
    weather_df
    .filter(
        F.col("weather_event_id").isNotNull()
        & F.col("airport_code").isNotNull()
        & F.col("weather_severity").between(0, 2)
        & (F.col("visibility_miles") >= 0)
        & (F.col("wind_speed_mph") >= 0)
    )
    .dropDuplicates(["weather_event_id"])
)


weather_clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(WEATHER_SILVER)


weather_count = spark.table(
    WEATHER_SILVER
).count()

print(
    f"Silver weather rows: {weather_count}"
)


# =========================================================
# 3. SILVER BAGGAGE EVENTS
# =========================================================

print("Processing baggage events...")

VALID_BAGGAGE_STATUSES = [
    "CHECKED_IN",
    "SECURITY_CLEARED",
    "LOADED",
    "TRANSFERRED",
    "ARRIVED",
    "MISROUTED"
]


baggage_df = (
    spark.table(BAGGAGE_BRONZE)
    .select(
        F.trim(
            F.col("baggage_event_id")
        ).alias("baggage_event_id"),

        F.trim(
            F.col("bag_tag_id")
        ).alias("bag_tag_id"),

        F.trim(
            F.col("flight_id")
        ).alias("flight_id"),

        F.trim(
            F.col("passenger_id")
        ).alias("passenger_id"),

        F.upper(
            F.trim(F.col("airport_code"))
        ).alias("airport_code"),

        F.upper(
            F.trim(F.col("baggage_status"))
        ).alias("baggage_status"),

        F.upper(
            F.trim(F.col("scan_location"))
        ).alias("scan_location"),

        F.to_timestamp(
            F.col("event_time_utc")
        ).alias("event_time_utc"),

        F.col("batch_id")
        .cast("long")
        .alias("batch_id"),

        F.col("source_system"),
        F.col("_source_file"),
        F.col("_ingested_at")
    )
)


baggage_clean_df = (
    baggage_df
    .filter(
        F.col("baggage_event_id").isNotNull()
        & F.col("bag_tag_id").isNotNull()
        & F.col("flight_id").isNotNull()
        & F.col("airport_code").isNotNull()
        & F.col("baggage_status").isin(
            VALID_BAGGAGE_STATUSES
        )
    )
    .dropDuplicates(["baggage_event_id"])
)


baggage_clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BAGGAGE_SILVER)


baggage_count = spark.table(
    BAGGAGE_SILVER
).count()

print(
    f"Silver baggage rows: {baggage_count}"
)


# =========================================================
# 4. AIRCRAFT CDC -> SCD TYPE 2
# =========================================================

print("Processing aircraft CDC / SCD Type 2...")

aircraft_cdc_df = (
    spark.table(AIRCRAFT_BRONZE)
    .select(
        F.col("cdc_event_id"),

        F.trim(
            F.col("aircraft_id")
        ).alias("aircraft_id"),

        F.upper(
            F.trim(F.col("tail_number"))
        ).alias("tail_number"),

        F.upper(
            F.trim(F.col("airline_code"))
        ).alias("airline_code"),

        F.trim(
            F.col("manufacturer")
        ).alias("manufacturer"),

        F.trim(
            F.col("model")
        ).alias("model"),

        F.col("seat_capacity")
        .cast("integer")
        .alias("seat_capacity"),

        F.upper(
            F.trim(F.col("operational_status"))
        ).alias("operational_status"),

        F.col("in_service_date")
        .cast("date")
        .alias("in_service_date"),

        F.upper(
            F.trim(F.col("operation"))
        ).alias("operation"),

        F.col("sequence_number")
        .cast("long")
        .alias("sequence_number"),

        F.to_timestamp(
            F.col("effective_time_utc")
        ).alias("effective_time_utc"),

        F.col("batch_id")
        .cast("long")
        .alias("batch_id"),

        F.col("source_system")
    )
    .filter(
        F.col("aircraft_id").isNotNull()
        & F.col("sequence_number").isNotNull()
        & F.col("effective_time_utc").isNotNull()
    )
    .dropDuplicates(["cdc_event_id"])
)


aircraft_window = (
    Window
    .partitionBy("aircraft_id")
    .orderBy(
        F.col("sequence_number"),
        F.col("effective_time_utc")
    )
)


aircraft_history_df = (
    aircraft_cdc_df

    .withColumn(
        "effective_from",
        F.col("effective_time_utc")
    )

    .withColumn(
        "effective_to",
        F.lead(
            "effective_time_utc"
        ).over(aircraft_window)
    )

    .withColumn(
        "is_current",
        (
            F.col("effective_to").isNull()
            & (F.col("operation") != "DELETE")
        )
    )

    # DELETE events close the prior version,
    # but are not stored as active dimension records.
    .filter(
        F.col("operation") != "DELETE"
    )

    .select(
        "aircraft_id",
        "tail_number",
        "airline_code",
        "manufacturer",
        "model",
        "seat_capacity",
        "operational_status",
        "in_service_date",
        "sequence_number",
        "effective_from",
        "effective_to",
        "is_current",
        "cdc_event_id",
        "batch_id",
        "source_system"
    )
)


aircraft_history_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(AIRCRAFT_SILVER)


aircraft_count = spark.table(
    AIRCRAFT_SILVER
).count()

current_aircraft_count = (
    spark.table(AIRCRAFT_SILVER)
    .filter(F.col("is_current") == True)
    .count()
)


print(
    f"Aircraft history rows: {aircraft_count}"
)

print(
    f"Current aircraft rows: "
    f"{current_aircraft_count}"
)


# =========================================================
# FINAL VALIDATION
# =========================================================

print("")
print("=" * 60)
print("SILVER TRANSFORMATION COMPLETE")
print("=" * 60)

print(
    f"flight_events_clean: "
    f"{flight_clean_count}"
)

print(
    f"flight_events_quarantine: "
    f"{flight_quarantine_count}"
)

print(
    f"weather_events_clean: "
    f"{weather_count}"
)

print(
    f"baggage_events_clean: "
    f"{baggage_count}"
)

print(
    f"aircraft_history: "
    f"{aircraft_count}"
)

print(
    f"current_aircraft: "
    f"{current_aircraft_count}"
)

print("=" * 60)