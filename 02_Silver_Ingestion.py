# Databricks notebook source
from pyspark.sql.functions import md5, upper, trim, col, lit, current_timestamp

# -- HUB STATION --
# -- Source: raw_stations
hub_station = (
    spark.read.table("kyotometrodb.kyoto_bronze.raw_stations")
    .select(
        md5(upper(trim(col("p_partkey").cast("string")))).alias("hk_station"),
            col("p_partkey").alias("station_id"),
            lit("METRO_ERP").alias("record_source"),
            current_timestamp().alias("load_date")
    ).distinct()
)

hub_station.write.mode("overwrite").saveAsTable("kyotometrodb.kyoto_silver.hub_station")

# -- HUB PASSENGER --
# -- Source: raw_swipes (Taking unique passenger IDs from the stream)
hub_passenger = (
    spark.read.table("kyotometrodb.kyoto_bronze.raw_swipes")
    .select(
        md5(upper(trim(col("passenger_id").cast("string")))).alias("hk_passenger"),
        col("passenger_id").alias("passenger_id"),
        lit("TURNSTILE_STREAM").alias("record_source"),
        current_timestamp().alias("current_timestamp")
    ).distinct()
)

hub_passenger.write.mode("overwrite").saveAsTable("kyotometrodb.kyoto_silver.hub_passenger")

# COMMAND ----------

from pyspark.sql.functions import concat_ws

# -- SAT STATION DETAILS --
sat_station = (
    spark.read.table("kyotometrodb.kyoto_bronze.raw_stations")
    .select(
        md5(upper(trim(col("p_partkey").cast("string")))).alias("hk_station"),
        current_timestamp().alias("load_date"),
        # HASH DIFF captures all descriptive columns to detect changes
        md5(concat_ws("|", col("p_name"), col("p_mfgr"), col("p_type"))).alias("hash_diff"),
        col("p_name").alias("station_name"),
        col("p_mfgr").alias("region_name"),
        col("p_type").alias("station_type"),
        lit("METRO_ERP").alias("record_source")
    )
)

sat_station.write.mode("overwrite").saveAsTable("kyotometrodb.kyoto_silver.sat_station_details")


# COMMAND ----------

from pyspark.sql.functions import md5, upper, trim, col, lit, current_timestamp, concat

# -- LINK RIDERSHIP --
# This connects the Passenger Hub to the Station Hub
link_ridership = (
    spark.read.table("kyotometrodb.kyoto_bronze.raw_swipes")
    .select(
        # The PK of the link is a hash of the combined Business keys
        md5(concat(col("passenger_id"), col("station_id"))).alias("hk_link_ridership"),
        # Foreign keys to our Hubs
        md5(col("passenger_id").cast("string")).alias("hk_passenger"),
        md5(col("station_id").cast("string")).alias("hk_station"),
        # Metadata
        current_timestamp().alias("load_date"),
        lit("TURNSTILE_STREAM").alias("record_source")
    
    )
)

link_ridership.write.mode("overwrite").saveAsTable("kyotometrodb.kyoto_silver.link_ridership")

# COMMAND ----------

# -- SAT SENSOR READINGS --
# This tracks the state of a sensor at a specific point in time

sat_sensors = (
    spark.read.table("kyotometrodb.kyoto_bronze.raw_sensors")
    .select(
        md5(upper(trim(col("sensor_id").cast("string")))).alias("hk_sensor"),
        current_timestamp().alias("load_date"),
        col("reading").alias("temperature_celsius"),
        lit("IOT_STREAM").alias("record_source")
    )
)

sat_sensors.write.mode("overwrite").saveAsTable("kyotometrodb.kyoto_silver.sat_sensor_readings")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog kyotometrodb;
# MAGIC
# MAGIC show tables in kyoto_silver;

# COMMAND ----------

