# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

# Define mapping: {Target_Table_Name    : Source}
batch_map = {
    "raw_stations"   : "samples.tpch.part",
    "raw_routes"    : "samples.tpch.region",
    "raw_vendors"   : "samples.tpch.supplier",
    "raw_inventory" : "samples.tpch.partsupp"
}

for target, source in batch_map.items():
    print(f"Ingesting {source} into bronze:{target}")
    (
        spark.read.table(source)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit("KYOTO_METRO_BATCH"))
        .write
        .format("delta")
        .mode("overwrite") # Overwrite for the initial load
        .saveAsTable(f"kyotometrodb.kyoto_bronze.{target}")
    )

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, rand

# Stream 1: Real-Time Passenger Swipes
(
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("Passenger_id", (col("value") * 7).cast("string"))
    .withColumn("station_id", (col("value") % 50).cast("int"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/mnt/kyoto_metro/checkpoints/swipes")
    .toTable("kyotometrodb.kyoto_bronze.raw_swipes")
)

# Stream 2: Station IoT Sensors
(
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("sensor_id", (col("value") % 100).cast("int"))
    .withColumn("reading", (rand() * 40).cast("double")) # Temp in Celcius
    .withColumn("ingestion_timestamp", current_timestamp())
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/mnt/kyoto_metro/checkpoints/sensors")
    .toTable("kyotometrodb.kyoto_bronze.raw_sensors")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in kyotometrodb.kyoto_bronze;