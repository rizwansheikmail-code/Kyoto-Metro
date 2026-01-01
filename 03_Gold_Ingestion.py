# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog kyotometrodb;
# MAGIC use schema kyoto_silver;

# COMMAND ----------

from pyspark.sql.functions import col

# Join Hub and Satellite, filtering for the most recent record (latest load_date)
# In production, you would often use a 'Point-in-Time' (PIT) table for this.

dim_station = spark.sql("""
                        select
                        h.hk_station,
                        h.station_id,
                        s.station_name,
                        s.region_name,
                        s.station_type,
                        s.load_date as valid_from
                        from hub_station h
                        join sat_station_details s
                        on h.hk_station = s.hk_station
                        qualify row_number() 
                        over(partition by h.hk_station order by s.load_date desc) =1
                        
                        """)

dim_station.write.mode("overwrite").saveAsTable("kyotometrodb.kyoto_gold.dim_station")

# COMMAND ----------

# Flattening the Link and adding a date dimension key

fact_ridership = spark.sql("""
                           select
                           hk_link_ridership as fact_id,
                           hk_passenger,
                           hk_station,
                           load_date as event_timestamp,
                           date_format(load_date, 'yyyyMMdd') as date_key,
                           hour(load_date) as hour_of_day
                           from link_ridership
                           """)

fact_ridership.write.mode("overwrite").saveAsTable("kyotometrodb.kyoto_gold.fact_ridership")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in kyoto_gold;