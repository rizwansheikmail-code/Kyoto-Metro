-- Databricks notebook source
SELECT 
    f.event_timestamp,
    f.hour_of_day,
    s.station_name,
    s.region_name,
    s.station_type,
    COUNT(f.fact_id) AS total_swipes
FROM kyotometrodb.kyoto_gold.fact_ridership f
JOIN kyotometrodb.kyoto_gold.dim_station s 
    ON f.hk_station = s.hk_station
GROUP BY ALL