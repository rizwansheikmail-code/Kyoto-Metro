**Project Overview**

This project demonstrates a production-grade data engineering pipeline for a simulated transit system, the Kyoto Metro. By leveraging the Databricks Lakehouse Platform, the system integrates high-frequency IoT sensor data and passenger turnstile streams with static operational batch data.

The architecture is built on a Medallion (Multi-hop) Design, specifically utilizing a Data Vault 2.0 model in the Silver layer to ensure long-term scalability and a full audit trail of all metro operations.

**Architectural Highlights**

Bronze (Raw): 

Ingests raw data "as-is" from multiple sources. Uses Spark Structured Streaming (Kappa-style) for passenger swipes and IoT sensors, alongside batch ingestion for station and vendor master data.

Silver (Raw Vault): Implements a Data Vault 2.0 methodology.

1. Hubs: Capture unique business keys (e.g., Station IDs, Passenger IDs).
2. Links: Record transactions and relationships (e.g., Ridership events).
3. Satellites: Store all descriptive context (e.g., Station names, sensor readings) using MD5 Hashing for change detection and deterministic key generation.

Gold (Information Mart): 

Transforms the normalized Vault structures into a Star Schema (Facts and Dimensions), optimized for business intelligence and high-performance SQL queries.

**Dashboard**
<img width="1392" height="737" alt="image" src="https://github.com/user-attachments/assets/ce2ff42b-75c0-4555-88c2-dd5981accf31" />

**Note**
This project utilizes the Databricks samples.tpch and rate_source datasets to simulate high-volume metro operations. While station names and metrics are generated from these synthetic sources, the underlying architecture is designed to scale to real-world production telemetry.

**Tech Stack**

Platform: Databricks (Unity Catalog enabled)

Storage: Delta Lake (ensuring ACID transactions)

Processing: PySpark (Batch & Structured Streaming)

Modeling: Data Vault 2.0 & Dimensional Modeling

Analytics: Databricks SQL & AI/BI Dashboards


This project provides a robust foundation for a university or enterprise-level data warehouse, showing how to maintain a historical, auditable integration layer while serving polished analytics to business users.
