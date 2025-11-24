# databricks-medallion-sales-pipeline
End-to-end ELT pipeline using PySpark, Delta Lake, and Medallion Architecture.
# Global Superstore Sales Pipeline (Medallion Architecture)

## Project Overview
This project is an end-to-end Data Engineering pipeline built on **Databricks** using **PySpark** and **Delta Lake**. It processes raw sales data through a **Medallion Architecture** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) to ensure data quality and business readiness.

## Architecture & Features
* **Bronze Layer:** Raw data ingestion from CSV to Delta format.
* **Silver Layer (Data Cleaning):**
    * **Schema Enforcement:** Standardized column naming conventions (removing spaces/special characters).
    * **Data Quality:** Deduplication of unique Order IDs and date formatting.
* **Gold Layer (Business Aggregation):** Aggregated sales and profit metrics by Country and Segment for reporting.
* **Disaster Recovery:** Implemented **Delta Time Travel** to restore accidental data deletion (simulated scenario restoring 4,985 rows).

## Tech Stack
* **Compute:** Databricks (Spark)
* **Storage:** Delta Lake
* **Language:** Python (PySpark) & SQL

## Key Results
**Gold Layer (Business Report):**
![Gold Layer Report](gold_layer_report.png)

**Time Travel (Data Recovery):**
![Time Travel Proof](https://github.com/ntokozo078/databricks-medallion-sales-pipeline/blob/main/Screenshot%202025-11-24%20183014.png?raw=true
)
