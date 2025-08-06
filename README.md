# RideTrack360

## Overview
RideTrack360 is a comprehensive big data analytics platform for ride-hailing services, focusing on New York City operations. It processes real-time GPS tracking, weather conditions, and historical taxi data to provide actionable insights and predictions. The platform features robust streaming and batch data pipelines, cloud-native storage, and advanced analytics, all orchestrated for reliability and scalability.

## Architecture

### Diagram Walkthrough

```mermaid
flowchart TD
    %% Sources
    GPS["GPS Data Producer (Kafka)"]
    Weather["Weather Data Source"]
    Taxi["NYC Taxi (Historical)"]

    %% Processing
    SparkStream["Spark Streaming"]
    Airflow["Airflow Batch ETL"]
    dbt["dbt Models"]

    %% Shared Storage
    S3["S3 Data Lake"]
    Snowflake["Snowflake Data Warehouse"]

    %% Dashboards
    Streamlit["Streamlit Real-Time "]
    PowerBI["Power BI Dashboard"]

    %% Streaming Path
    GPS -->|Stream| SparkStream
    Weather -->|Stream| SparkStream
    SparkStream --> S3
    S3 --> Snowflake
    Snowflake --> Streamlit

    %% Batch Path
    Taxi --> Airflow
    Airflow --> S3
    S3 --> Snowflake
    Snowflake --> dbt
    dbt --> PowerBI
```

- Both streaming and batch pipelines share S3 Data Lake and Snowflake Data Warehouse.
- The streaming pipeline flows from GPS/Weather → Spark Streaming → S3 → Snowflake → Streamlit.
- The batch pipeline flows from NYC Taxi Data → Airflow → S3 → Snowflake → dbt → Power BI.
