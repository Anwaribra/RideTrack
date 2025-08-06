# RideTrack360

## Overview
RideTrack360 is a comprehensive big data analytics platform for ride-hailing services, focusing on New York City operations. It processes real-time GPS tracking, weather conditions, and historical taxi data to provide actionable insights and predictions. The platform features robust streaming and batch data pipelines, cloud-native storage, and advanced analytics, all orchestrated for reliability and scalability.

## Architecture


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
    ClickHouse["ClickHouse Data Warehouse"]

    %% Dashboards
    Streamlit["Streamlit Real-Time "]
    PowerBI["Power BI Dashboard"]

    %% Streaming Path
    GPS -->|Stream| SparkStream
    Weather -->|Stream| SparkStream
    SparkStream --> S3
    S3 --> ClickHouse
    ClickHouse --> Streamlit

    %% Batch Path
    Taxi --> S3
    S3 --> Airflow
    Airflow --> ClickHouse
    ClickHouse --> dbt
    dbt --> PowerBI
```

- Both streaming and batch pipelines share S3 Data Lake and ClickHouse Data Warehouse.
- The streaming pipeline flows from GPS/Weather → Spark Streaming → S3 → ClickHouse → Streamlit.
- The batch pipeline flows from NYC Taxi Data → S3 → Airflow → ClickHouse → dbt → Power BI.

