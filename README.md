# RideTrack360

## Overview
RideTrack360 is a comprehensive big data analytics platform for ride-hailing services, focusing on New York City operations. It processes real-time GPS tracking, weather conditions, and historical taxi data to provide actionable insights and predictions.

## Architecture

### 1. Data Ingestion Layer
Three main data sources are processed through dedicated Python scripts:

#### Real-time Data Sources
1. **GPS Tracking** (`gps_data_producer.py`)
   - Simulates vehicle locations in NYC
   - Updates every 2 seconds
   - JSON format with vehicle ID, coordinates, speed
   - Streams to Kafka topic: `gps_topic`

2. **Weather Data** (`weather_api_ingest.py`)
   - Real-time NYC weather from OpenWeather API
   - Updates every 5 minutes
   - Temperature, conditions, humidity
   - Streams to Kafka topic: `weather_topic`

#### Batch Data Source
3. **NYC Taxi Data** (`nyc_taxi_ingest.py`)
   - Monthly historical trip data
   - Parquet files from NYC TLC
   - Automated download and S3 storage
   - Source: NYC Taxi & Limousine Commission

### 2. Data Processing
- **Stream Processing**: Apache Spark Structured Streaming
  - Real-time GPS and weather data integration
  - Live metrics calculation
  - Anomaly detection

- **Batch Processing**: Apache Spark & dbt
  - Historical data transformation
  - Feature engineering for ML
  - Analytics-ready views

### 3. Storage Solutions
- **Data Lake**: Amazon S3
  - Raw data storage
  - Processed data partitions
  - Cost-effective long-term storage

- **Data Warehouse**: Snowflake
  - Structured analytics tables
  - Optimized for querying
  - Multi-tenant access

### 4. Analytics & ML
- **Real-time Analytics**
  - Vehicle tracking
  - Demand patterns
  - Weather impact analysis

- **Predictive Models**
  - ETA predictions
  - Demand forecasting
  - Weather-based adjustments

### 5. Visualization
- **Real-time Dashboard**: Streamlit
  - Live maps
  - Current metrics
  - Active vehicle tracking

- **Analytics Dashboard**: Power BI
  - Historical trends
  - KPI tracking
  - Custom reports

[GPS Data Producer (Kafka)] ---> [Spark Streaming] ---\
[Weather API Ingest] -----------/                     \
                                                      [S3 Data Lake] ---> [Snowflake Data Warehouse] ---> [dbt Models] ---> [Power BI Dashboard]
[NYC Taxi Dataset (Batch)] ---> [Airflow Batch ETL] --/

[Streamlit Real-Time Dashboard] <--- [Spark Streaming Output]
