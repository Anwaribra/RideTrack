# RideTrack


## Overview
RideTrack is an intelligent system for real-time monitoring and analysis of ride data, predicting demand and arrival times using GPS data, weather conditions, and historical trip data.  
It combines **Big Data Engineering**, **Real-Time Analytics**, and **Machine Learning** to simulate ride-hailing platforms like Uber or Careem.

---

## Project Components

### 1. Ingestion Layer
- `gps_data_producer.py`: Generates simulated real-time GPS data for vehicles.
- `weather_api_ingest.py`: Pulls live weather data from OpenWeather API.
- `nyc_taxi_ingest.py`: Fetches historical trip data from the NYC Taxi public dataset.
- Transport & Messaging:
  - **Kafka** for streaming data.
  - **Amazon S3** for batch storage.

### 2. Processing Layer
- **Spark Structured Streaming**: Joins GPS and Weather streams, applies cleaning and transformation in real-time.
- **Spark Batch**: Processes historical data for deep analysis and modeling.
- **dbt**: Transforms and models data into a **Star Schema** for analytics-ready consumption.

### 3. Storage Layer
- **Amazon S3** as a Data Lake for raw and processed data.
- **PostgreSQL / Snowflake** as a Data Warehouse for curated, analytics-ready datasets.

### 4. Analytics & Machine Learning
- ML models for:
  - **ETA Prediction** (Estimated Time of Arrival).
  - **Demand Forecasting** for different regions and times.
- Libraries: `scikit-learn`, `pyspark.ml`.

### 5. Visualization Layer
- **Streamlit**: Real-time interactive dashboard with live maps and metrics.
- **Power BI / Tableau**: Historical analysis, performance KPIs, and predictive analytics.

---

## Expected Outputs

### Real-Time Dashboard
- Live vehicle positions on an interactive map.
- Weather conditions affecting ride performance.
- KPIs: active trips, average ETA, average speed, etc.

### Historical Analytics Dashboard
- Trip demand trends over days/weeks.
- Peak traffic and demand times.
- Correlation between weather and trip duration.

### Machine Learning Predictions
- Predict ETA for any ongoing trip.
- Forecast demand in different areas for upcoming time slots.

---

##  what we do 
- Combines **Streaming & Batch** data processing.
- Integrates with **Cloud Services** (AWS S3, possibly Snowflake).
- Includes **Machine Learning** for predictive insights.
- Features clear, interactive **visualization dashboards**.
- Mimics real-world ride-hailing analytics infrastructure.

---
