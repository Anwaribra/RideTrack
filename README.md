# RideTrack360

## Overview
RideTrack360 is an intelligent real-time analytics platform designed for ride-hailing and fleet management services in New York City. The system continuously ingests and processes live GPS tracking data from vehicles, real-time weather conditions, and NYC taxi trip information to deliver instant insights and operational intelligence. Built with cutting-edge streaming technologies, RideTrack360 empowers businesses with live fleet monitoring, demand forecasting, and revenue optimization capabilities.

## Architecture
![Pipeline Architecture](data_ingestion/doc/PipelineArchitectur.jpg) 


## Key Features
-  **Real-time Vehicle Tracking** - Live GPS monitoring of 50+ vehicles with sub-second updates
- **Weather Intelligence** - Automated weather data integration for demand correlation analysis  
- **Live Analytics Dashboard** - Instant visibility into fleet performance and business metrics
- **Revenue Optimization** - Real-time fare analysis and pricing intelligence
- **Demand Forecasting** - Predictive analytics for trip demand patterns
- **High-Performance Processing** - Handles millions of data points with Kafka and Spark streaming
- **Cloud-Native Architecture** - Scalable infrastructure using AWS S3 and ClickHouse




## Data Model

The data warehouse follows a star schema with:
- **Fact Tables**: trips, vehicle_movement, weather
- **Dimension Tables**: time, location, vehicle, payment, rate, weather
- **Views**: Trip Analysis, Vehicle & Operational, Weather & Environmental,Real-time & Monitoring Views


### Data Warehouse Schema

```mermaid
erDiagram
    %% Dimension Tables
    dim_time {
        Int64 time_key PK
        DateTime64 datetime
        Int32 hour
        Int32 day
        Int32 month
        Int32 year
        Int32 day_of_week
        Bool is_weekend
        Int32 hour_of_day
        String time_bucket
    }
    
    dim_location {
        Int32 location_key PK
        Decimal latitude
        Decimal longitude
        String city
        String zone
    }
    
    dim_vehicle {
        String vehicle_id PK
        String vehicle_type
        DateTime last_seen
        DateTime first_seen
    }
    
    dim_payment {
        Int64 payment_key PK
        Int64 payment_type
        String payment_description
    }
    
    dim_rate {
        Int64 rate_key PK
        Int64 rate_code_id
        String rate_description
    }
    
    dim_weather {
        Int32 weather_key PK
        String weather_condition
        String temperature_range
        String humidity_range
    }
    
    %% Fact Tables
    fact_trips {
        Int64 trip_key PK
        Int64 pickup_time_key FK
        Int64 dropoff_time_key FK
        Int64 pickup_location_key FK
        Int64 dropoff_location_key FK
        Int64 payment_key FK
        Int64 rate_key FK
        Int32 vendor_id
        Int64 passenger_count
        Float64 trip_distance
        Float64 fare_amount
        Float64 extra
        Float64 mta_tax
        Float64 tip_amount
        Float64 tolls_amount
        Float64 total_amount
        Float64 trip_duration_minutes
        Float64 congestion_surcharge
        String store_and_fwd_flag
    }
    
    fact_vehicle_movement {
        Int64 movement_key PK
        String vehicle_id FK
        Int32 time_key FK
        Int32 location_key FK
        Int32 weather_key FK
        Decimal speed_kmh
        DateTime event_timestamp
    }
    
    fact_weather {
        Int64 weather_key PK
        Int64 time_key FK
        Int64 weather_dim_key FK
        Float64 temperature_c
        Int32 humidity
        String city
    }
    
    fact_weather_measurements {
        Int64 measurement_key PK
        Int32 time_key FK
        Int32 location_key FK
        Int32 weather_key FK
        Decimal temperature_c
        Int32 humidity
        DateTime event_timestamp
    }
    
    %% Relationships
    fact_trips ||--o{ dim_time : "pickup_time"
    fact_trips ||--o{ dim_time : "dropoff_time"
    fact_trips ||--o{ dim_location : "pickup_location"
    fact_trips ||--o{ dim_location : "dropoff_location"
    fact_trips ||--o{ dim_payment : "payment_method"
    fact_trips ||--o{ dim_rate : "rate_code"
    
    fact_vehicle_movement ||--o{ dim_vehicle : "vehicle"
    fact_vehicle_movement ||--o{ dim_time : "time"
    fact_vehicle_movement ||--o{ dim_location : "location"
    fact_vehicle_movement ||--o{ dim_weather : "weather"
    
    fact_weather ||--o{ dim_time : "time"
    fact_weather ||--o{ dim_weather : "weather_condition"
    
    fact_weather_measurements ||--o{ dim_time : "time"
    fact_weather_measurements ||--o{ dim_location : "location"
    fact_weather_measurements ||--o{ dim_weather : "weather"
```






## Streamlit Dashboard

The Streamlit dashboard provides a powerful and interactive interface for exploring the NYC taxi data. It connects directly to the ClickHouse data warehouse, leveraging its high performance for real-time analytics. The dashboard is designed to offer various insights into business operations, revenue, trip patterns, location analytics, operational metrics, customer behavior, and predictive trip analytics.

### Dashboard Sections

The dashboard is organized into several key sections, accessible via a sidebar navigation menu:

*   **Business Overview**: Provides a high-level summary of key performance indicators (KPIs) such as total trips, total revenue, average fare, and average trip distance. It also includes visualizations for revenue and trip count by hour of day, and payment method distribution.

*   **Revenue Analysis**: Delves deeper into financial aspects, showcasing monthly revenue trends, average fare trends, detailed revenue breakdown by payment method, and comprehensive tip analysis including average tip amounts and tip frequency.

*   **Trip Patterns**: Focuses on the characteristics of trips, including distributions of trip distance and duration. It also provides insights into peak hours of operation based on trip count.

*   **Location Analytics**: Visualizes geographical insights, highlighting top pickup and dropoff locations, and identifying the most popular routes. This helps in understanding demand hotspots and traffic flows.

*   **Operational Metrics**: Offers insights into the efficiency of operations, including average speed analysis by hour, rate code distribution, and analysis of extra charges incurred during trips.

*   **Customer Insights**: Explores passenger behavior, such as trip distribution by passenger count, and analyzes tip frequency across different payment methods. It also includes a detailed zone performance analysis, showing trip volume versus average fare for various zones.

*   **Predictive Trip Analytics**: This advanced section provides a deeper dive into trip characteristics with interactive visualizations:
    *   **Trip Duration vs. Distance Analysis**: An interactive scatter plot showing the relationship between trip duration and distance, with total fare indicated by color and passenger count by size. Users can filter data by maximum distance, duration, and sample size.
    *   **Efficiency Analysis by Time Period**: A bar chart illustrating average speed by time period, colored by fare per mile, to pinpoint optimal operational times.
    *   **Distance vs. Duration Correlation**: Displays the correlation coefficient between trip distance and duration, complemented by a trend line plot for visual analysis of their relationship.
    *   **Fare Analysis by Trip Characteristics**: A heatmap that visualizes average fares across different distance and duration categories, providing insights into pricing strategies and their effectiveness.

Each section is designed to be intuitive and interactive, allowing users to gain actionable insights from the vast amounts of taxi trip data.





Use the interactive charts and filters provided within each 



## SQL Queries

All the SQL queries used to power this Streamlit dashboard are consolidated in the `all_sql_queries.sql` file. This file is organized by dashboard section, making it easy to understand the data retrieval logic for each visualization. You can find queries for:

*   Business Overview
*   Revenue Analysis
*   Trip Patterns
*   Location Analytics
*   Operational Metrics
*   Customer Insights
*   Predictive Trip Analytics
*   Database Exploration (for understanding schema and data quality)

These queries are optimized for ClickHouse performance and demonstrate various analytical patterns, including aggregations, joins, and conditional logic.







