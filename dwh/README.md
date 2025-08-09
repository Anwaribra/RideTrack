# RideTrack360 Data Warehouse


##  Data Model

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

###  Data Flow Architecture

```mermaid
graph TD
    A[Raw Data Sources] --> B[Staging Layer]
    B --> C[Data Warehouse]
    C --> D[Analytics Layer]
    
    subgraph "Raw Data Sources"
        A1[green_tripdata<br/>NYC Taxi Data]
        A2[gps_data_v2<br/>Vehicle GPS]
        A3[weather_data_v2<br/>Weather API]
    end
    
    subgraph "Data Warehouse Tables"
        C1[Dimension Tables<br/>- dim_time<br/>- dim_location<br/>- dim_vehicle<br/>- dim_payment<br/>- dim_rate<br/>- dim_weather]
        C2[Fact Tables<br/>- fact_trips<br/>- fact_vehicle_movement<br/>- fact_weather<br/>- fact_weather_measurements]
    end
    
    subgraph "Analytics Layer"
        D1[Analytical Views<br/>- v_hourly_trip_summary<br/>- v_payment_analysis<br/>- v_location_analysis<br/>- v_vehicle_utilization]
        D2[Business Intelligence<br/>- Revenue Analysis<br/>- Demand Patterns<br/>- Performance Metrics]
        D3[Dashboards<br/>- Executive KPIs<br/>- Operational Monitoring<br/>- Real-time Metrics]
    end
    
    A1 --> B
    A2 --> B
    A3 --> B
    B --> C1
    B --> C2
    C1 --> D1
    C2 --> D1
    D1 --> D2
    D1 --> D3
```


