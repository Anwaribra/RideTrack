-- DIMENSION TABLES


-- Time Dimension
CREATE TABLE IF NOT EXISTS dim_time (
    time_key Int64,
    datetime DateTime64(6),
    hour Int32,
    day Int32,
    month Int32,
    year Int32,
    day_of_week Int32,
    is_weekend Bool,
    hour_of_day Int32,
    time_bucket String 
) ENGINE = MergeTree()
ORDER BY time_key;

-- Location Dimension
CREATE TABLE IF NOT EXISTS dim_location (
    location_key Int32,
    latitude Decimal(9, 6),
    longitude Decimal(9, 6),
    city String,
    zone String
) ENGINE = MergeTree()
ORDER BY location_key;

-- Vehicle Dimension
CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_id String,
    vehicle_type String,
    last_seen DateTime,
    first_seen DateTime
) ENGINE = MergeTree()
ORDER BY vehicle_id;

-- Payment Dimension
CREATE TABLE IF NOT EXISTS dim_payment (
    payment_key Int64,
    payment_type Int64,
    payment_description String
) ENGINE = MergeTree()
ORDER BY payment_key;

-- Rate Dimension
CREATE TABLE IF NOT EXISTS dim_rate (
    rate_key Int64,
    rate_code_id Int64,
    rate_description String
) ENGINE = MergeTree()
ORDER BY rate_key;

-- Weather Dimension
CREATE TABLE IF NOT EXISTS dim_weather (
    weather_key Int32,
    weather_condition String,
    temperature_range String, 
    humidity_range String     
) ENGINE = MergeTree()
ORDER BY weather_key;


-- FACT TABLES


-- Fact Trips Table (Main business transactions)
CREATE TABLE IF NOT EXISTS fact_trips (
    trip_key Int64,
    pickup_time_key Int64,
    dropoff_time_key Int64,
    pickup_location_key Int64,
    dropoff_location_key Int64,
    payment_key Int64,
    rate_key Int64,
    vendor_id Int32,
    passenger_count Int64,
    trip_distance Float64,
    fare_amount Float64,
    extra Float64,
    mta_tax Float64,
    tip_amount Float64,
    tolls_amount Float64,
    total_amount Float64,
    trip_duration_minutes Float64,
    congestion_surcharge Float64,
    store_and_fwd_flag String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(pickup_time_key)
ORDER BY (pickup_time_key, trip_key);

-- Fact Vehicle Movement Table (GPS tracking data)
CREATE TABLE IF NOT EXISTS fact_vehicle_movement (
    movement_key Int64,
    vehicle_id String,
    time_key Int32,
    location_key Int32,
    weather_key Int32,
    speed_kmh Decimal(5, 2),
    event_timestamp DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_key)
ORDER BY (time_key, vehicle_id);

-- Fact Weather Table (Weather measurements)
CREATE TABLE IF NOT EXISTS fact_weather (
    weather_key Int64,
    time_key Int64,
    weather_dim_key Int64,
    temperature_c Float64,
    humidity Int32,
    city String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_key)
ORDER BY time_key;

-- Fact Weather Measurements Table (Detailed weather metrics)
CREATE TABLE IF NOT EXISTS fact_weather_measurements (
    measurement_key Int64,
    time_key Int32,
    location_key Int32,
    weather_key Int32,
    temperature_c Decimal(4, 1),
    humidity Int32,
    event_timestamp DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_key)
ORDER BY time_key;


