-- POPULATE TIME DIMENSION

INSERT INTO dim_time
SELECT DISTINCT
    toUnixTimestamp(datetime) as time_key,
    toHour(datetime) as hour,
    toDayOfMonth(datetime) as day,
    toMonth(datetime) as month,
    toYear(datetime) as year,
    toDayOfWeek(datetime) as day_of_week,
    toDayOfWeek(datetime) IN (6, 7) as is_weekend,
    toHour(datetime) as hour_of_day
FROM (
    SELECT lpep_pickup_datetime as datetime FROM green_tripdata WHERE lpep_pickup_datetime IS NOT NULL
    UNION ALL
    SELECT lpep_dropoff_datetime as datetime FROM green_tripdata WHERE lpep_dropoff_datetime IS NOT NULL
    UNION ALL
    SELECT timestamp as datetime FROM gps_data_v2 WHERE timestamp IS NOT NULL
    UNION ALL
    SELECT timestamp as datetime FROM weather_data_v2 WHERE timestamp IS NOT NULL
)
WHERE datetime IS NOT NULL;


-- POPULATE PAYMENT DIMENSION


INSERT INTO dim_payment
SELECT DISTINCT
    payment_type as payment_key,
    payment_type,
    CASE payment_type
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Other'
    END as payment_description
FROM green_tripdata
WHERE payment_type IS NOT NULL;


-- POPULATE RATE DIMENSION


INSERT INTO dim_rate
SELECT DISTINCT
    RatecodeID as rate_key,
    RatecodeID,
    CASE RatecodeID
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        ELSE 'Unknown'
    END as rate_description
FROM green_tripdata
WHERE RatecodeID IS NOT NULL;


-- POPULATE LOCATION DIMENSION


INSERT INTO dim_location (location_key, latitude, longitude, city, zone)
SELECT DISTINCT
    location_id as location_key,
    0 as latitude,  -- Will be updated with actual coordinates if available
    0 as longitude, -- Will be updated with actual coordinates if available
    'New York' as city,
    CASE 
        WHEN location_id IN (1, 132, 138) THEN 'Airport'
        WHEN location_id IN (4, 12, 13, 24, 41, 42, 43, 45, 48, 50, 68, 74, 75, 79, 87, 88, 90, 100, 103, 104, 105, 107, 113, 114, 116, 120, 125, 127, 128, 140, 141, 142, 143, 144, 148, 151, 152, 153, 158, 161, 162, 163, 164, 166, 170, 186, 194, 202, 209, 211, 224, 229, 230, 231, 232, 233, 234, 236, 237, 238, 239, 243, 244, 246, 249, 261, 262, 263) THEN 'Manhattan'
        WHEN location_id BETWEEN 1 AND 100 THEN 'Brooklyn'
        WHEN location_id BETWEEN 101 AND 200 THEN 'Queens'
        WHEN location_id BETWEEN 201 AND 300 THEN 'Bronx'
        ELSE 'Other'
    END as zone
FROM (
    SELECT PULocationID as location_id FROM green_tripdata WHERE PULocationID IS NOT NULL
    UNION ALL
    SELECT DOLocationID as location_id FROM green_tripdata WHERE DOLocationID IS NOT NULL
)
WHERE location_id > 0;


-- POPULATE VEHICLE DIMENSION


INSERT INTO dim_vehicle
SELECT 
    vehicle_id,
    'gps_tracker' as vehicle_type,
    max(timestamp) as last_seen,
    min(timestamp) as first_seen
FROM gps_data_v2
GROUP BY vehicle_id;




-- POPULATE WEATHER DIMENSION


INSERT INTO dim_weather
SELECT DISTINCT
    row_number() OVER () as weather_key,
    weather_condition,
    CASE 
        WHEN temperature_c < 0 THEN 'freezing'
        WHEN temperature_c < 10 THEN 'cold'
        WHEN temperature_c < 20 THEN 'mild'
        WHEN temperature_c < 30 THEN 'warm'
        ELSE 'hot'
    END as temperature_range,
    CASE 
        WHEN humidity < 40 THEN 'low'
        WHEN humidity < 70 THEN 'medium'
        ELSE 'high'
    END as humidity_range
FROM weather_data_v2
WHERE weather_condition IS NOT NULL;





-- POPULATE FACT TABLES--


-- Populate fact_trips (if not already populated)
INSERT INTO fact_trips
SELECT 
    row_number() OVER () as trip_key,
    toUnixTimestamp(lpep_pickup_datetime) as pickup_time_key,
    toUnixTimestamp(lpep_dropoff_datetime) as dropoff_time_key,
    PULocationID as pickup_location_key,
    DOLocationID as dropoff_location_key,
    payment_type as payment_key,
    RatecodeID as rate_key,
    VendorID as vendor_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    total_amount,
    dateDiff('minute', lpep_pickup_datetime, lpep_dropoff_datetime) as trip_duration_minutes,
    congestion_surcharge,
    store_and_fwd_flag
FROM green_tripdata
WHERE lpep_pickup_datetime IS NOT NULL 
  AND lpep_dropoff_datetime IS NOT NULL;

-- Populate fact_vehicle_movement (if not already populated)
INSERT INTO fact_vehicle_movement 
(movement_key, vehicle_id, time_key, location_key, weather_key, speed_kmh, event_timestamp)
SELECT 
    row_number() OVER () as movement_key,
    gps.vehicle_id,
    toInt32(toUnixTimestamp(gps.timestamp)) as time_key,
    1 as location_key,  -- Default location, update based on GPS coordinates if needed
    1 as weather_key,   -- Default weather, can be joined with weather data
    gps.speed_kmh,
    gps.timestamp as event_timestamp
FROM gps_data_v2 gps
WHERE gps.timestamp IS NOT NULL;

-- Populate fact_weather (if not already populated)
INSERT INTO fact_weather
SELECT 
    row_number() OVER () as weather_key,
    toUnixTimestamp(w.timestamp) as time_key,
    dw.weather_key as weather_dim_key,
    w.temperature_c,
    w.humidity,
    w.city
FROM weather_data_v2 w
CROSS JOIN dim_weather dw
WHERE w.timestamp IS NOT NULL
LIMIT 1000;  -- Limit to avoid too many combinations

-- Populate fact_weather_measurements (if not already populated)
INSERT INTO fact_weather_measurements 
(measurement_key, time_key, location_key, weather_key, temperature_c, humidity, event_timestamp)
SELECT 
    row_number() OVER () as measurement_key,
    toInt32(toUnixTimestamp(timestamp)) as time_key,
    1 as location_key,
    1 as weather_key,
    toDecimal64(temperature_c, 1) as temperature_c,
    toInt32(humidity) as humidity,
    timestamp as event_timestamp
FROM weather_data_v2
WHERE timestamp IS NOT NULL;

------------------------
-- VERIFICATION QUERIES


-- Check dimension table populations
SELECT 'dim_time' as table_name, COUNT(*) as record_count FROM dim_time
UNION ALL
SELECT 'dim_location' as table_name, COUNT(*) as record_count FROM dim_location
UNION ALL
SELECT 'dim_vehicle' as table_name, COUNT(*) as record_count FROM dim_vehicle
UNION ALL
SELECT 'dim_payment' as table_name, COUNT(*) as record_count FROM dim_payment
UNION ALL
SELECT 'dim_rate' as table_name, COUNT(*) as record_count FROM dim_rate
UNION ALL
SELECT 'dim_weather' as table_name, COUNT(*) as record_count FROM dim_weather
UNION ALL
SELECT 'fact_trips' as table_name, COUNT(*) as record_count FROM fact_trips
UNION ALL
SELECT 'fact_vehicle_movement' as table_name, COUNT(*) as record_count FROM fact_vehicle_movement
UNION ALL
SELECT 'fact_weather' as table_name, COUNT(*) as record_count FROM fact_weather
UNION ALL
SELECT 'fact_weather_measurements' as table_name, COUNT(*) as record_count FROM fact_weather_measurements
ORDER BY record_count DESC;
