-- TRIP ANALYSIS VIEWS 


-- Hourly Trip Summary
CREATE OR REPLACE VIEW v_hourly_trip_summary AS
SELECT 
    dt.hour_of_day,
    dt.is_weekend,
    COUNT(*) as trip_count,
    AVG(ft.fare_amount) as avg_fare,
    AVG(ft.trip_distance) as avg_distance,
    SUM(ft.total_amount) as total_revenue,
    AVG(ft.trip_duration_minutes) as avg_duration_minutes
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.hour_of_day, dt.is_weekend
ORDER BY dt.hour_of_day;

-- Daily Trip Metrics
CREATE OR REPLACE VIEW v_daily_trip_metrics AS
SELECT 
    toDate(makeDate(dt.year, dt.month, dt.day)) as date,
    COUNT(*) as daily_trips,
    SUM(ft.total_amount) as daily_revenue,
    AVG(ft.fare_amount) as avg_fare,
    AVG(ft.trip_distance) as avg_distance,
    AVG(ft.trip_duration_minutes) as avg_duration
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY date
ORDER BY date DESC;


-- PAYMENT ANALYSIS VIEWS

-- Payment Method Analysis
CREATE OR REPLACE VIEW v_payment_analysis AS
SELECT 
    dp.payment_description,
    COUNT(*) as trip_count,
    AVG(ft.fare_amount) as avg_fare,
    AVG(ft.tip_amount) as avg_tip,
    AVG(ft.tip_amount / NULLIF(ft.fare_amount, 0) * 100) as avg_tip_percentage,
    SUM(ft.total_amount) as total_revenue
FROM fact_trips ft
JOIN dim_payment dp ON ft.payment_key = dp.payment_key
GROUP BY dp.payment_description
ORDER BY trip_count DESC;


-- LOCATION ANALYSIS VIEWS


-- Location Performance Analysis
CREATE OR REPLACE VIEW v_location_analysis AS
SELECT 
    dl.zone,
    dl.city,
    COUNT(*) as trip_count,
    AVG(ft.fare_amount) as avg_fare,
    AVG(ft.trip_distance) as avg_distance,
    SUM(ft.total_amount) as total_revenue
FROM fact_trips ft
JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
GROUP BY dl.zone, dl.city
ORDER BY trip_count DESC;

-- Pickup vs Dropoff Location Analysis
CREATE OR REPLACE VIEW v_pickup_dropoff_analysis AS
SELECT 
    pickup.zone as pickup_zone,
    dropoff.zone as dropoff_zone,
    COUNT(*) as trip_count,
    AVG(ft.fare_amount) as avg_fare,
    AVG(ft.trip_distance) as avg_distance
FROM fact_trips ft
JOIN dim_location pickup ON ft.pickup_location_key = pickup.location_key
JOIN dim_location dropoff ON ft.dropoff_location_key = dropoff.location_key
GROUP BY pickup.zone, dropoff.zone
HAVING trip_count > 100
ORDER BY trip_count DESC;


-- VEHICLE ANALYSIS VIEWS


-- Vehicle Utilization Analysis
CREATE OR REPLACE VIEW v_vehicle_utilization AS
SELECT 
    fvm.vehicle_id,
    COUNT(*) as total_movements,
    AVG(fvm.speed_kmh) as avg_speed,
    MIN(fvm.event_timestamp) as first_seen,
    MAX(fvm.event_timestamp) as last_seen,
    COUNT(DISTINCT toDate(fvm.event_timestamp)) as active_days
FROM fact_vehicle_movement fvm
GROUP BY fvm.vehicle_id
ORDER BY total_movements DESC;

-- Vehicle Speed Analysis
CREATE OR REPLACE VIEW v_vehicle_speed_analysis AS
SELECT 
    dt.hour_of_day,
    COUNT(*) as movement_count,
    AVG(fvm.speed_kmh) as avg_speed,
    MIN(fvm.speed_kmh) as min_speed,
    MAX(fvm.speed_kmh) as max_speed,
    quantile(0.5)(fvm.speed_kmh) as median_speed
FROM fact_vehicle_movement fvm
JOIN dim_time dt ON fvm.time_key = dt.time_key
GROUP BY dt.hour_of_day
ORDER BY dt.hour_of_day;


-- WEATHER ANALYSIS VIEWS


-- Weather Impact on Trips
CREATE OR REPLACE VIEW v_weather_impact AS
SELECT 
    dw.weather_condition,
    dw.temperature_range,
    COUNT(DISTINCT ft.trip_key) as trip_count,
    AVG(ft.fare_amount) as avg_fare,
    AVG(ft.trip_distance) as avg_distance,
    COUNT(DISTINCT fvm.vehicle_id) as active_vehicles,
    AVG(fvm.speed_kmh) as avg_vehicle_speed
FROM dim_time dt
LEFT JOIN fact_trips ft ON ft.pickup_time_key = dt.time_key
LEFT JOIN fact_weather fw ON fw.time_key = dt.time_key
LEFT JOIN dim_weather dw ON fw.weather_dim_key = dw.weather_key
LEFT JOIN fact_vehicle_movement fvm ON fvm.time_key = dt.time_key
WHERE dw.weather_condition IS NOT NULL
GROUP BY dw.weather_condition, dw.temperature_range
ORDER BY trip_count DESC;

-- Weather Measurements Summary
CREATE OR REPLACE VIEW v_weather_summary AS
SELECT 
    toDate(fwm.event_timestamp) as date,
    AVG(fwm.temperature_c) as avg_temperature,
    MIN(fwm.temperature_c) as min_temperature,
    MAX(fwm.temperature_c) as max_temperature,
    AVG(fwm.humidity) as avg_humidity,
    COUNT(*) as measurement_count
FROM fact_weather_measurements fwm
GROUP BY toDate(fwm.event_timestamp)
ORDER BY date DESC;


-- BUSINESS INTELLIGENCE VIEWS


-- Revenue Analysis by Time Periods
CREATE OR REPLACE VIEW v_revenue_analysis AS
SELECT 
    dt.hour_of_day AS time_bucket,
    dt.is_weekend,
    COUNT(*) as trip_count,
    SUM(ft.total_amount) as total_revenue,
    AVG(ft.total_amount) as avg_revenue_per_trip,
    SUM(ft.tip_amount) as total_tips,
    AVG(ft.tip_amount / NULLIF(ft.fare_amount, 0) * 100) as avg_tip_percentage
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.hour_of_day, dt.is_weekend
ORDER BY total_revenue DESC;



-- Performance Dashboard View

CREATE OR REPLACE VIEW v_performance_dashboard AS
SELECT 
    'trips' as metric_type,
    'Total Trips' as metric_name,
    COUNT(*)::String as metric_value,
    'count' as metric_unit
FROM fact_trips
UNION ALL
SELECT 
    'revenue' as metric_type,
    'Total Revenue' as metric_name,
    toString(ROUND(SUM(total_amount), 2)) as metric_value,
    'USD' as metric_unit
FROM fact_trips
UNION ALL
SELECT 
    'vehicles' as metric_type,
    'Active Vehicles' as metric_name,
    COUNT(DISTINCT vehicle_id)::String as metric_value,
    'count' as metric_unit
FROM fact_vehicle_movement
UNION ALL
SELECT 
    'locations' as metric_type,
    'Active Locations' as metric_name,
    COUNT(*)::String as metric_value,
    'count' as metric_unit
FROM dim_location;


-- OPERATIONAL VIEWS


-- Real-time Activity View (last 24 hours)
CREATE OR REPLACE VIEW v_realtime_activity AS
SELECT 
    dt.hour_of_day,
    COUNT(ft.trip_key) as current_trips,
    COUNT(DISTINCT fvm.vehicle_id) as active_vehicles,
    AVG(fw.temperature_c) as avg_temperature,
    any(dw.weather_condition) as weather_condition
FROM dim_time dt
LEFT JOIN fact_trips ft ON ft.pickup_time_key = dt.time_key 
    AND toDate(fromUnixTimestamp(dt.time_key)) >= today() - 1
LEFT JOIN fact_vehicle_movement fvm ON fvm.time_key = dt.time_key 
    AND toDate(fromUnixTimestamp(dt.time_key)) >= today() - 1
LEFT JOIN fact_weather fw ON fw.time_key = dt.time_key 
    AND toDate(fromUnixTimestamp(dt.time_key)) >= today() - 1
LEFT JOIN dim_weather dw ON fw.weather_dim_key = dw.weather_key
GROUP BY dt.hour_of_day
ORDER BY dt.hour_of_day;

-- Data Quality Summary View
CREATE OR REPLACE VIEW v_data_quality_summary AS
SELECT 
    'fact_trips' as table_name,
    COUNT(*) as total_records,
    COUNT(*) - COUNT(pickup_time_key) as null_pickup_time,
    COUNT(*) - COUNT(dropoff_time_key) as null_dropoff_time,
    SUM(CASE WHEN trip_duration_minutes < 0 THEN 1 ELSE 0 END) as negative_duration,
    SUM(CASE WHEN trip_distance = 0 AND fare_amount > 0 THEN 1 ELSE 0 END) as zero_distance_positive_fare
FROM fact_trips
UNION ALL
SELECT 
    'fact_vehicle_movement' as table_name,
    COUNT(*) as total_records,
    COUNT(*) - COUNT(vehicle_id) as null_vehicle_id,
    COUNT(*) - COUNT(time_key) as null_time_key,
    0 as negative_duration,
    0 as zero_distance_positive_fare
FROM fact_vehicle_movement
UNION ALL
SELECT 
    'fact_weather' as table_name,
    COUNT(*) as total_records,
    COUNT(*) - COUNT(time_key) as null_time_key,
    COUNT(*) - COUNT(temperature_c) as null_temperature,
    0 as negative_duration,
    0 as zero_distance_positive_fare
FROM fact_weather;
