-- NYC Taxi Analytics Dashboard - All SQL Queries


-- 1. BUSINESS OVERVIEW QUERIES


-- Summary Metrics Query
SELECT 
    'Total Trips' as metric,
    COUNT(*)::String as value
FROM fact_trips
UNION ALL
SELECT 
    'Total Revenue' as metric,
    '$' || toString(ROUND(SUM(total_amount), 2)) as value
FROM fact_trips
UNION ALL
SELECT 
    'Average Fare' as metric,
    '$' || toString(ROUND(AVG(fare_amount), 2)) as value
FROM fact_trips
UNION ALL
SELECT 
    'Average Trip Distance' as metric,
    toString(ROUND(AVG(trip_distance), 2)) || ' miles' as value
FROM fact_trips;

-- Hourly Revenue and Trip Count Query
SELECT 
    dt.hour_of_day,
    COUNT(*) as trips,
    ROUND(SUM(ft.total_amount), 2) as revenue,
    ROUND(AVG(ft.total_amount), 2) as avg_fare
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.hour_of_day
ORDER BY dt.hour_of_day;

-- Payment Method Distribution Query
SELECT 
    dp.payment_description,
    COUNT(*) as trips,
    ROUND(SUM(ft.total_amount), 2) as revenue
FROM fact_trips ft
JOIN dim_payment dp ON ft.payment_key = dp.payment_key
GROUP BY dp.payment_description
ORDER BY revenue DESC;

-- 2. REVENUE ANALYSIS QUERIES


-- Monthly Revenue Trends Query
SELECT 
    dt.year,
    dt.month,
    COUNT(*) as trips,
    ROUND(SUM(ft.total_amount), 2) as revenue,
    ROUND(AVG(ft.total_amount), 2) as avg_fare
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.year, dt.month
ORDER BY dt.year DESC, dt.month DESC
LIMIT 12;

-- Revenue by Payment Method Detailed Query
SELECT 
    dp.payment_description,
    COUNT(*) as trips,
    ROUND(SUM(ft.total_amount), 2) as revenue,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(SUM(ft.tip_amount), 2) as total_tips
FROM fact_trips ft
JOIN dim_payment dp ON ft.payment_key = dp.payment_key
GROUP BY dp.payment_description
ORDER BY revenue DESC;

-- Tip Analysis Query
SELECT 
    dp.payment_description,
    ROUND(AVG(ft.tip_amount), 2) as avg_tip,
    ROUND(AVG(ft.tip_amount / NULLIF(ft.fare_amount, 0) * 100), 2) as avg_tip_percentage,
    COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) as trips_with_tips,
    ROUND(COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as tip_frequency_percent
FROM fact_trips ft
JOIN dim_payment dp ON ft.payment_key = dp.payment_key
GROUP BY dp.payment_description
ORDER BY avg_tip_percentage DESC;

-- 3. TRIP PATTERNS QUERIES


-- Trip Distance Distribution Query
SELECT 
    CASE 
        WHEN ft.trip_distance <= 1 THEN '0-1 miles'
        WHEN ft.trip_distance <= 3 THEN '1-3 miles'
        WHEN ft.trip_distance <= 5 THEN '3-5 miles'
        WHEN ft.trip_distance <= 10 THEN '5-10 miles'
        ELSE '10+ miles'
    END as distance_bucket,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare
FROM fact_trips ft
WHERE ft.trip_distance > 0
GROUP BY distance_bucket
ORDER BY AVG(ft.trip_distance);

-- Trip Duration Distribution Query
SELECT 
    CASE 
        WHEN ft.trip_duration_minutes <= 5 THEN '0-5 min'
        WHEN ft.trip_duration_minutes <= 15 THEN '5-15 min'
        WHEN ft.trip_duration_minutes <= 30 THEN '15-30 min'
        WHEN ft.trip_duration_minutes <= 60 THEN '30-60 min'
        ELSE '60+ min'
    END as duration_bucket,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare
FROM fact_trips ft
WHERE ft.trip_duration_minutes > 0
GROUP BY duration_bucket
ORDER BY AVG(ft.trip_duration_minutes);

-- Peak Hours Analysis Query
SELECT 
    dt.hour_of_day,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.hour_of_day
ORDER BY dt.hour_of_day;

-- 4. LOCATION ANALYTICS QUERIES


-- Top Pickup Locations Query
SELECT 
    dl.zone,
    dl.city,
    COUNT(*) as pickup_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare
FROM fact_trips ft
JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
GROUP BY dl.zone, dl.city
ORDER BY pickup_count DESC
LIMIT 15;

-- Top Dropoff Locations Query
SELECT 
    dl.zone,
    dl.city,
    COUNT(*) as dropoff_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare
FROM fact_trips ft
JOIN dim_location dl ON ft.dropoff_location_key = dl.location_key
GROUP BY dl.zone, dl.city
ORDER BY dropoff_count DESC
LIMIT 15;

-- Most Popular Routes Query
SELECT 
    pickup.zone as pickup_zone,
    dropoff.zone as dropoff_zone,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance
FROM fact_trips ft
JOIN dim_location pickup ON ft.pickup_location_key = pickup.location_key
JOIN dim_location dropoff ON ft.dropoff_location_key = dropoff.location_key
GROUP BY pickup.zone, dropoff.zone
HAVING trip_count >= 100
ORDER BY trip_count DESC
LIMIT 20;

-- 5. OPERATIONAL METRICS QUERIES


-- Speed Analysis by Hour Query
SELECT 
    dt.hour_of_day as time_bucket,
    COUNT(*) as trips,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration,
    ROUND(AVG(ft.trip_distance / NULLIF(ft.trip_duration_minutes, 0) * 60), 1) as avg_speed_kmh
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
WHERE ft.trip_duration_minutes > 0 AND ft.trip_distance > 0
GROUP BY dt.hour_of_day
ORDER BY dt.hour_of_day;

-- Rate Code Distribution Query
SELECT 
    dr.rate_description,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(SUM(ft.total_amount), 2) as total_revenue
FROM fact_trips ft
JOIN dim_rate dr ON ft.rate_key = dr.rate_key
GROUP BY dr.rate_description
ORDER BY trip_count DESC;

-- Extra Charges Analysis Query
SELECT 
    dt.hour_of_day,
    COUNT(*) as trips,
    ROUND(AVG(ft.extra), 2) as avg_extra,
    ROUND(AVG(ft.congestion_surcharge), 2) as avg_congestion_surcharge,
    COUNT(CASE WHEN ft.extra > 0 THEN 1 END) as trips_with_extra
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.hour_of_day
ORDER BY avg_extra DESC
LIMIT 10;

-- 6. CUSTOMER INSIGHTS QUERIES


-- Passenger Count Distribution Query
SELECT 
    ft.passenger_count,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.tip_amount), 2) as avg_tip
FROM fact_trips ft
WHERE ft.passenger_count > 0 AND ft.passenger_count <= 6
GROUP BY ft.passenger_count
ORDER BY ft.passenger_count;

-- Tip Frequency by Payment Method Query
SELECT 
    dp.payment_description,
    COUNT(*) as trips,
    COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) as trips_with_tips,
    ROUND(COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as tip_frequency_percent
FROM fact_trips ft
JOIN dim_payment dp ON ft.payment_key = dp.payment_key
GROUP BY dp.payment_description
ORDER BY tip_frequency_percent DESC;

-- Zone Performance Analysis Query
SELECT 
    dl.zone,
    dl.city,
    COUNT(*) as total_trips,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(SUM(ft.total_amount), 2) as total_revenue
FROM fact_trips ft
JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
GROUP BY dl.zone, dl.city
HAVING total_trips >= 100
ORDER BY total_revenue DESC
LIMIT 20;

-- 7. PREDICTIVE TRIP ANALYTICS QUERIES
-- 

-- Trip Duration vs Distance Scatter Plot Query (Dynamic with filters)
SELECT 
    ft.trip_distance,
    ft.trip_duration_minutes,
    ft.fare_amount,
    ft.total_amount,
    ft.passenger_count,
    dp.payment_description,
    dt.hour_of_day,
    CASE 
        WHEN dt.hour_of_day BETWEEN 6 AND 11 THEN 'Morning'
        WHEN dt.hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN dt.hour_of_day BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Night'
    END as time_period
FROM fact_trips ft
JOIN dim_payment dp ON ft.payment_key = dp.payment_key
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
WHERE ft.trip_distance > 0 
    AND ft.trip_duration_minutes > 0 
    AND ft.trip_distance <= {max_distance}
    AND ft.trip_duration_minutes <= {max_duration}
    AND ft.fare_amount > 0
ORDER BY RAND()
LIMIT {sample_size};

-- Efficiency Analysis by Time Period Query
SELECT 
    CASE 
        WHEN dt.hour_of_day BETWEEN 6 AND 11 THEN 'Morning'
        WHEN dt.hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN dt.hour_of_day BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Night'
    END as time_period,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.trip_distance / NULLIF(ft.trip_duration_minutes, 0) * 60), 2) as avg_speed_mph,
    ROUND(AVG(ft.fare_amount / NULLIF(ft.trip_distance, 0)), 2) as fare_per_mile,
    ROUND(AVG(ft.fare_amount / NULLIF(ft.trip_duration_minutes, 0)), 2) as fare_per_minute
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
WHERE ft.trip_distance > 0 AND ft.trip_duration_minutes > 0
GROUP BY time_period
ORDER BY avg_speed_mph DESC;

-- Fare Analysis by Trip Characteristics Query
SELECT 
    CASE 
        WHEN ft.trip_distance <= 2 THEN 'Short (≤2 mi)'
        WHEN ft.trip_distance <= 5 THEN 'Medium (2-5 mi)'
        WHEN ft.trip_distance <= 10 THEN 'Long (5-10 mi)'
        ELSE 'Very Long (>10 mi)'
    END as distance_category,
    CASE 
        WHEN ft.trip_duration_minutes <= 10 THEN 'Quick (≤10 min)'
        WHEN ft.trip_duration_minutes <= 20 THEN 'Normal (10-20 min)'
        WHEN ft.trip_duration_minutes <= 40 THEN 'Slow (20-40 min)'
        ELSE 'Very Slow (>40 min)'
    END as duration_category,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(stddevPop(ft.total_amount), 2) as fare_std,
    ROUND(MIN(ft.total_amount), 2) as min_fare,
    ROUND(MAX(ft.total_amount), 2) as max_fare
FROM fact_trips ft
WHERE ft.trip_distance > 0 AND ft.trip_duration_minutes > 0 AND ft.total_amount > 0
GROUP BY distance_category, duration_category
HAVING trip_count >= 50
ORDER BY avg_fare DESC;

-- 8. DATABASE EXPLORATION QUERIES

-- Show all tables in the database
SHOW TABLES;

-- Describe fact_trips table structure
DESCRIBE fact_trips;

-- Sample data from fact_trips
SELECT * FROM fact_trips LIMIT 10;

-- Count total records in fact_trips
SELECT COUNT(*) as total_records FROM fact_trips;

-- Check data quality - null values
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN trip_distance IS NULL THEN 1 END) as null_distance,
    COUNT(CASE WHEN trip_duration_minutes IS NULL THEN 1 END) as null_duration,
    COUNT(CASE WHEN fare_amount IS NULL THEN 1 END) as null_fare,
    COUNT(CASE WHEN total_amount IS NULL THEN 1 END) as null_total
FROM fact_trips;

-- Data range analysis
SELECT 
    MIN(trip_distance) as min_distance,
    MAX(trip_distance) as max_distance,
    MIN(trip_duration_minutes) as min_duration,
    MAX(trip_duration_minutes) as max_duration,
    MIN(fare_amount) as min_fare,
    MAX(fare_amount) as max_fare
FROM fact_trips
WHERE trip_distance > 0 AND trip_duration_minutes > 0;

