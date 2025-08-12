



-- BUSINESS SUMMARY METRICS


-- Overall Business Summary
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
FROM fact_trips
UNION ALL
SELECT 
    'Average Trip Duration' as metric,
    toString(ROUND(AVG(trip_duration_minutes), 1)) || ' minutes' as value
FROM fact_trips
WHERE trip_duration_minutes > 0;



























-- REVENUE ANALYSIS


-- Revenue by Hour of Day
SELECT 
    dt.hour_of_day,
    COUNT(*) as trips,
    ROUND(SUM(ft.total_amount), 2) as revenue,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(SUM(ft.tip_amount), 2) as tips
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.hour_of_day
ORDER BY revenue DESC;

-- Weekend vs Weekday Revenue
SELECT 
    CASE WHEN dt.is_weekend THEN 'Weekend' ELSE 'Weekday' END as period_type,
    COUNT(*) as trips,
    ROUND(SUM(ft.total_amount), 2) as total_revenue,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(SUM(ft.tip_amount), 2) as total_tips
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.is_weekend
ORDER BY total_revenue DESC;

-- Revenue by Payment Method
SELECT 
    dp.payment_description,
    COUNT(*) as trips,
    ROUND(SUM(ft.total_amount), 2) as revenue,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(AVG(ft.tip_amount / NULLIF(ft.fare_amount, 0) * 100), 2) as avg_tip_percentage,
    ROUND(SUM(ft.tip_amount), 2) as total_tips
FROM fact_trips ft
JOIN dim_payment dp ON ft.payment_key = dp.payment_key
GROUP BY dp.payment_description
ORDER BY revenue DESC;




-- TRIP DEMAND ANALYSIS


-- Peak Hours Analysis
SELECT 
    dt.hour_of_day,
    dt.hour AS time_bucket,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration_minutes
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.hour_of_day, dt.hour
ORDER BY trip_count DESC;

-- Monthly Trip Trends
SELECT 
    dt.year,
    dt.month,
    COUNT(*) as trips,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(SUM(ft.total_amount), 2) as revenue
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.year, dt.month
ORDER BY dt.year DESC, dt.month DESC;

-- Trip Distance Distribution
SELECT 
    CASE 
        WHEN ft.trip_distance <= 1 THEN '0-1 miles'
        WHEN ft.trip_distance <= 3 THEN '1-3 miles'
        WHEN ft.trip_distance <= 5 THEN '3-5 miles'
        WHEN ft.trip_distance <= 10 THEN '5-10 miles'
        ELSE '10+ miles'
    END as distance_bucket,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.tip_amount), 2) as avg_tip,
    ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration
FROM fact_trips ft
WHERE ft.trip_distance > 0
GROUP BY distance_bucket
ORDER BY AVG(ft.trip_distance);



-- LOCATION ANALYSIS


-- Top Pickup Locations
SELECT 
    dl.zone,
    dl.city,
    COUNT(*) as pickup_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance
FROM fact_trips ft
JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
GROUP BY dl.zone, dl.city
ORDER BY pickup_count DESC
LIMIT 20;

-- Top Dropoff Locations
SELECT 
    dl.zone,
    dl.city,
    COUNT(*) as dropoff_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance
FROM fact_trips ft
JOIN dim_location dl ON ft.dropoff_location_key = dl.location_key
GROUP BY dl.zone, dl.city
ORDER BY dropoff_count DESC
LIMIT 20;

-- Popular Routes (Pickup to Dropoff)
SELECT 
    pickup.zone as pickup_zone,
    dropoff.zone as dropoff_zone,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration
FROM fact_trips ft
JOIN dim_location pickup ON ft.pickup_location_key = pickup.location_key
JOIN dim_location dropoff ON ft.dropoff_location_key = dropoff.location_key
GROUP BY pickup.zone, dropoff.zone
HAVING trip_count >= 100
ORDER BY trip_count DESC
LIMIT 20;


-- OPERATIONAL PERFORMANCE


-- Average Trip Metrics by Time Period
SELECT 
    dt.hour_of_day as time_bucket,
    COUNT(*) as trips,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance / NULLIF(ft.trip_duration_minutes, 0) * 60), 1) as avg_speed_kmh
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
WHERE ft.trip_duration_minutes > 0 AND ft.trip_distance > 0
GROUP BY dt.hour_of_day
ORDER BY avg_speed_kmh DESC;

-- Trip Duration Analysis
SELECT 
    CASE 
        WHEN ft.trip_duration_minutes <= 5 THEN '0-5 min'
        WHEN ft.trip_duration_minutes <= 15 THEN '5-15 min'
        WHEN ft.trip_duration_minutes <= 30 THEN '15-30 min'
        WHEN ft.trip_duration_minutes <= 60 THEN '30-60 min'
        ELSE '60+ min'
    END as duration_bucket,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance
FROM fact_trips ft
WHERE ft.trip_duration_minutes > 0
GROUP BY duration_bucket
ORDER BY AVG(ft.trip_duration_minutes);


-- CUSTOMER BEHAVIOR ANALYSIS


-- Passenger Count Distribution
SELECT 
    ft.passenger_count,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.tip_amount), 2) as avg_tip,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance
FROM fact_trips ft
WHERE ft.passenger_count > 0 AND ft.passenger_count <= 6
GROUP BY ft.passenger_count
ORDER BY ft.passenger_count;

-- Tip Analysis by Payment Method
SELECT 
    dp.payment_description,
    COUNT(*) as trips,
    ROUND(AVG(ft.tip_amount), 2) as avg_tip,
    ROUND(AVG(ft.tip_amount / NULLIF(ft.fare_amount, 0) * 100), 2) as avg_tip_percentage,
    COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) as trips_with_tips,
    ROUND(COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as tip_frequency_percent
FROM fact_trips ft
JOIN dim_payment dp ON ft.payment_key = dp.payment_key
GROUP BY dp.payment_description
ORDER BY avg_tip_percentage DESC;





-- RATE CODE ANALYSIS


-- Trip Distribution by Rate Code
SELECT 
    dr.rate_description,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.fare_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration,
    ROUND(SUM(ft.total_amount), 2) as total_revenue
FROM fact_trips ft
JOIN dim_rate dr ON ft.rate_key = dr.rate_key
GROUP BY dr.rate_description
ORDER BY trip_count DESC;




-- SURGE PRICING ANALYSIS

-- Extra Charges Analysis
SELECT 
    dt.hour_of_day,
    COUNT(*) as trips,
    ROUND(AVG(ft.extra), 2) as avg_extra,
    ROUND(AVG(ft.congestion_surcharge), 2) as avg_congestion_surcharge,
    COUNT(CASE WHEN ft.extra > 0 THEN 1 END) as trips_with_extra,
    COUNT(CASE WHEN ft.congestion_surcharge > 0 THEN 1 END) as trips_with_congestion
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.hour_of_day
ORDER BY avg_extra DESC;

