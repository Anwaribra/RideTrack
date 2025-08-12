

-- Pickup-Dropoff Heatmap Data

SELECT 
    dl.latitude as pickup_lat,
    dl.longitude as pickup_lon,
    COUNT(*) as trip_density,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance
FROM fact_trips ft
JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
WHERE dl.latitude IS NOT NULL 
    AND dl.longitude IS NOT NULL
GROUP BY dl.latitude, dl.longitude
HAVING trip_density >= 10
ORDER BY trip_density DESC;

-- Zone-to-Zone Flow Analysis
-- For creating flow maps showing trip patterns
SELECT 
    pickup.zone as pickup_zone,
    pickup.latitude as pickup_lat,
    pickup.longitude as pickup_lon,
    dropoff.zone as dropoff_zone,
    dropoff.latitude as dropoff_lat,
    dropoff.longitude as dropoff_lon,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance
FROM fact_trips ft
JOIN dim_location pickup ON ft.pickup_location_key = pickup.location_key
JOIN dim_location dropoff ON ft.dropoff_location_key = dropoff.location_key
WHERE pickup.latitude IS NOT NULL 
    AND pickup.longitude IS NOT NULL
    AND dropoff.latitude IS NOT NULL 
    AND dropoff.longitude IS NOT NULL
GROUP BY 
    pickup.zone, pickup.latitude, pickup.longitude,
    dropoff.zone, dropoff.latitude, dropoff.longitude
HAVING trip_count >= 50
ORDER BY trip_count DESC;





-- Weekly Patterns Analysis
SELECT 
    dt.day_of_week,
    dt.hour_of_day,
    COUNT(*) as trips,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dt.day_of_week, dt.hour_of_day
ORDER BY dt.day_of_week, dt.hour_of_day;







-- Zone Performance Analysis
SELECT 
    dl.zone,
    dl.city,
    COUNT(*) as total_trips,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration,
    COUNT(DISTINCT dt.day) as active_days,
    ROUND(COUNT(*) / COUNT(DISTINCT dt.day), 2) as trips_per_day,
    ROUND(SUM(ft.total_amount) / COUNT(DISTINCT dt.day), 2) as revenue_per_day
FROM fact_trips ft
JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dl.zone, dl.city
HAVING total_trips >= 100
ORDER BY revenue_per_day DESC;

-- Peak Period Performance
WITH peak_hours AS (
    SELECT 
        dt.hour_of_day,
        COUNT(*) as trips
    FROM fact_trips ft
    JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
    GROUP BY dt.hour_of_day
    ORDER BY trips DESC
    LIMIT 5
)
SELECT 
    dt.hour_of_day,
    dl.zone,
    dl.city,
    COUNT(*) as trip_count,
    ROUND(AVG(ft.total_amount), 2) as avg_fare,
    ROUND(AVG(ft.trip_distance), 2) as avg_distance,
    ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration
FROM fact_trips ft
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
WHERE dt.hour_of_day IN (SELECT hour_of_day FROM peak_hours)
GROUP BY dt.hour_of_day, dl.zone, dl.city
HAVING trip_count >= 50
ORDER BY dt.hour_of_day, trip_count DESC;

--  Payment Analysis
SELECT 
    dp.payment_description,
    dl.zone,
    COUNT(*) as transactions,
    ROUND(AVG(ft.total_amount), 2) as avg_amount,
    ROUND(AVG(ft.tip_amount), 2) as avg_tip,
    ROUND(COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as tip_frequency,
    ROUND(AVG(CASE WHEN dt.is_weekend THEN ft.total_amount END), 2) as weekend_avg,
    ROUND(AVG(CASE WHEN NOT dt.is_weekend THEN ft.total_amount END), 2) as weekday_avg
FROM fact_trips ft
JOIN dim_payment dp ON ft.payment_key = dp.payment_key
JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
GROUP BY dp.payment_description, dl.zone
HAVING transactions >= 50
ORDER BY transactions DESC;
