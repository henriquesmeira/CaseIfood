-- ============================================================================
-- iFood Case - Data Architect
-- Business Questions SQL Analysis
-- ============================================================================

-- Initial configuration
USE CATALOG main;
USE SCHEMA nyc_taxi;

-- ============================================================================
-- QUESTION 1: Average total amount for yellow taxis per month
-- ============================================================================

-- Detailed answer by month
SELECT 
    'QUESTION 1 - MONTHLY BREAKDOWN' as analysis,
    year_month,
    ROUND(AVG(avg_total_amount), 2) as avg_total_amount,
    SUM(total_trips) as total_trips,
    ROUND(SUM(sum_total_amount), 2) as total_revenue_month
FROM main.nyc_taxi.gold_trips
WHERE taxi_type = 'yellow'
GROUP BY year_month
ORDER BY year_month;

-- Main answer - Overall average
SELECT 
    'QUESTION 1 - MAIN ANSWER' as analysis,
    ROUND(
        SUM(sum_total_amount) / SUM(total_trips), 2
    ) as avg_total_amount_yellow_taxis,
    SUM(total_trips) as total_yellow_trips,
    ROUND(SUM(sum_total_amount), 2) as total_yellow_revenue
FROM main.nyc_taxi.gold_trips
WHERE taxi_type = 'yellow';

-- Complementary analysis - Monthly comparison
SELECT 
    'QUESTION 1 - MONTHLY COMPARISON' as analysis,
    year_month,
    ROUND(AVG(avg_total_amount), 2) as avg_amount,
    SUM(total_trips) as trips,
    ROUND(
        (SUM(total_trips) * 100.0) / 
        (SELECT SUM(total_trips) FROM main.nyc_taxi.gold_trips WHERE taxi_type = 'yellow'), 
        2
    ) as percentage_trips_month
FROM main.nyc_taxi.gold_trips
WHERE taxi_type = 'yellow'
GROUP BY year_month
ORDER BY year_month;

-- ============================================================================
-- QUESTION 2: Average passengers per hour in May for all taxis
-- ============================================================================

-- Main answer - Average per hour in May
SELECT 
    'QUESTION 2 - MAIN ANSWER' as analysis,
    pickup_hour as hour,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as avg_passengers_per_trip,
    SUM(total_trips) as total_trips,
    SUM(sum_passenger_count) as total_passengers
FROM main.nyc_taxi.gold_trips
WHERE pickup_month = 5 
  AND pickup_year = 2023
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- Peak analysis
SELECT 
    'QUESTION 2 - PEAK ANALYSIS' as analysis,
    pickup_hour as hour,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as avg_passengers,
    SUM(total_trips) as trips,
    CASE 
        WHEN pickup_hour BETWEEN 7 AND 9 THEN 'Morning Peak'
        WHEN pickup_hour BETWEEN 17 AND 19 THEN 'Evening Peak'
        WHEN pickup_hour BETWEEN 22 AND 23 THEN 'Night Peak'
        ELSE 'Normal'
    END as demand_period
FROM main.nyc_taxi.gold_trips
WHERE pickup_month = 5 
  AND pickup_year = 2023
GROUP BY pickup_hour
ORDER BY avg_passengers DESC;

-- Analysis by time period in May
SELECT 
    'QUESTION 2 - BY TIME PERIOD' as analysis,
    pickup_period as period,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as avg_passengers,
    SUM(total_trips) as total_trips,
    ROUND(
        (SUM(total_trips) * 100.0) / 
        (SELECT SUM(total_trips) FROM main.nyc_taxi.gold_trips 
         WHERE pickup_month = 5 AND pickup_year = 2023), 
        2
    ) as percentage_trips
FROM main.nyc_taxi.gold_trips
WHERE pickup_month = 5 
  AND pickup_year = 2023
GROUP BY pickup_period
ORDER BY avg_passengers DESC;

-- ============================================================================
-- COMPLEMENTARY ANALYSIS AND INSIGHTS
-- ============================================================================

-- Yellow vs Green taxi comparison
SELECT 
    'COMPLEMENTARY - YELLOW vs GREEN' as analysis,
    taxi_type,
    ROUND(
        SUM(sum_total_amount) / SUM(total_trips), 2
    ) as avg_fare,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as avg_passengers,
    SUM(total_trips) as total_trips,
    ROUND(SUM(sum_total_amount), 2) as total_revenue
FROM main.nyc_taxi.gold_trips
GROUP BY taxi_type
ORDER BY total_trips DESC;

-- Monthly evolution
SELECT 
    'COMPLEMENTARY - MONTHLY EVOLUTION' as analysis,
    year_month,
    SUM(total_trips) as monthly_trips,
    ROUND(
        SUM(sum_total_amount) / SUM(total_trips), 2
    ) as avg_fare_month,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as avg_passengers_month,
    ROUND(SUM(sum_total_amount), 2) as monthly_revenue
FROM main.nyc_taxi.gold_trips
GROUP BY year_month
ORDER BY year_month;

-- Top 5 busiest hours
SELECT 
    'COMPLEMENTARY - TOP HOURS' as analysis,
    pickup_hour as hour,
    SUM(total_trips) as total_trips,
    ROUND(
        SUM(sum_total_amount) / SUM(total_trips), 2
    ) as avg_fare,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as avg_passengers
FROM main.nyc_taxi.gold_trips
GROUP BY pickup_hour
ORDER BY total_trips DESC
LIMIT 5;

-- Weekend vs weekday analysis
SELECT 
    'COMPLEMENTARY - WEEKEND ANALYSIS' as analysis,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    SUM(total_trips) as trips,
    ROUND(
        SUM(sum_total_amount) / SUM(total_trips), 2
    ) as avg_fare,
    ROUND(
        SUM(sum_passenger_count) / SUM(total_trips), 2
    ) as avg_passengers,
    ROUND(
        (SUM(total_trips) * 100.0) / 
        (SELECT SUM(total_trips) FROM main.nyc_taxi.gold_trips), 
        2
    ) as percentage_trips
FROM main.nyc_taxi.gold_trips
GROUP BY is_weekend
ORDER BY trips DESC;

-- ============================================================================
-- DATA VALIDATION
-- ============================================================================

-- Data quality verification
SELECT 
    'VALIDATION - DATA QUALITY' as analysis,
    COUNT(*) as total_gold_records,
    COUNT(DISTINCT taxi_type) as taxi_types,
    COUNT(DISTINCT year_month) as available_months,
    MIN(pickup_year) as min_year,
    MAX(pickup_year) as max_year,
    SUM(total_trips) as total_aggregated_trips
FROM main.nyc_taxi.gold_trips;

-- Temporal coverage verification
SELECT 
    'VALIDATION - TEMPORAL COVERAGE' as analysis,
    taxi_type,
    year_month,
    SUM(total_trips) as trips,
    COUNT(DISTINCT pickup_hour) as hours_covered
FROM main.nyc_taxi.gold_trips
GROUP BY taxi_type, year_month
ORDER BY taxi_type, year_month;

-- ============================================================================
-- EXECUTIVE SUMMARY
-- ============================================================================

-- Key metrics for presentation
SELECT 
    'EXECUTIVE SUMMARY' as analysis,
    'Metric' as indicator,
    'Value' as result
UNION ALL
SELECT 
    'EXECUTIVE SUMMARY',
    'Total Trips',
    FORMAT_NUMBER(SUM(total_trips), 0)
FROM main.nyc_taxi.gold_trips
UNION ALL
SELECT 
    'EXECUTIVE SUMMARY',
    'Total Revenue',
    CONCAT('$', FORMAT_NUMBER(SUM(sum_total_amount), 2))
FROM main.nyc_taxi.gold_trips
UNION ALL
SELECT 
    'EXECUTIVE SUMMARY',
    'Yellow Taxi Average (Answer 1)',
    CONCAT('$', FORMAT_NUMBER(
        SUM(CASE WHEN taxi_type = 'yellow' THEN sum_total_amount ELSE 0 END) / 
        SUM(CASE WHEN taxi_type = 'yellow' THEN total_trips ELSE 0 END), 2
    ))
FROM main.nyc_taxi.gold_trips
UNION ALL
SELECT 
    'EXECUTIVE SUMMARY',
    'May Passengers Average (Answer 2)',
    FORMAT_NUMBER(
        SUM(CASE WHEN pickup_month = 5 THEN sum_passenger_count ELSE 0 END) / 
        SUM(CASE WHEN pickup_month = 5 THEN total_trips ELSE 0 END), 2
    )
FROM main.nyc_taxi.gold_trips;

-- ============================================================================
-- END OF QUERIES
-- ============================================================================
