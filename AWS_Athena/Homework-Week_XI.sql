-- SQL for AWS Athena at Cubix Data engineer course
-- Created by Vizsy, István on May 28th, 2024
-- 
------------------------------------------------------------------------------
-- Task 1
SELECT c.company, ROUND(SUM(tt.trip_total), 1) total_fare
FROM "AwsDataCatalog"."chicago_taxi_db"."taxi_trips" tt
INNER JOIN "AwsDataCatalog"."chicago_taxi_db"."company" c
    ON tt.company_id = c.company_id
GROUP BY c.company
ORDER BY total_fare DESC
LIMIT 10;

------------------------------------------------------------------------------
-- Task 2
SELECT ca.community_name pickup_community, COUNT(tt.trip_id) count_of_rides
FROM "AwsDataCatalog"."chicago_taxi_db"."taxi_trips" tt
INNER JOIN "AwsDataCatalog"."chicago_taxi_db"."community_areas" ca
    ON tt.pickup_community_area_id = ca.area_code
GROUP BY ca.community_name
ORDER BY count_of_rides DESC
LIMIT 10;

------------------------------------------------------------------------------
-- Task 3
WITH rpd as -- rides per day
    (SELECT DATE(trip_start_timestamp) date1, COUNT(trip_id) count_of_rides
        FROM "AwsDataCatalog"."chicago_taxi_db"."taxi_trips"
        GROUP BY DATE(trip_start_timestamp)),
    drw as -- average daily rides per weekdays
    (SELECT d.day_of_week, ROUND(AVG(rpd.count_of_rides), 1) daily_rides
        FROM rpd
        INNER JOIN "AwsDataCatalog"."chicago_taxi_db".date d ON rpd.date1 = d.date
        GROUP BY d.day_of_week
        ORDER BY d.day_of_week)
SELECT 
    CASE 
        WHEN drw.day_of_week = 1 THEN 'Monday'
        WHEN drw.day_of_week = 2 THEN 'Tuesday'
        WHEN drw.day_of_week = 3 THEN 'Wednesday'
        WHEN drw.day_of_week = 4 THEN 'Thursday'
        WHEN drw.day_of_week = 5 THEN 'Friday'
        WHEN drw.day_of_week = 6 THEN 'Saturday'
        WHEN drw.day_of_week = 7 THEN 'Sunday'
        ELSE 'Invalid workday'
    END,
    drw.daily_rides
    FROM drw;
