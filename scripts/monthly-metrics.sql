SELECT
    DATE_FORMAT(pickup_date, '%Y-%m') AS month,
    AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN 1 ELSE 0 END) AS sat_mean_trip_count,
    AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN fare_amount END) AS sat_mean_fare_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN TIMESTAMPDIFF('SECOND', pickup_datetime, dropoff_datetime) END) AS sat_mean_duration_per_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN 1 ELSE 0 END) AS sun_mean_trip_count,
    AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN fare_amount END) AS sun_mean_fare_trip,
    AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN TIMESTAMPDIFF('SECOND', pickup_datetime, dropoff_datetime) END) AS sun_mean_duration_per_trip
FROM
    tripdata
WHERE
    pickup_date BETWEEN '2014-01-01' AND '2016-12-31'
GROUP BY
    DATE_FORMAT(pickup_date, '%Y-%m')
ORDER BY
    month;