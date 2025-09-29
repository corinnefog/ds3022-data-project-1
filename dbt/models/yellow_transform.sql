{{ config(
    materialized='table', 
    )
}}

SELECT
    t.trip_distance,
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    t.passenger_count,

    t.trip_distance * e.co2_grams_per_mile / 1000 as trip_co2_kgs,
    t.trip_distance / EPOCH(t.tpep_dropoff_datetime - t.tpep_pickup_datetime) / 3600.0 as avg_mph,
    extract(hour from t.tpep_pickup_datetime) as hour_of_day,
    extract(dow from t.tpep_pickup_datetime) as day_of_week,         
    extract(week from t.tpep_pickup_datetime) as week_of_year,       
    extract(month from t.tpep_pickup_datetime) as month_of_year
    


FROM
    {{ source('taxi_data', 'yellow_taxis') }} t
LEFT JOIN
    {{ source('taxi_data', 'vehicle_emissions') }} e
ON e.vehicle_type = 'yellow_taxi'