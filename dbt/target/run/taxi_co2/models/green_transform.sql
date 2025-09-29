
  
    
    

    create  table
      "emissions"."main"."green_transform__dbt_tmp"
  
    as (
      

SELECT
    g.trip_distance,
    g.lpep_pickup_datetime,
    g.lpep_dropoff_datetime,
    g.passenger_count,

    g.trip_distance * e.co2_grams_per_mile / 1000 as trip_co2_kgs,
    g.trip_distance / EPOCH(g.lpep_dropoff_datetime - g.lpep_pickup_datetime) / 3600.0 as avg_mph,
    extract(hour from g.lpep_pickup_datetime) as hour_of_day,
    extract(dow from g.lpep_pickup_datetime) as day_of_week,         
    extract(week from g.lpep_pickup_datetime) as week_of_year,       
    extract(month from g.lpep_pickup_datetime) as month_of_year
    


FROM
    "emissions"."main"."green_taxis" g
LEFT JOIN
    "emissions"."main"."vehicle_emissions" e
ON e.vehicle_type = 'green_taxi'
    );
  
  