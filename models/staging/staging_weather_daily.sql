WITH daily_raw AS (
    SELECT
            airport_code,
            station_id,
            JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
    FROM {{source('weather_data', 'weather_daily_raw')}}
 --           FROM weather_daily_raw
),
daily_flattened AS (
    SELECT  airport_code,
            station_id,
            (json_data->>'date')::DATE AS date,
            (json_data->>'tavg')::NUMERIC AS avg_temp_c,
            (json_data->>'tmin')::NUMERIC AS min_temp_c,
            (json_data->>'tmax')::NUMERIC AS max_temp_c,
            (json_data->>'prcp')::NUMERIC AS precipitation_mm,
            ((json_data->>'snow')::NUMERIC)::INTEGER AS max_snow_mm,
            ((json_data->>'wdir')::NUMERIC)::INTEGER AS avg_wind_direction,
            (json_data->>'wspd')::NUMERIC AS avg_wind_speed_kmh,
            (json_data->>'wpgt')::NUMERIC AS wind_peakgust_kmh,
            (json_data->>'pres')::NUMERIC AS avg_pressure_hpa,
            (json_data->>'tsun')::INTEGER AS sun_minutes
    FROM daily_raw
)
SELECT * 
FROM daily_flattened;



WITH hourly_data AS (
  SELECT * 
-- FROM {{ref('staging_weather_hourly')}}
  FROM staging_weather_hourly
),
add_features AS (
  SELECT *
    , timestamp::DATE AS date -- only date (year-month-day) as DATE data type
    , timestamp::TIME AS time -- only time (hours:minutes:seconds) as TIME data type
    , TO_CHAR(timestamp,'HH24:MI') as hour -- time (hours:minutes) as TEXT data type
    , TO_CHAR(timestamp, 'FMmonth') AS month_name -- month name as a TEXT
    , TO_CHAR(timestamp, 'FMday') AS weekday    -- weekday name as TEXT      
    , DATE_PART('day', timestamp) AS date_day
    , DATE_PART('month', timestamp) AS date_month
    , DATE_PART('year', timestamp) AS date_year
    , DATE_PART('week', timestamp) AS cw
  FROM hourly_data
),
add_more_features AS (
  SELECT *
    ,(CASE 
        WHEN time BETWEEN '00:00:00' AND '05:59:00' THEN 'night'
        WHEN time BETWEEN '06:00:00' AND '18:00:00' THEN 'day'
        WHEN time BETWEEN '18:00:00' AND '23:59:00' THEN 'evening'
    END) AS day_part
  FROM add_features
)
SELECT *
FROM add_more_features


