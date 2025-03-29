
-- 3. Find the percentage of cities in each state experiencing "rain" as the weather condition.

WITH daily_data AS (
  SELECT 
    city,
    state,
    data,
  FROM `hnesman-challenge.hn_etl.hn_locations_weather`
  -- Have to add a filter for a one single day, to make sense of this calculation
  WHERE DATE = "2024-01-03" -- I could declare this as a variable, and pass it as a parameter
), 
cat_data AS (
  SELECT
      city,
      state,
      CASE WHEN weather.main = 'Rain' THEN 1
      ELSE 0
      END AS rain_condition
  FROM daily_data,
    UNNEST (data) as data,
    UNNEST (data.weather) as weather
)
SELECT 
  ROUND(SUM(rain_condition)/COUNT(*) * 100) as perc_rain_cities  
FROM cat_data