-- 1. Provide the average daily temperature for each city in each state.
SELECT 
  city,
  state,
  AVG(data.temp) as AVG_TEMP_KELVIN
FROM `hnesman-challenge.hn_etl.hn_locations_weather`,
UNNEST (data) as data
WHERE
DATE BETWEEN '2024-01-01'AND '2024-01-07'
GROUP BY city, state
