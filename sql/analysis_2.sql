-- 2. Find the top 3 cities with the highest average humidity in each state.
WITH grouped_data AS (

SELECT 
  city,
  state,
  AVG(data.humidity) AS AVG_HUMIDITY
FROM `hnesman-challenge.hn_etl.hn_locations_weather`,
UNNEST (data) as data
WHERE
DATE BETWEEN '2024-01-01'AND '2024-01-07'
GROUP BY city, state
),

ranked_data AS (
SELECT 
  city,
  state,
  AVG_HUMIDITY,
  RANK() OVER (PARTITION BY state order by AVG_HUMIDITY DESC) as rank_humid
FROM grouped_data
)

SELECT 
  city,
  state,
  AVG_HUMIDITY,
  rank_humid
FROM ranked_data
WHERE rank_humid <= 3
ORDER BY state, rank_humid DESC


