WITH parsed_data AS (
  -- Extracts 'city' and 'state_code' from the field state, and 'lat'/'lon'
  SELECT 
  name,
  SPLIT(name,',')[OFFSET(0)] AS city,
  TRIM(SPLIT(name,',')[OFFSET(1)]) AS state_code,
  internal_point_lat AS lat,
  internal_point_lon AS lon,
  FROM `bigquery-public-data.geo_us_boundaries.urban_areas`
),


states AS (
  -- Fetch the unique list of state name and state code. Using as filter the list of states from the assignment

SELECT 
  DISTINCT 
  state as state_code, 
  state_name
 FROM `bigquery-public-data.geo_us_boundaries.states` 
 WHERE state_name in ("Michigan", "North Dakota", "South Dakota", "Minnesota", "Nebraska", "Montana")
),

cities AS (
-- Filters the list of requested cities, and uses the 'state_code' field to bring the 'state_name'
-- I'll use this to identify those repeated cities names that don't belong to the requested states
-- Consider there are some 'cities' with null state code, these ones remain in the query
  SELECT 
    city,
    parsed_data.state_code,
    states.state_name,
    lat,
    lon
  FROM parsed_data
  LEFT JOIN states
  ON parsed_data.state_code = states.state_code
  WHERE  city in ( "Sioux Falls",
    "Great Falls",
    "Houghton",
    "Fargo", 
    "Duluth", 
    "Bismarck",
    "Aberdeen",
    "Grand Island",
    "Glasgow",
    "Omaha", 
    "Portland"
    ) 
    
), 

unique_cities AS (
  -- I used the WINDOW FUNCTION to identify the cities that were requested
  -- Using the ORDER BY 'state_name' DESC, I get the matched state_names at the top of list
  SELECT 
    city,
    state_code,
    state_name,
    lon,
    lat,
    RANK() OVER (PARTITION BY city ORDER BY state_name DESC) as count
  FROM cities

)
-- I use the field 'count' to keep the requested cities for the task
SELECT 
    city,
    state_code,
    state_name,
    lon,
    lat, 
FROM unique_cities
WHERE count = 1
ORDER BY city
