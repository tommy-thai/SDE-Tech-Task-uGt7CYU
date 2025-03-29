# **ETL**

The module **app.main()** executes the pipeline in the following order:

1-Extracts the data from Open Weather API. It uses the file 'locations_complete.json' to extract lat/lon.
    1.1-'locations_complete.json' can be created using **geocode_api.py**
    
  **NOTE:** I got confused with the assignment, and used the Geocode API to extract the lat/long. I'm aware that I made a mistake in this step.

2-Parses the data

3-Loads the data into BigQuery in the table `hnesman-challenge.hn_etl.hn_locations_weather`



# Deliverables Description
##  1.Python ETL

I decided to deploy this ETL as a Cloud Run Job, due: 1-It can be easily scheduled to run daily.
2-This ETL has the risk that if it's rerun multiple times on the same day, it would generate duplicated data. Ideally the data should be added into a 'temp_table' that is merged with a 'permanent_table'. Then data in the 'temp_table' has to be deleted.

### Missing steps
**1**-Create a Dockerfile that:  
  1.1-setups the environment

  1.2-install the dependencies

  1.3-copies the folder /etl_pipeline into the container

  1.4-runs the ENTRYPOINT

  1.5-runs the ENTRYPOINT

**2**-Create a .github/workflows.yaml file

2.1-it's triggered when 'push to main or feature/**' 

2.2-Set's up the environment variables, for deployment to GCP. I have to the Service_Accounts_Keys as a Secret in GitHub Actions.

2.3-Runs 'tests' (didn't include Tests as I have very little experience)

2.4-Authenticates with GCP

2.5-Builds and Push the image to Artifact Registry

2.6-Deploy the image to a Cloud Run Job, -it has to pass a the Open Weather API Key as a Secret when deploying, hence the Open Weather API Key should be uploaded to Secret Manager -

2.7-Creates a trigger to execute the Job daily


## 2.**Unit Tests**  
Didn't answer this one as my experience with testing is very limited.
##  3.**SQL** 

### I created the BQ table for the historic data using the module  **bq_utils.py**

**table_name:**`hnesman-challenge.hn_etl.hn_locations_weather`

#### I used the schema below:
```
    schema = [
        SchemaField("city", "STRING", mode="NULLABLE"),
        SchemaField("state", "STRING", mode="NULLABLE"),
        SchemaField("country", "STRING", mode="NULLABLE"),
        SchemaField("lat", "FLOAT", mode="NULLABLE"),
        SchemaField("lon", "FLOAT", mode="NULLABLE"),
        SchemaField("date", "DATE", mode="REQUIRED"),
        SchemaField(
            "data", 
            "RECORD", 
            mode="REPEATED",  # Because it's an array of structs
            fields=[
                SchemaField("dt", "INTEGER", mode="NULLABLE"),
                SchemaField("sunrise", "INTEGER", mode="NULLABLE"),
                SchemaField("sunset", "INTEGER", mode="NULLABLE"),
                SchemaField("temp", "FLOAT", mode="NULLABLE"),
                SchemaField("feels_like", "FLOAT", mode="NULLABLE"),
                SchemaField("pressure", "INTEGER", mode="NULLABLE"),
                SchemaField("humidity", "INTEGER", mode="NULLABLE"),
                SchemaField("dew_point", "FLOAT", mode="NULLABLE"),
                SchemaField("uvi", "FLOAT", mode="NULLABLE"),
                SchemaField("clouds", "INTEGER", mode="NULLABLE"),
                SchemaField("visibility", "INTEGER", mode="NULLABLE"),
                SchemaField("wind_speed", "FLOAT", mode="NULLABLE"),
                SchemaField("wind_deg", "INTEGER", mode="NULLABLE"),
                SchemaField("wind_gust", "FLOAT", mode="NULLABLE"),
                SchemaField(
                    "weather", 
                    "RECORD", 
                    mode="REPEATED",  # Because it's an array
                    fields=[
                        SchemaField("id", "INTEGER", mode="NULLABLE"),
                        SchemaField("main", "STRING", mode="NULLABLE"),
                        SchemaField("description", "STRING", mode="NULLABLE"),
                        SchemaField("icon", "STRING", mode="NULLABLE"),
                    ],
                ),
            ],
        ),
    ]



```

To create the table I used the module **bq_utils.py**

```

project = os.getenv("PROJECT_ID")
dataset = 'hn_etl'
table = 'hn_locations_weather'

client = BigQueryIO(table, dataset, project)
client.create_table(schema)
```

##  4.**SQL Answers**

### 1. Provide the average daily temperature for each city in each state.

**NOTES:**

**a.** The average temperature is in Kelvin


**a.** I fetched the temperature 
```
WITH filtered_data AS (

SELECT 
  city,
  state,
  data
FROM `hnesman-challenge.hn_etl.hn_locations_weather`
 -- I could declare 'DATE' as a variable, and pass it as a parameter
WHERE DATE BETWEEN '2024-01-01'AND '2024-01-07'
)
SELECT 
  city,
  state,
  AVG(data.temp) as AVG_TEMP_KELVIN
FROM filtered_data,
UNNEST (data) as data
GROUP BY city, state
```
### 2. Find the top 3 cities with the highest average humidity in each state.

**NOTES:**

**a.** Taking the top 3 might seem trivial since there are at most two 'cities' per 'state'. However, if we were to extend the query to cover the entire country, it would become more relevant.

```
WITH filtered_data AS (

SELECT 
  city,
  state,
  data
  FROM `hnesman-challenge.hn_etl.hn_locations_weather`
  -- I could declare 'DATE' as a variable, and pass it as a parameter
  WHERE DATE BETWEEN '2024-01-01'AND '2024-01-07'
),
grouped_data AS (
  SELECT 
    city,
    state,
    AVG(data.humidity) AS AVG_HUMIDITY
  FROM filtered_data,
  UNNEST (data) as data
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
```

### 3. Find the percentage of cities in each state experiencing "rain" as the weather condition.

**NOTES:**

**a.**Have to add a filter for a one single day, to make sense of this calculation

```

-- 3. Find the percentage of cities in each state experiencing "rain" as the weather condition.

WITH daily_data AS (
  SELECT 
    city,
    state,
    data,
  FROM `hnesman-challenge.hn_etl.hn_locations_weather`

  WHERE DATE = "2024-01-03" -- I could declare 'DATE' as a variable, and pass it as a parameter
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
```






