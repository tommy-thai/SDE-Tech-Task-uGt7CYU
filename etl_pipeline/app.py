import requests
from datetime import datetime, timedelta
import json
import os
import pandas as pd

from bq_utils import BigQueryIO
from etl_pipeline.utils import log


class OPEN_WEATHER_API:
    def __init__(self):
        self.api_prefix = "https://api.openweathermap.org/data/3.0/onecall/timemachine?"
        self.api_key = os.getenv("OPEN_WEATHER_KEY")
        self.locations = json.load(open("etl_pipeline/locations_complete.json"))

    def fetch_data(self,lat,lon, dt):
        url = f"{self.api_prefix}lat={lat}&lon={lon}&dt={dt}&appid={self.api_key}"
        try:
            response = requests.get(url)
        except Exception as e:
            log(f"Error fetching data: {e}")
            return None
        return response
    
    def run(self,start_date: str=None) -> pd.DataFrame:
        '''
        start_date: str. Date in the format YYYY-MM-DD. If None, it will fetch data for yesterday.
        '''
        if start_date:
            # Convert the string to a datetime object and then to Unix time
            unix_start_date = convert_dates(start_date)
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            unix_start_date = int((datetime.now() - timedelta(days=1)).timestamp())

            start_date = (datetime.now() - timedelta(days=1)).date()

        locations_weather = []
        for location in self.locations:
            lat = location["lat"]
            lon = location["lon"]
            try:
                response = self.fetch_data(lat, lon, dt=unix_start_date)    
            except Exception as e:
                log(f"Error fetching data for {location['city']}: {e}")
                continue
            # Check if the response is successful
            if response.status_code == 200:
                response = json.loads(response.text)
                # Extract the relevant data from the response
                data = response["data"]
                # Adding the date to the data
                location["date"] = start_date
                # Adding the location to the data
                location["data"] = data
                locations_weather.append(location)
                log(f"Data fetched successfully for {location['city']}")
        
            else:
                log(f"Failed to fetch data for {location['city']}: {response.status_code}")

        return pd.DataFrame(locations_weather)

def convert_dates(date:str) -> int:
    # Convert the string to a datetime object and then to Unix time
    if isinstance(date, str):
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        unix_time = date_obj.timestamp()
        return int(unix_time)
    else:
        log(f"Date format is not string: {date}", "error")
        return None

def main(start_date=None) -> None:
    """
    Main function to execute the ETL pipeline.
    start_date: str. Date in the format YYYY-MM-DD. If None, it will fetch data for yesterday.
    """
    weather_client = OPEN_WEATHER_API()
    data = weather_client.run(start_date)

    bq_client = BigQueryIO(
        table_name="hn_locations_weather",
        dataset="hn_etl",
        project_id=os.getenv("PROJECT_ID"),
    )

    bq_client.load_pandas_dataframe(data)



if __name__ == "__main__":
    main()
    # range_dates = pd.date_range(start='2024-01-01', periods=7, freq='D')

    # for date in range_dates:
    #     date = date.strftime("%Y-%m-%d")
    #     print(date)
    #     main(start_date=date)

