# app.py

import requests
from datetime import datetime, timedelta
import json
# import os

# from bq_utils import 


API_URL_PREFIX = 'https://history.openweathermap.org/data/2.5/history/city?' \
'lat={lat}&lon={lon}&type=hour&start={start}&end={end}&appid={API key}'



START = ''
END = ''

class CODE_LOCATOR:
    def __init__(self):
        self.API_PREFIX = "https://history.openweathermap.org/data/2.5/history/city?"
        self.API_KEY = json.load(open("etl_pipeline/open_weather.json"))["API_KEY"]
        self.LOCATIONS = json.load(open("etl_pipeline/locations_complete.json"))


    def fetch_data(self,lat,lon, start, end):
        url = f"{self.API_PREFIX}lat={lat}&lon={lon}&type=hour&start={start}&end={end}"
        
        response = requests.get(url)
        return response
    
def convert_dates(date:str=None):
    # Get the current datetime
    if not date:
        date = datetime.now().date()

    if isinstance(date, datetime):
        date_obj = date
    else:
        # Convert the string to a datetime object
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        
    # Convert it to Unix time (seconds since the epoch)
    unix_time = date_obj.timestamp()
    return int(unix_time)

def write_data_to_bq():
    pass



if __name__ == "__main__":
    # date_string = "2024-03-28 12:30:00"
    date_string = "2024-03-28"
    # Convert the string to a datetime object
    # date_obj = datetime.strptime(date_string, "%Y-%m-%d")

# Convert to Unix time
    # unix_time = convert_dates(date_string)
    unix_time = convert_dates()
    unix_time = int(unix_time)
    client = CODE_LOCATOR()
    client.fetch_data()
