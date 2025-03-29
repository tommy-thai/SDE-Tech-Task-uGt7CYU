import json
import requests
import re

'''
This is an aux module to add the Lat/Lon codes to the Locations list
'''

class CODE_LOCATOR:
    def __init__(self):
        self.API_PREFIX = "http://api.openweathermap.org/geo/1.0/direct?"
        self.API_KEY = json.load(open("etl_pipeline/open_weather.json"))["API_KEY"]
        self.LOCATIONS = json.load(open("etl_pipeline/locations.json"))


    def add_missing_info(self) -> list[dict]:
        """ 
        This function adds missing information to the locations list by fetching
        latitude and longitude from the OpenWeatherMap API.
        """
        locations_list = []
        for location in self.LOCATIONS:
            city = location["city"]
            state = location["state"]
            country = location["country"]
            try:
                lat, lon, state = self._fetch_lat_lon(city, state, country)
                location["lat"] = lat
                location["lon"] = lon
                location["state"] = state
                locations_list.append(location)
            except Exception as e:
                print(f"Error fetching data for {city}, {state}, {country}: {e}")
                continue

        return locations_list

    def _fetch_lat_lon(self, city: str, state: str, country:str) -> tuple[float, float, str]:
        if state == None:
            state = ""
        url = f"{self.API_PREFIX}q={city},{state},{country}&appid={self.API_KEY}"
        print(url)

        response = requests.get(url)
        response = response.text
        response = json.loads(response)

        try:
            lat = response[0]["lat"]
            lon = response[0]["lon"]
            state = response[0]["state"]
            return lat, lon, state
        except KeyError as e:
            print(e)


def main(): 
    '''
    Main function to execute the code locator and fetch missing information.
    Creates a JSON file with the updated locations.
    '''

    client = CODE_LOCATOR()
    locations = client.add_missing_info()

    # Convert the list to a JSON string
    json_string = json.dumps(locations, indent=4)

    # Write the JSON string to a file
    with open("etl_pipeline/locations_complete.json", "w") as file:
        file.write(json_string)


if __name__ == "__main__":
    main()


    # city = 'Portland'
    # state = ''
    # country = 'US'
    # response = client.fetch_lat_lon(city, state, country)
    # lat, lon = client.fetch_lat_lon(city, state, country)
    # locations = client.add_missing_info()
    # print(locations)
