import json
import time
import os
import logging

from geopy.geocoders import Nominatim

CACHE_FILE = "src/data/lat_lon_cache.json"

def city_state_to_latlon(city: str, state: str) -> tuple:
    logging.info(f"city_state_to_latlon(city = {city}, state = {state})")
    location = None

    # if cache file exists
    if os.path.exists(CACHE_FILE):

        # open cache file and load with json lib
        with open(CACHE_FILE, "r") as cache_file:
            cache = json.load(cache_file)

            # if cache contains query fetch from cache
            if f"{city}, state" in cache:
                return cache[f"{city}, state"]

            else:
                logging.info("Query not found in cache")

    else:
        logging.info("Cache file does not exist")

    geolocator = Nominatim(user_agent = "rent_vs_buy")
    
    location = geolocator.geocode(f"{city}, {state}")

    # sleep due to rate limited api
    time.sleep(1)

    if location:
        return location.latitude, location.longitude
    
    # read in cache if exists
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as cache_file:
            old_cache = json.load(cache_file)

    else: 
        old_cache = {}

    # update cache
    old_cache[f"{city}, {state}"] = location

    with open(CACHE_FILE, "w") as cache_file:
        json.dump(old_cache, cache_file, indent = 4)
    
    return None, None

def read_json(filepath = "src/output/sample_output.json"):
    logging.info(f"filepath = {filepath}")

    # open the json
    with open(filepath, "r") as file:
        data = json.load(file)

        return data
    
def update_data(data: dict) -> dict: 
    """
    This last step in processing data
        1. Tosses out any entries that we can't geocode
        2. Adds lat lon to entries that don't have it
        3. Offsets the locationof any repeats
        3. Overwrites file so we don't have to do it again 
    """
    logging.info(f"update_data(data)")

    # fills in lat lon for any unkown datum in renters
    renters = []
    for datum in data["renters"]:

        if "latitude" not in datum and "longitude" not in datum:
            lat, lon = city_state_to_latlon(datum["city"], datum["state"])

            if not lat or not lon:
                continue

            # save lat lon in entry
            datum["latitude"] = lat - 0.02
            datum["longitude"] = lon- 0.02
        
        renters.append(datum)
    data["renters"] = renters
    
    # fills in lat lon for any unkown datum in buyers
    buyers = []
    for datum in data["buyers"]:

        if "latitude" not in datum and "longitude" not in datum:
            lat, lon = city_state_to_latlon(datum["city"], datum["state"])

            if not lat or not lon:
                continue

            # save lat lon in entry
            datum["latitude"] = lat + 0.02
            datum["longitude"] = lon + 0.02

        buyers.append(datum)
    data["buyers"] = buyers

    return data


def get_data() -> dict:
    """
    Called by frontend js module to get data
    """
    logging.info(f"get_data()")

    # read in algo output and process
    data = read_json()
    data = update_data(data)

    return data

    
