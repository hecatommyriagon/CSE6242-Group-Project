import json
import time
import os
import logging

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable

CACHE_FILE = "src/data/lat_lon_cache.json"
MAX_TRIES_GEOCODING = 2

def city_state_to_latlon(city: str, state: str) -> tuple:
    logging.info(f"city_state_to_latlon(city = {city}, state = {state})")
    location = None

    # if cache file exists
    if os.path.exists(CACHE_FILE):

        # open cache file and load with json lib
        with open(CACHE_FILE, "r") as cache_file:
            cache = json.load(cache_file)

            # if cache contains query fetch from cache
            if f"{city}, {state}" in cache:
                logging.info("Query found in cache!")

                if cache[f"{city}, {state}"] == "NA":
                    return None, None
                
                return cache[f"{city}, {state}"]

            else:
                logging.info("Query not found in cache")

    else:
        logging.info("Cache file does not exist")

    geolocator = Nominatim(user_agent = "rent_vs_buy")
    
    # try at least 5 times 
    for i in range(MAX_TRIES_GEOCODING):
        try:
            location = geolocator.geocode(f"{city}, {state}")
            
        except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError) as e:
            logging.warning("Failed to geocode due to API failure")

            # sleep due to rate limited api
            time.sleep(2)

            if i == MAX_TRIES_GEOCODING - 1:
                logging.warning("Giving up on geocoding")
                location = "NA"
    
    # sleep due to rate limited api
    time.sleep(1.1)

    # read in cache if exists
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as cache_file:
            old_cache = json.load(cache_file)

    else: 
        logging.info("Will try to create new cache file")
        old_cache = {}

    # update cache
    if location == ("NA"):
        old_cache[f"{city}, {state}"] = "NA"
    
    elif location is not None:
        old_cache[f"{city}, {state}"] = location.latitude, location.longitude

    with open(CACHE_FILE, "w") as cache_file:
        logging.info("Cache file updated with new entry")
        json.dump(old_cache, cache_file, indent = 4)
    
    if location == ("NA"):
        return None, None
    elif location:
        return location.latitude, location.longitude
    
    logging.warning("Geocoding failed")
    return None, None

def read_json(filepath = "src/output/homelens_output.json"):
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

    
