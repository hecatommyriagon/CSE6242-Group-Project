import csv
import json
import os
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable

MISSED_CSV = "src/data/missed_cities.csv"
CACHE_FILE = "src/data/lat_lon_cache_GENERATED.json"
MAX_GEOCODE_TRIES = 5


def geocode_city_state(city, state):
    geolocator = Nominatim(user_agent="rent_vs_buy_cache_gen")
    delay = 1.0
    for attempt in range(MAX_GEOCODE_TRIES):
        try:
            location = geolocator.geocode(f"{city}, {state}")
            if location:
                return (location.latitude, location.longitude)
        except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError):
            pass
        time.sleep(delay)
        delay *= 2  # Exponential backoff
    return None


def main():
    if not os.path.exists(MISSED_CSV):
        print(f"Missed cities file not found: {MISSED_CSV}")
        return
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    else:
        cache = {}
    with open(MISSED_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missed = list(reader)
    print(f"Attempting geocode lookup for {len(missed)} missed cities...")
    updated = 0
    for idx, row in enumerate(missed, 1):
        city = row["city"].strip()
        state = row["state"].strip()
        key = f"{city}, {state}"
        print(f"[{idx}/{len(missed)}] {key}: Geocoding...")
        latlon = geocode_city_state(city, state)
        if latlon:
            cache[key] = [latlon[0], latlon[1]]
            print(f"    Geocode lookup succeeded.")
            updated += 1
        else:
            print(f"    Geocode lookup FAILED.")
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=4)
    print(f"\nUpdated {updated} cities in cache: {os.path.abspath(CACHE_FILE)}")
    print(f"Total cache entries: {len(cache)}")


if __name__ == "__main__":
    main()
