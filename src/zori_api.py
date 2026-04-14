import csv
import os
import json
import logging
from flask import Blueprint, jsonify, request
from pathlib import Path
from src.frontend_utils import city_state_to_latlon


zori_api = Blueprint("zori_api", __name__)


zori_data = {}
zori_cities = []
ZORI_CSV = Path("resources/zillow_rent_index.csv")
ZORI_CITIES_CSV = Path("data/processed/zori_cities.csv")


def get_latest_rent(timeline):
    if not timeline:
        return None, None
    dates = sorted(timeline.keys())
    if not dates:
        return None, None
    latest_date = dates[-1]
    return latest_date, timeline[latest_date]


# Cache for city lat/lon to avoid repeated geocoding
LATLON_CACHE_FILE = "src/data/lat_lon_cache.json"
if os.path.exists(LATLON_CACHE_FILE):
    with open(LATLON_CACHE_FILE, "r") as f:
        latlon_cache = json.load(f)
else:
    latlon_cache = {}


# Helper function to get lat/lon for a city, using cache and geocoding as needed
def get_city_latlon(city_key):
    # Check cache first
    if city_key in latlon_cache:
        logging.info(f"latlon_cache hit for {city_key}: {latlon_cache[city_key]}")
        return latlon_cache[city_key]

    # If not in cache, geocode and update cache
    try:
        city, state = city_key.rsplit(", ", 1)
        logging.info(f"Geocoding {city_key} ...")
        lat, lon = city_state_to_latlon(city, state)
        logging.info(f"Geocode result for {city_key}: {lat}, {lon}")
        if lat and lon:
            latlon_cache[city_key] = (lat, lon)

            # Save updated cache to file
            os.makedirs(os.path.dirname(LATLON_CACHE_FILE), exist_ok=True)
            with open(LATLON_CACHE_FILE, "w") as f:
                json.dump(latlon_cache, f, indent=2)

            return lat, lon
    except Exception as e:
        logging.error(f"Geocoding failed for {city_key}: {e}")

    return None, None


# API endpoint to get all cities with their latest ZORI and lat/lon for map plotting
@zori_api.route("/api/zori/all_points")
def get_all_zori_points():
    load_zori()
    points = []
    for city_key in zori_cities:
        timeline = zori_data.get(city_key)
        latest_date, latest_rent = get_latest_rent(timeline)
        lat, lon = get_city_latlon(city_key)
        if lat and lon and latest_rent:
            points.append(
                {
                    "city": city_key,
                    "latitude": lat,
                    "longitude": lon,
                    "latest_rent": latest_rent,
                    "latest_date": latest_date,
                }
            )
    return jsonify(points)


# API endpoint to get ZORI timeline and lat/lon for a specific city
def load_zori():
    global zori_data, zori_cities
    if zori_data:
        return

    # First try to load city list from cache, then load data for all cities (which will also populate cache if needed)
    if ZORI_CITIES_CSV.exists():
        with open(ZORI_CITIES_CSV, newline="") as f:
            reader = csv.reader(f)
            zori_cities = [row[0] for row in reader]
    else:
        # If no cache, read from original CSV and extract city list, then write to cache for future use
        with open(ZORI_CSV, newline="") as f:
            reader = csv.DictReader(f)
            city_set = set()
            for row in reader:
                city = row["RegionName"]
                state = row["State"]
                city_key = f"{city}, {state}"
                city_set.add(city_key)
            zori_cities = sorted(city_set)

        ZORI_CITIES_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(ZORI_CITIES_CSV, "w", newline="") as f:
            writer = csv.writer(f)
            for city in zori_cities:
                writer.writerow([city])

    # Now load the full ZORI data for all cities (this will also populate lat/lon cache as needed)
    with open(ZORI_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row["RegionName"]
            state = row["State"]
            city_key = f"{city}, {state}"
            if city_key not in zori_data:
                timeline = {
                    k: float(v) for k, v in row.items() if k[:4].isdigit() and v
                }
                zori_data[city_key] = timeline


# API endpoint to get list of cities with ZORI data
@zori_api.route("/api/zori/cities")
def get_cities():
    load_zori()
    return jsonify(sorted(zori_cities))


# API endpoint to get ZORI timeline and lat/lon for a specific city
@zori_api.route("/api/zori/<city>")
def get_city_zori(city):
    load_zori()

    # Fetch timeline for requested city, return 404 if not found
    timeline = zori_data.get(city)
    if not timeline:
        return jsonify({"error": "City not found"}), 404

    # Sort timeline by date and get lat/lon for city
    sorted_timeline = dict(sorted(timeline.items()))
    lat, lon = get_city_latlon(city)

    return jsonify(
        {"city": city, "timeline": sorted_timeline, "latitude": lat, "longitude": lon}
    )
