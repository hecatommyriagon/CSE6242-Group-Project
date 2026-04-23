import os
import sys
import json
import csv
import time
from pathlib import Path
from collections import defaultdict
from typing import Dict, Tuple, Optional
import requests

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RESOURCES = BASE_DIR / "resources"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
US_CITIES_LATLON = RESOURCES / "us_cities_latlon"

# Datasets for lat/lon lookup (priority order)
DATASETS = [
    US_CITIES_LATLON / "uscities.csv",
    US_CITIES_LATLON / "US.txt",
    US_CITIES_LATLON / "2025_Gaz_place_national.txt",
]

# Geocoding config
GEOCODE_URL = "https://nominatim.openstreetmap.org/search"
GEOCODE_USER_AGENT = "homelens-batch-geocoder/1.0"
GEOCODE_RETRIES = 3
GEOCODE_SLEEP = 1.5  # seconds between retries

# Input/output
INPUT_JSON = RESOURCES / "homelens_output.json"
TS = time.strftime("%Y%m%d_%H%M%S")
RENTALS_CSV = DATA_PROCESSED / f"rentals_{TS}.csv"
HOUSING_CSV = DATA_PROCESSED / f"housing_{TS}.csv"

# Helper: Load city/state/lat/lon from all datasets
CITY_LATLON = {}


def load_city_latlon():
    # uscities.csv: city,state_id,lat,lng
    uscities = DATASETS[0]
    if uscities.exists():
        with open(uscities, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row["city"].strip().lower(), row["state_id"].strip().lower())
                CITY_LATLON[key] = (float(row["lat"]), float(row["lng"]))
    # US.txt: US\tZIP\tCity\tState\tST\t...\tlat\tlng
    us_txt = DATASETS[1]
    if us_txt.exists():
        with open(us_txt, newline="") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 11:
                    city, state, lat, lon = parts[2], parts[4], parts[9], parts[10]
                    try:
                        key = (city.strip().lower(), state.strip().lower())
                        CITY_LATLON.setdefault(key, (float(lat), float(lon)))
                    except Exception:
                        continue
    # 2025_Gaz_place_national.txt: ...|City|...|lat|lon
    gaz = DATASETS[2]
    if gaz.exists():
        with open(gaz, newline="") as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) >= 12:
                    city, state, lat, lon = parts[4], parts[0], parts[10], parts[11]
                    try:
                        key = (city.strip().lower(), state.strip().lower())
                        CITY_LATLON.setdefault(key, (float(lat), float(lon)))
                    except Exception:
                        continue


def lookup_latlon(city: str, state: str) -> Optional[Tuple[float, float]]:
    key = (city.strip().lower(), state.strip().lower())
    return CITY_LATLON.get(key)


def geocode_city(city: str, state: str) -> Optional[Tuple[float, float]]:
    params = {
        "city": city,
        "state": state,
        "country": "USA",
        "format": "json",
        "limit": 1,
    }
    headers = {"User-Agent": GEOCODE_USER_AGENT}
    for attempt in range(GEOCODE_RETRIES):
        try:
            resp = requests.get(GEOCODE_URL, params=params, headers=headers, timeout=10)
            if resp.status_code == 200:
                results = resp.json()
                if results:
                    lat = float(results[0]["lat"])
                    lon = float(results[0]["lon"])
                    return lat, lon
            time.sleep(GEOCODE_SLEEP)
        except Exception:
            time.sleep(GEOCODE_SLEEP)
    return None


def parse_statistics(stats):
    if isinstance(stats, dict):
        return stats
    if not stats:
        return {}
    try:
        return json.loads(stats.replace("'", '"'))
    except Exception:
        return {}


def parse_cost(cost):
    if isinstance(cost, dict):
        return cost
    if not cost:
        return {}
    try:
        return json.loads(cost.replace("'", '"'))
    except Exception:
        return {}


def process_entry(entry, kind):
    city = entry.get("city")
    state = entry.get("state")
    latlon = lookup_latlon(city, state)
    # if not latlon:
    # latlon = geocode_city(city, state)
    lat, lon = latlon if latlon else (None, None)
    stats = parse_statistics(entry.get("statistics"))
    cost = parse_cost(entry.get("cost"))
    return {
        "city": city,
        "state": state,
        "cost": json.dumps(cost),
        "statistics": json.dumps(stats),
        "latitude": lat,
        "longitude": lon,
    }


def main():
    load_city_latlon()
    with open(INPUT_JSON) as f:
        data = json.load(f)
    renters = data.get("renters", [])
    buyers = data.get("buyers", [])
    # Output headers
    headers = ["city", "state", "cost", "statistics", "latitude", "longitude"]
    # Rentals
    with open(RENTALS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for entry in renters:
            row = process_entry(entry, "renter")
            writer.writerow(row)
    # Housing
    with open(HOUSING_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for entry in buyers:
            row = process_entry(entry, "buyer")
            writer.writerow(row)
    print(f"Wrote {RENTALS_CSV}")
    print(f"Wrote {HOUSING_CSV}")


if __name__ == "__main__":
    main()
