import csv
import json
import os
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable

# Paths
HOMELENS_JSON = "src/output/homelens_output.json"
USCITIES_CSV = "resources/us_cities_latlon/uscities.csv"
US_TXT = "resources/us_cities_latlon/US.txt"
GAZ_TXT = "resources/us_cities_latlon/2025_Gaz_place_national.txt"
CACHE_FILE = "src/data/lat_lon_cache_GENERATED.json"
MAX_GEOCODE_TRIES = 5


def extract_city_state_pairs():
    pairs = set()
    with open(HOMELENS_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
        for group in ("renters", "buyers"):
            for entry in data.get(group, []):
                city = entry.get("city")
                state = entry.get("state")
                if city and state:
                    pairs.add((city.strip(), state.strip()))
    return pairs


def load_uscities():
    lookup = {}
    with open(USCITIES_CSV, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            city = row.get("city")
            state = row.get("state_name") or row.get("state_id")
            lat = row.get("lat")
            lon = row.get("lng") or row.get("lon")
            if city and state and lat and lon:
                try:
                    lat = float(lat)
                    lon = float(lon)
                    key = (city.strip(), state.strip())
                    lookup[key] = (lat, lon)
                except ValueError:
                    continue
    return lookup


def load_us_txt():
    lookup = {}
    with open(US_TXT, encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 11:
                continue
            city = parts[2]
            state = parts[4] or parts[3]
            lat = parts[9]
            lon = parts[10]
            try:
                lat = float(lat)
                lon = float(lon)
                key = (city.strip(), state.strip())
                lookup[key] = (lat, lon)
            except ValueError:
                continue
    return lookup


def load_gaz_txt():
    lookup = {}
    with open(GAZ_TXT, encoding="utf-8") as f:
        header = f.readline().strip().split("|")
        for line in f:
            parts = line.strip().split("|")
            if len(parts) < 13:
                continue
            city = parts[4]
            state = parts[0]
            lat = parts[11]
            lon = parts[12]
            try:
                lat = float(lat)
                lon = float(lon)
                key = (city.strip(), state.strip())
                lookup[key] = (lat, lon)
            except ValueError:
                continue
    return lookup


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
    pairs = extract_city_state_pairs()
    uscities = load_uscities()
    us_txt = load_us_txt()
    gaz_txt = load_gaz_txt()
    cache = {}
    missed = []
    pairs_list = list(pairs)
    print(
        f"Attempting to get lat/lon for {len(pairs_list)} unique city/state pairs...\n"
    )
    for idx, (city, state) in enumerate(pairs_list, 1):
        key = f"{city}, {state}"
        latlon = uscities.get((city, state))
        if latlon:
            print(f"[{idx}/{len(pairs_list)}] {key}: Found in uscities.csv")
        else:
            latlon = us_txt.get((city, state))
            if latlon:
                print(f"[{idx}/{len(pairs_list)}] {key}: Found in US.txt")
            else:
                latlon = gaz_txt.get((city, state))
                if latlon:
                    print(
                        f"[{idx}/{len(pairs_list)}] {key}: Found in 2025_Gaz_place_national.txt"
                    )
                else:
                    print(f"[{idx}/{len(pairs_list)}] {key}: Not found in any file.")
        if latlon:
            cache[key] = [latlon[0], latlon[1]]
        else:
            cache[key] = "NA"
            missed.append((city, state))
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=4)

    missed_csv = os.path.join(os.path.dirname(CACHE_FILE), "missed_cities.csv")
    with open(missed_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["city", "state"])
        for city, state in missed:
            writer.writerow([city, state])

    print(
        f"\nWrote {len(cache) - len(missed)} cities to cache: {os.path.abspath(CACHE_FILE)}"
    )
    print(f"Missed {len(missed)} cities, written to: {os.path.abspath(missed_csv)}")
    print(f"Total processed: {len(cache)}")


if __name__ == "__main__":
    main()
