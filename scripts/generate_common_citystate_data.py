import csv
import json
from collections import defaultdict

# File paths
HOUSING_CSV = "data/processed/housing.csv"
RENTALS_CSV = "data/processed/rentals.csv"
LAT_LON_CACHE = "src/data/lat_lon_cache_GENERATED.json"

HOUSING_COMMON_CSV = "data/processed/housing_common.csv"
RENTALS_COMMON_CSV = "data/processed/rentals_common.csv"
LAT_LON_COMMON_JSON = "src/data/lat_lon_cache_COMMON.json"


def get_city_state_key(row):
    return f"{row['city']}, {row['state']}"


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    # Read both CSVs
    housing = read_csv(HOUSING_CSV)
    rentals = read_csv(RENTALS_CSV)

    # Get city/state combos
    housing_keys = set(get_city_state_key(row) for row in housing)
    rentals_keys = set(get_city_state_key(row) for row in rentals)
    common_keys = housing_keys & rentals_keys

    # Filter rows
    housing_common = [row for row in housing if get_city_state_key(row) in common_keys]
    rentals_common = [row for row in rentals if get_city_state_key(row) in common_keys]

    # Write filtered CSVs
    if housing_common:
        write_csv(HOUSING_COMMON_CSV, housing_common[0].keys(), housing_common)
    if rentals_common:
        write_csv(RENTALS_COMMON_CSV, rentals_common[0].keys(), rentals_common)

    # Filter lat/lon cache
    with open(LAT_LON_CACHE, "r") as f:
        latlon_cache = json.load(f)
    latlon_common = {k: v for k, v in latlon_cache.items() if k in common_keys}
    with open(LAT_LON_COMMON_JSON, "w") as f:
        json.dump(latlon_common, f, indent=4)

    print(
        f"Wrote {len(housing_common)} housing, {len(rentals_common)} rentals, {len(latlon_common)} lat/lon entries."
    )


if __name__ == "__main__":
    main()
