import csv
import json
import os
from flask import Blueprint, jsonify, request

# Blueprint for new data API
data_api = Blueprint("data_api", __name__)

RENTALS_CSV = os.path.join("data", "processed", "rentals_common.csv")
HOUSING_CSV = os.path.join("data", "processed", "housing_common.csv")


def parse_json_field(val):
    if isinstance(val, str):
        try:
            # Try JSON first
            return json.loads(val.replace("'", '"'))
        except Exception:
            return val
    return val


def load_csv_data(csv_path):
    results = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse statistics and cost fields as JSON if possible
            if "statistics" in row:
                row["statistics"] = parse_json_field(row["statistics"])
            if "cost" in row:
                row["cost"] = parse_json_field(row["cost"])
            # Ensure lat/lon are floats
            if "latitude" in row and row["latitude"]:
                row["latitude"] = float(row["latitude"])
            if "longitude" in row and row["longitude"]:
                row["longitude"] = float(row["longitude"])
            results.append(row)
    return results


@data_api.route("/api/data/cities")
def get_data_cities():
    renters = load_csv_data(RENTALS_CSV)
    buyers = load_csv_data(HOUSING_CSV)
    cities = sorted(
        set((r["city"], r["state"]) for r in renters)
        | set((b["city"], b["state"]) for b in buyers)
    )
    return jsonify([f"{city}, {state}" for city, state in cities])


@data_api.route("/api/data/<city>")
def get_city_data(city):
    renters = load_csv_data(RENTALS_CSV)
    buyers = load_csv_data(HOUSING_CSV)
    # city is 'City, ST'
    city_name, state = city.rsplit(", ", 1)
    renter = next(
        (r for r in renters if r["city"] == city_name and r["state"] == state), None
    )
    buyer = next(
        (b for b in buyers if b["city"] == city_name and b["state"] == state), None
    )
    return jsonify({"city": city, "renter": renter, "buyer": buyer})
