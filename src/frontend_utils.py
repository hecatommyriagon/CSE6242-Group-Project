import json
import logging


def read_json(filepath="src/output/sample_output.json"):
    logging.info(f"filepath = {filepath}")
    with open(filepath, "r") as file:
        data = json.load(file)
        return data


def update_data(data: dict) -> dict:
    """
    Only keep entries that already have lat/lon
    """
    logging.info(f"update_data(data)")
    renters = [
        datum
        for datum in data["renters"]
        if "latitude" in datum
        and "longitude" in datum
        and datum["latitude"]
        and datum["longitude"]
    ]
    buyers = [
        datum
        for datum in data["buyers"]
        if "latitude" in datum
        and "longitude" in datum
        and datum["latitude"]
        and datum["longitude"]
    ]
    data["renters"] = renters
    data["buyers"] = buyers
    return data


def get_data() -> dict:
    """
    Called by frontend js module to get data
    """
    logging.info(f"get_data()")
    data = read_json()
    data = update_data(data)
    return data
