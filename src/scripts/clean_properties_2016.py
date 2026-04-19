"""
Script to remove bad rows from properties_2016.csv, and reduce amount of columns
    - drop rows with missing/invalid lat/lon, non-positive sqft, or missing bedroom
    - creates cleaned CSV for downstream use
    - new CSV only contains the following columns: 
        * parcelid, 
        * latitude, longitude, 
        * sqft, bedroomcnt, bathroomcnt, yearbuilt, 
        * regionidcity, regionidzip, 
        * propertylandusetypeid, propertycountylandusecode, 
        * lotsizesquarefeet, taxvaluedollarcnt

Usage:
    python src/clean_properties.py --input resources/properties_2016.csv --output data/processed/properties_2016_cleaned.csv
"""

import os
import argparse
import logging
from typing import List

import pandas as pd

logging.basicConfig(level=logging.INFO)


REQUIRED_COLS = [
    "parcelid",
    "latitude",
    "longitude",
    "calculatedfinishedsquarefeet",
    "finishedsquarefeet12",
    "bedroomcnt",
    "bathroomcnt",
    "yearbuilt",
    "regionidcity",
    "regionidzip",
    "propertylandusetypeid",
    "propertycountylandusecode",
    "lotsizesquarefeet",
    "taxvaluedollarcnt",
]


def normalize_latlon(df: pd.DataFrame) -> pd.DataFrame:
    # some lat/lon values are in microdegrees, so convert those to degrees
    def fix_val(v):
        try:
            if pd.isna(v):
                return v
            fv = float(v)
            if abs(fv) > 180:
                return fv / 1e6
            return fv
        except Exception:
            return v

    df["latitude"] = df["latitude"].apply(fix_val)
    df["longitude"] = df["longitude"].apply(fix_val)

    return df


def choose_sqft(df: pd.DataFrame, out_col: str = "sqft") -> pd.DataFrame:
    # prefer calculatedfinishedsquarefeet, but fall back to finishedsquarefeet12 when missing
    df[out_col] = df["calculatedfinishedsquarefeet"].fillna(df.get("finishedsquarefeet12"))
    return df


def is_valid_row(row) -> bool:
    # row must have lat/lon, positive sqft, and bedroom count
    try:
        lat = row["latitude"]
        lon = row["longitude"]
        sqft = row["sqft"]
        beds = row["bedroomcnt"]
    except KeyError:
        return False

    if pd.isna(lat) or pd.isna(lon):
        return False
    if not (-90 <= float(lat) <= 90 and -180 <= float(lon) <= 180):
        return False

    if pd.isna(sqft) or float(sqft) <= 0:
        return False

    if pd.isna(beds):
        return False

    return True


def clean_file(input_path: str, output_path: str, chunksize: int = 100000) -> None:
    logging.info(f"Cleaning {input_path} -> {output_path}")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # inspect header and select available required columns
    sample = pd.read_csv(input_path, nrows=0)
    cols = list(sample.columns)
    usecols = [c for c in REQUIRED_COLS if c in cols]

    logging.info(f"Found {len(cols)} columns, using {len(usecols)} columns for cleaning")

    total = 0
    kept = 0
    dropped = {"latlon": 0, "sqft": 0, "beds": 0}

    first_write = True

    for chunk in pd.read_csv(input_path, usecols=usecols, chunksize=chunksize):
        total += len(chunk)

        # normalize lat/lon
        if "latitude" in chunk.columns and "longitude" in chunk.columns:
            chunk = normalize_latlon(chunk)

        # add sqft column
        if "calculatedfinishedsquarefeet" in chunk.columns or "finishedsquarefeet12" in chunk.columns:
            # ensure both columns exist to allow fillna
            if "calculatedfinishedsquarefeet" not in chunk.columns:
                chunk["calculatedfinishedsquarefeet"] = pd.NA
            if "finishedsquarefeet12" not in chunk.columns:
                chunk["finishedsquarefeet12"] = pd.NA

            chunk = choose_sqft(chunk, out_col="sqft")
        else:
            chunk["sqft"] = pd.NA

        # filter valid rows
        mask_valid = chunk.apply(is_valid_row, axis=1)

        # count dropped rows by reason
        invalid = chunk[~mask_valid]
        if not invalid.empty:
            dropped["latlon"] += int(invalid[invalid["latitude"].isna() | invalid["longitude"].isna()].shape[0])
            dropped["sqft"] += int(invalid[invalid["sqft"].isna() | (invalid["sqft"] <= 0)].shape[0])
            dropped["beds"] += int(invalid[invalid["bedroomcnt"].isna()].shape[0])

        kept_chunk = chunk[mask_valid].copy()

        # write reduced set of columns to output
        out_cols = [c for c in ["parcelid", "latitude", "longitude", "sqft", "bedroomcnt", "bathroomcnt", "yearbuilt", "regionidcity", "regionidzip", "propertylandusetypeid", "propertycountylandusecode", "lotsizesquarefeet", "taxvaluedollarcnt"] if c in kept_chunk.columns]

        if kept_chunk.empty:
            continue

        if first_write:
            kept_chunk.to_csv(output_path, index=False, columns=out_cols, mode="w")
            first_write = False
        else:
            kept_chunk.to_csv(output_path, index=False, columns=out_cols, mode="a", header=False)

        kept += len(kept_chunk)

    logging.info("Cleaning complete")
    logging.info(f"Total rows scanned: {total}")
    logging.info(f"Rows kept: {kept}")
    logging.info(f"Rows dropped (approx): {dropped}")


def parse_args(argv: List[str] = None):
    parser = argparse.ArgumentParser(description="Clean properties CSV for downstream processing")
    parser.add_argument("--input", default="resources/properties_2016.csv")
    parser.add_argument("--output", default="data/processed/properties_2016_cleaned.csv")
    parser.add_argument("--chunksize", type=int, default=100000)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    clean_file(args.input, args.output, args.chunksize)
