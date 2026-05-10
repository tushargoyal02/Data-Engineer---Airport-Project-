"""
========================================================
PHASE 0: Data Acquisition — US Flight Delay Analytics
========================================================

Project: US Domestic Flight Delay Analytics Platform
Dataset: Bureau of Transportation Statistics (BTS) On-Time Performance
         https://www.transtats.bts.gov/

Data Engineer Notes:
  - We download 3 years of monthly CSVs (2021-2023) = ~7 GB raw
  - Airport reference (FAA) ~5 MB JSON
  - Carrier lookup CSV ~50 KB
  - Weather enrichment data (OpenWeatherMap) via API (optional)

DQ Watchpoints at this stage:
  - File completeness: verify row counts per month match BTS website
  - File encoding: BTS uses UTF-8 but some columns have commas inside quotes
  - Header consistency: all monthly files must have IDENTICAL column count
"""

import os
import requests
import zipfile
import io
import json
import time
import boto3
from pathlib import Path

# # ─────────────────────────────────────────────────
# # CONFIG — fill in your values
# # ─────────────────────────────────────────────────
# AWS_ACCESS_KEY     = "YOUR_ACCESS_KEY_HERE"
# AWS_SECRET_KEY     = "YOUR_SECRET_KEY_HERE"
# AWS_REGION         = "us-east-1"
# S3_BUCKET          = "your-flight-analytics-bucket"
# S3_RAW_PREFIX      = "raw/"

LOCAL_DOWNLOAD_DIR = "./downloads"
Path(LOCAL_DOWNLOAD_DIR).mkdir(exist_ok=True)

# ─────────────────────────────────────────────────
# STEP 1: Download BTS On-Time Performance CSV files
# ─────────────────────────────────────────────────
# BTS TranStats download URL pattern
# Each zip = ~120-180 MB compressed, ~500 MB unzipped per month
# 36 months (2021-2023) = ~7 GB uncompressed CSVs

BTS_BASE_URL = "https://transtats.bts.gov/PREZIP/"
# YEARS  = [2021, 2022, 2023]
YEARS  = [2021]
MONTHS = range(1, 2)

# Fields we want from BTS — reduces zip download size
# Key fields for a Data Engineer to always track:
#   FlightDate     — temporal anchor for all partitioning
#   Reporting_Airline — dimension key
#   Origin, Dest   — spatial keys
#   DepDelay, ArrDelay — core metrics (watch for negatives = early)
#   Cancelled, Diverted — boolean flags (0/1, NOT true/false)
#   CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay
#                  — delay cause breakdown (DQ: must sum = ArrDelay when not null)

BTS_FIELDS = [
    "FlightDate", "Reporting_Airline", "IATA_CODE_Reporting_Airline",
    "Tail_Number", "Flight_Number_Reporting_Airline", "Origin", "OriginCityName",
    "OriginState", "Dest", "DestCityName", "DestState", "CRSDepTime", "DepTime",
    "DepDelay", "DepDelayMinutes", "DepDel15", "DepartureDelayGroups",
    "CRSArrTime", "ArrTime", "ArrDelay", "ArrDelayMinutes", "ArrDel15",
    "ArrivalDelayGroups", "Cancelled", "CancellationCode", "Diverted",
    "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Flights", "Distance",
    "DistanceGroup", "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay"
]


def download_bts_month(year, month, dest_dir):
    """
    Download BTS On-Time CSV for a given year/month.
    BTS provides monthly zips at a fixed URL pattern.

    Data Engineer Note: Always verify md5 or at minimum row count after download.
    """
    filename = f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
    url = f"{BTS_BASE_URL}{filename}"

    local_zip  = os.path.join(dest_dir, filename)
    local_csv  = os.path.join(dest_dir, filename.replace(".zip", ".csv"))

    if os.path.exists(local_csv):
        print(f"  [SKIP] Already exists: {local_csv}")
        return local_csv

    print(f"  [DOWNLOAD] {year}-{month:02d} → {url}")
    try:
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        with open(local_zip, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

        # Extract CSV from zip
        with zipfile.ZipFile(local_zip, "r") as zf:
            csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
            zf.extract(csv_name, dest_dir)
            os.rename(os.path.join(dest_dir, csv_name), local_csv)

        os.remove(local_zip)
        size_mb = os.path.getsize(local_csv) / (1024**2)
        print(f"  [OK] {local_csv}  ({size_mb:.1f} MB)")
        return local_csv

    except Exception as e:
        print(f"  [ERROR] {year}-{month:02d}: {e}")
        return None


# Alternative: Download directly from Kaggle (requires kaggle API)
# Dataset: https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022
# ~8.5 GB — covers 2018-2022 already compiled, great for this project
# def download_from_kaggle():
#     """
#     Alternative: use Kaggle CLI to pull the pre-compiled dataset.
#     Run this instead of the BTS loop if you have a Kaggle account.

#     Steps:
#       1. pip install kaggle
#       2. Place kaggle.json in ~/.kaggle/
#       3. Uncomment and run this function

#     Data Engineer Note:
#       Kaggle dataset may have slight schema differences from raw BTS.
#       Always validate column names against official BTS codebook:
#       https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ
#     """
#     import subprocess
#     cmd = "kaggle datasets download -d robikscube/flight-delay-dataset-20182022 -p ./downloads --unzip"
#     print(f"Running: {cmd}")
#     subprocess.run(cmd, shell=True, check=True)


# ─────────────────────────────────────────────────
# STEP 2: Download Airport Reference Data (FAA)
# ─────────────────────────────────────────────────
# FAA provides airport metadata in JSON format including:
# IATA code, name, city, state, lat, lon, elevation
# DQ Note: ~150 US airports have changed codes since 2018 — track carefully

def download_airport_reference(dest_dir):
    """
    Download airport reference from OpenFlights (public, free, no key needed).
    ~7,700 airports worldwide. We filter to US later in Spark.

    Columns: id, name, city, country, IATA, ICAO, lat, lon, altitude, tz_offset,
             DST, timezone_name, type, source

    DQ Watchpoints:
      - IATA field can be '\\N' (null in OpenFlights format) — filter these
      - lat/lon values: range check lat -90..90, lon -180..180
      - Some airports appear twice with different source entries
    """
    url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
    local_path = os.path.join(dest_dir, "airports.dat")

    print(f"[DOWNLOAD] Airport reference data")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(resp.content)

    # Parse and save as clean JSON
    import csv
    cols = ["id","name","city","country","iata","icao","latitude","longitude",
            "altitude","utc_offset","dst","timezone","type","source"]
    airports = []
    for line in resp.text.splitlines():
        row = next(csv.reader([line]))
        if len(row) == len(cols):
            d = dict(zip(cols, row))
            # DQ: Filter valid IATA codes (3 uppercase letters)
            if d["iata"] and len(d["iata"]) == 3 and d["iata"] != "\\N":
                airports.append(d)

    json_path = os.path.join(dest_dir, "airports.json")
    with open(json_path, "w") as f:
        json.dump(airports, f, indent=2)

    print(f"  [OK] {len(airports)} airports → {json_path}")
    return json_path


# # ─────────────────────────────────────────────────
# # STEP 3: Download Carrier/Airline Reference
# # ─────────────────────────────────────────────────
# def download_carrier_reference(dest_dir):
#     """
#     BTS provides official carrier lookup table.
#     DQ Note: Carrier codes change due to mergers (e.g. AA absorbed US Airways).
#     We need ALL historical codes, not just active ones.

#     Key columns: Code, Description (airline name)
#     """
#     url = "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_CARRIER_HISTORY"
#     local_path = os.path.join(dest_dir, "carriers.csv")

#     print(f"[DOWNLOAD] Carrier lookup table")
#     # BTS sometimes blocks automated requests; use headers
#     headers = {"User-Agent": "Mozilla/5.0 (compatible; DataEngineer/1.0)"}
#     try:
#         resp = requests.get(url, headers=headers, timeout=30)
#         with open(local_path, "wb") as f:
#             f.write(resp.content)
#         print(f"  [OK] Carriers → {local_path}")
#     except Exception as e:
#         print(f"  [WARN] BTS carrier download failed: {e}")
#         print("  [INFO] Creating manual carrier CSV from well-known codes...")
#         create_manual_carriers(local_path)

#     return local_path


# def create_manual_carriers(path):
#     """Fallback: common US carriers for the project period 2021-2023."""
#     data = """Code,Description
# AA,American Airlines Inc.
# AS,Alaska Airlines Inc.
# B6,JetBlue Airways
# DL,Delta Air Lines Inc.
# F9,Frontier Airlines Inc.
# G4,Allegiant Air
# HA,Hawaiian Airlines Inc.
# NK,Spirit Air Lines
# OO,SkyWest Airlines Inc.
# QX,Horizon Air
# UA,United Air Lines Inc.
# WN,Southwest Airlines Co.
# YX,Republic Airways
# 9E,Endeavor Air Inc.
# MQ,Envoy Air
# OH,PSA Airlines Inc.
# YV,Mesa Airlines Inc.
# ZW,Air Wisconsin Airlines Corp
# """
#     with open(path, "w") as f:
#         f.write(data)
#     print(f"  [OK] Manual carrier CSV written to {path}")


# # ─────────────────────────────────────────────────
# # STEP 4: Upload everything to S3
# # ─────────────────────────────────────────────────
# def upload_to_s3(local_path, s3_prefix):
#     """
#     Upload a file to S3 under the given prefix.

#     Data Engineer Note on S3 organisation:
#       raw/flights/year=2021/month=01/On_Time_2021_1.csv  ← partition-aware naming
#       raw/airports/airports.json
#       raw/carriers/carriers.csv

#     Using partition-aware paths lets Spark auto-discover partitions
#     without a full table scan.
#     """
#     s3 = boto3.client(
#         "s3",
#         aws_access_key_id=AWS_ACCESS_KEY,
#         aws_secret_access_key=AWS_SECRET_KEY,
#         region_name=AWS_REGION
#     )

#     filename  = os.path.basename(local_path)
#     s3_key    = f"{S3_RAW_PREFIX}{s3_prefix}/{filename}"
#     file_size = os.path.getsize(local_path) / (1024**2)

#     print(f"  [UPLOAD] {filename} ({file_size:.1f} MB) → s3://{S3_BUCKET}/{s3_key}")

#     s3.upload_file(
#         local_path,
#         S3_BUCKET,
#         s3_key,
#         ExtraArgs={"ContentType": "text/csv" if filename.endswith(".csv") else "application/json"}
#     )
#     print(f"  [OK] Uploaded")


# ─────────────────────────────────────────────────
# MAIN EXECUTION
# ─────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  US Flight Delay Analytics — Phase 0: Data Acquisition")
    print("=" * 60)

    # Download reference files
    # airport_path = download_airport_reference(LOCAL_DOWNLOAD_DIR)
    # carrier_path = download_carrier_reference(LOCAL_DOWNLOAD_DIR)

    # # Upload reference files to S3
    # upload_to_s3(airport_path, "airports")
    # upload_to_s3(carrier_path, "carriers")

    # # Download monthly BTS flight data
    # print("\n[INFO] Starting BTS monthly downloads (this will take a while)")
    # print("[INFO] Tip: Run in screen/tmux or use nohup on a server")
    # print("[INFO] Alternatively: Download from Kaggle using download_from_kaggle()")
    # print()

    for year in YEARS:
        for month in MONTHS:
            csv_path = download_bts_month(year, month, LOCAL_DOWNLOAD_DIR)
            if csv_path:
                # Partition-aware S3 prefix
                # s3_prefix = f"flights/year={year}/month={month:02d}"
                print("---- Values of yeay and month", year, month)
                # upload_to_s3(csv_path, s3_prefix)
            time.sleep(2)  # Be polite to BTS servers

    # print("\n[DONE] All data uploaded to S3. Proceed to Phase 1 (Bronze Layer).")
    # print(f"  S3 bucket: s3://{S3_BUCKET}/{S3_RAW_PREFIX}")