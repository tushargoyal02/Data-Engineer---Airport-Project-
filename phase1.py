"""
Data Acquisition — US Flight Delay Analytics
========================================================

Project: US Domestic Flight Delay Analytics Platform
Dataset: Bureau of Transportation Statistics (BTS) On-Time Performance
         https://www.transtats.bts.gov/

Data Engineer Notes:
  - We download 3 years of monthly CSVs (2021-2023) = ~7 GB raw
  - Airport reference (FAA) ~5 MB JSON
  - Carrier lookup CSV ~50 KB
"""

import os
import requests
import zipfile
import io
import json
import time
import boto3
from pathlib import Path



LOCAL_DOWNLOAD_DIR = "./downloads"
Path(LOCAL_DOWNLOAD_DIR).mkdir(exist_ok=True)



BTS_BASE_URL = "https://transtats.bts.gov/PREZIP/"
# YEARS  = [2021, 2022, 2023]
YEARS  = [2021]
MONTHS = range(1, 2)


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

  

    except Exception as e:
        print(f"  [ERROR] {year}-{month:02d}: {e}")
        return None

if __name__ == "__main__":
    print("=" * 30)
    print("  US Flight Delay Analytics — Data Acquisition")
    print("=" * 70)

    
    # access all the files of each year and month
    for year in YEARS:
        for month in MONTHS:
            csv_path = download_bts_month(year, month, LOCAL_DOWNLOAD_DIR)
            if csv_path:
                # Partition-aware S3 prefix
                # s3_prefix = f"flights/year={year}/month={month:02d}"
                print("---- Values of yeay and month", year, month)
                # upload_to_s3(csv_path, s3_prefix)
            time.sleep(2)  # Be polite to BTS servers
