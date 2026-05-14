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
  - Download manifest (download_manifest.json) tracks all completed downloads
    to ensure idempotency — no file is ever downloaded twice.
"""

import os
import json
import time
import requests
import zipfile
from pathlib import Path
import pandas as pd
import fastparquet
import boto3
from datetime import datetime, timezone
from awscredits import AWS_ACCESS_KEY,AWS_SECRET_KEY, AWS_REGION
from botocore.exceptions import ClientError



# ── Directories ───────────────────────────────────────────────────────────────
LOCAL_DOWNLOAD_DIR = "./downloads"
Path(LOCAL_DOWNLOAD_DIR).mkdir(exist_ok=True)

# ── Manifest file — single source of truth for what has been downloaded ───────
# Real-world pattern: same idea used by tools like wget --continue, Spark
# checkpointing, dbt state, and Airflow task state stores.
# Stored alongside downloads so it travels with the data.
MANIFEST_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "download_manifest.json")

# ── BTS config ────────────────────────────────────────────────────────────────
BTS_BASE_URL = "https://transtats.bts.gov/PREZIP/"
YEARS        = [2021]
MONTHS       = range(1, 4)   # extend to range(1, 13) for full year


# ── Manifest helpers ──────────────────────────────────────────────────────────

def _load_manifest() -> dict:
    """
    Load the download manifest from disk.

    Schema:
    {
      "downloaded_files": {
        "<filename>": {
          "status":        "completed",          # completed | failed
          "downloaded_at": "2024-01-15T10:30:00Z",
          "file_size_bytes": 123456789,
          "local_zip_path":  "./downloads/<filename>",
          "year":  2021,
          "month": 1
        },
        ...
      }
    }

    Real-world note: In production you'd store this in a Delta table, a DynamoDB
    table, or a dedicated metadata DB.  For a single-machine pipeline a JSON
    manifest is perfectly fine and has the advantage of being human-readable.
    """
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH, "r") as f:
            return json.load(f)
    # First run — create empty manifest
    return {"downloaded_files": {}}


def _save_manifest(manifest: dict) -> None:
    """Persist the manifest atomically (write-then-rename prevents corruption)."""
    tmp_path = MANIFEST_PATH + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(manifest, f, indent=2)
    os.replace(tmp_path, MANIFEST_PATH)   # atomic on POSIX & Windows


def _is_already_downloaded(manifest: dict, filename: str) -> bool:
    """
    Return True only when the file was previously completed successfully.
    A 'failed' entry is NOT treated as downloaded — it will be retried.
    """
    entry = manifest["downloaded_files"].get(filename)
    return entry is not None and entry.get("status") == "completed"


def _record_download(manifest: dict, filename: str, year: int, month: int,
                     local_zip_path: str, file_size_bytes: int) -> None:
    """Write a 'completed' entry into the manifest and flush to disk."""
    manifest["downloaded_files"][filename] = {
        "status":           "completed",
        "downloaded_at":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "file_size_bytes":  file_size_bytes,
        "local_zip_path":   local_zip_path,
        "year":             year,
        "month":            month,
    }
    _save_manifest(manifest)


def _record_failure(manifest: dict, filename: str, year: int, month: int,
                    error: str) -> None:
    """Write a 'failed' entry so we can see what went wrong and retry later."""
    manifest["downloaded_files"][filename] = {
        "status":      "failed",
        "failed_at":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "error":       error,
        "year":        year,
        "month":       month,
    }
    _save_manifest(manifest)


# ── Core download function ────────────────────────────────────────────────────

def download_bts_month(year: int, month: int, dest_dir: str,
                       manifest: dict) -> str | None:
    """
    Download BTS On-Time ZIP for a given year/month — idempotent.

    Idempotency logic (checked in this order):
      1. Manifest says 'completed'  → skip, return existing path.
      2. ZIP already on disk        → record it in the manifest & skip
         (handles the case where a previous run downloaded but crashed before
          writing the manifest).
      3. Neither                    → download, record in manifest.

    Returns the local ZIP path on success, None on failure.
    """
    filename      = (f"On_Time_Reporting_Carrier_On_Time_Performance"
                     f"_1987_present_{year}_{month}.zip")
    url           = f"{BTS_BASE_URL}{filename}"
    local_zip     = os.path.join(dest_dir, filename)

    # ── Guard 1: manifest says we already have it ──────────────────────────
    if _is_already_downloaded(manifest, filename):
        print(f"  [SKIP] Already downloaded (manifest): {filename}")
        return local_zip

    # ── Guard 2: file is on disk but manifest wasn't updated ───────────────
    # (crash-recovery / manual copy scenario)
    if os.path.exists(local_zip) and os.path.getsize(local_zip) > 0:
        size = os.path.getsize(local_zip)
        print(f"  [SKIP] ZIP found on disk but not in manifest — recording it.")
        print(f"         {filename}  ({size:,} bytes)")
        _record_download(manifest, filename, year, month, local_zip, size)
        return local_zip

    # ── Proceed with download ──────────────────────────────────────────────
    print(f"  [DOWNLOAD] {year}-{month:02d}  →  {url}")
    try:
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        bytes_written = 0
        with open(local_zip, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                f.write(chunk)
                bytes_written += len(chunk)

        print(f"  [OK]   Saved {bytes_written:,} bytes → {local_zip}")

        # ── Record success in manifest ─────────────────────────────────────
        _record_download(manifest, filename, year, month, local_zip, bytes_written)
        return local_zip

    except Exception as exc:
        print(f"  [ERROR] {year}-{month:02d}: {exc}")
        # Record failure so we can audit and retry
        _record_failure(manifest, filename, year, month, str(exc))
        # Remove partial file so next run starts clean
        if os.path.exists(local_zip):
            os.remove(local_zip)
        return None


# ── Manifest summary helper ───────────────────────────────────────────────────

def print_manifest_summary(manifest: dict) -> None:
    """Print a human-readable summary of the manifest."""
    entries = manifest["downloaded_files"]
    if not entries:
        print("  (manifest is empty — no downloads recorded yet)")
        return

    completed = [e for e in entries.values() if e["status"] == "completed"]
    failed    = [e for e in entries.values() if e["status"] == "failed"]
    total_bytes = sum(e.get("file_size_bytes", 0) for e in completed)

    print(f"\n{'─'*60}")
    print(f"  Manifest: {MANIFEST_PATH}")
    print(f"  Completed : {len(completed)}  |  Failed: {len(failed)}")
    print(f"  Total size: {total_bytes / (1024**3):.2f} GB")
    if failed:
        print("  Failed files:")
        for name, e in entries.items():
            if e["status"] == "failed":
                print(f"     {name}  — {e.get('error', 'unknown error')}")
    print(f"{'─'*60}\n")





# ─────────────────────────────────────────────────
# STEP 2: Download Airport Reference Data (FAA)
# ─────────────────────────────────────────────────
# FAA provides airport metadata in JSON format including:
# IATA code, name, city, state, lat, lon, elevation

def download_airport_reference(dest_dir):
    """
    Download airport reference from OpenFlights (public, free, no key needed).
     Nearby 7,700 airports worldwide.

    Columns: id, name, city, country, IATA, ICAO, lat, lon, altitude, tz_offset,
             DST, timezone_name, type, source

    DQ Watchpoints:
      - IATA field can be '\\N' (null in OpenFlights format) — filter these   [Some of the NUll values here]
      -- in python to see the null value  \\N is used
      - lat/lon values: range check lat -90..90, lon -180..180
      - Some airports appear twice with different source entries
    """
    url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
    local_path = os.path.join(dest_dir, "airports.dat")

    print(f"[DOWNLOAD] Airport reference data")
    resp = requests.get(url, timeout=30)

    # PRINTING THE AIRPORT DATA USING THE BELOW LINE
    # print(resp.content)

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



# ─────────────────────────────────────────────────
# STEP 3: Convert ZIP to Parquet
# ─────────────────────────────────────────────────
def convert_zip_to_parquet(zip_path ):

    # replace the string .zip to .parquet to create parquet later on
    parquet_path = zip_path.replace(".zip", ".parquet")

    with zipfile.ZipFile(zip_path) as zf:
        # take the name of the file
        csv_filename = [f for f in zf.namelist() if f.endswith(".csv")][0]
        with zf.open(csv_filename) as csv_file:
            df = pd.read_csv(csv_file)

    df.to_parquet(parquet_path, index=False)
    print(f" Parquet files saved --→ {parquet_path}")

    return parquet_path


# ------
# ─────────────────────────────────────────────────
# STEP 4: Upload Parquet to S3
# once we have the parquet file locally, we upload it to s3
# the folder structure year -> month is built automatically from the loop values
# if the bucket doesn't exist we create it before uploading
# ─────────────────────────────────────────────────

# HERE WE GIVE THE BUCKET_NAME
def upload_to_s3(local_path , year , month , bucket_name ) :

    # connect to s3 using our aws credentials

    # accessing all these from the awscredit.py file by importing it
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    # check if bucket exists — if not, create it
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f" ### The S3 Bucket '{bucket_name}' exists #### ")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        # 403 means bucket exists but belongs to someone else — we can't touch it
        if error_code == "403":
            raise PermissionError(
                f"Bucket '{bucket_name}' is owned by another AWS account."
            ) from e

        # 404 means bucket doesn't exist — let's create it
        print(f" The S3 Bucket ==> '{bucket_name}' is not found — so creating...")

        create_kwargs = {"Bucket": bucket_name}

        # us-east-1 is aws default region — it doesn't need LocationConstraint
        # every other region does otherwise aws throws an error
        if AWS_REGION != "us-east-1":
            create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": AWS_REGION}

        s3.create_bucket(**create_kwargs)
        print(f"  Bucket ==> '{bucket_name}' <== created.")

    # build the s3 path with year and month as partition folders
    # this is what makes spark happy when reading — it auto-detects partitions
    # raw/flights/year=2021/month=01/filename.parquet
    filename     = os.path.basename(local_path)
    s3_key       = f"flights/year={year}/month={month:02d}/{filename}"
    file_size_mb = os.path.getsize(local_path) / (1024 ** 2)

    print(f" ## --> ## UPLOAD {filename} ({file_size_mb:.1f} MB) → s3://{bucket_name}/{s3_key}")

    # upload the parquet file to s3
    s3.upload_file(
        local_path,
        bucket_name,
        s3_key,
        ExtraArgs={"ContentType": "application/octet-stream"},
    )

    print(f"  File OK Uploaded → s3://{bucket_name}/{s3_key}")


   # End of step 4 ----






# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("  US Flight Delay Analytics — Data Acquisition")
    print("=" * 70)

    # download the airport code info in dat file then the json file as required
    airport_path = download_airport_reference(LOCAL_DOWNLOAD_DIR)


    # Load (or create) the manifest once at startup
    manifest = _load_manifest()
    print(f"\n  Manifest loaded — {len(manifest['downloaded_files'])} entries found.")

    for year in YEARS:
        for month in MONTHS:
            zip_path = download_bts_month(year, month, LOCAL_DOWNLOAD_DIR, manifest)
            if zip_path:
                print(f" --- Ready ---: {zip_path}")
                # Uncomment when S3 upload is wired up:

                #ZIP TO PARQUET -- CALLING FUNCTIONS
                parquet_path = convert_zip_to_parquet(zip_path)

                upload_to_s3(parquet_path,year,month, 'airport-project-de' )
            time.sleep(2)   # Be polite to BTS servers

    print_manifest_summary(manifest)
    print("  Done.")