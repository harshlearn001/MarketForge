#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | NSE CM Bhavcopy Downloader (ASK DATE)

✔ No hardcoded date
✔ No auto backtracking
✔ User / scheduler controlled date
✔ Uses nsearchives.nseindia.com
✔ ZIP validation
✔ Auto-run safe
"""

from pathlib import Path
from datetime import datetime
import requests

# =================================================
# ASK DATE
# =================================================
date_str = input(" Enter trade date (YYYY-MM-DD): ").strip()

try:
    trade_date = datetime.strptime(date_str, "%Y-%m-%d").date()
except ValueError:
    raise SystemExit(" Invalid date format. Use YYYY-MM-DD")

YYYYMMDD = trade_date.strftime("%Y%m%d")

# =================================================
# PATHS
# =================================================
OUT_DIR = Path(r"H:\MarketForge\data\raw\equity")
OUT_DIR.mkdir(parents=True, exist_ok=True)

FILENAME = f"BhavCopy_NSE_CM_0_0_0_{YYYYMMDD}_F_0000.csv.zip"
OUT_FILE = OUT_DIR / FILENAME

URL = f"https://nsearchives.nseindia.com/content/cm/{FILENAME}"

# =================================================
# NSE SAFE HEADERS
# =================================================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/",
}

# =================================================
# ZIP VALIDATION
# =================================================
def is_valid_zip(content: bytes) -> bool:
    return content[:2] == b"PK" and len(content) > 1024

# =================================================
# DOWNLOAD
# =================================================
print(f" Downloading CM Bhavcopy for {trade_date}")
print(f" URL: {URL}")

if OUT_FILE.exists():
    print(f"⏭ Already exists: {OUT_FILE.name}")
    raise SystemExit

session = requests.Session()
session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)

resp = session.get(URL, headers=HEADERS, timeout=30)

if resp.status_code != 200 or not is_valid_zip(resp.content):
    raise RuntimeError(" Bhavcopy not available for this date")

OUT_FILE.write_bytes(resp.content)

print(" Download successful")
print(f" Saved at: {OUT_FILE}")
print(" CM BHAVCOPY DOWNLOAD COMPLETED")
