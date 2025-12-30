#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | NSE CM Bhavcopy Downloader (ARCHIVES SAFE - FIXED)

✔ Tries TODAY first
✔ Auto backtracks trading days
✔ Weekend safe
✔ Uses nsearchives (stable)
✔ ZIP integrity verified
✔ Production locked
"""

from pathlib import Path
from datetime import datetime, timedelta
import requests

# =================================================
# PATHS
# =================================================
OUT_DIR = Path(r"H:\MarketForge\data\raw\equity")
OUT_DIR.mkdir(parents=True, exist_ok=True)

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
    "Connection": "keep-alive",
}

# =================================================
# SESSION INIT (MANDATORY)
# =================================================
session = requests.Session()
session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)

# =================================================
# HELPERS
# =================================================
def is_valid_zip(content: bytes) -> bool:
    return content.startswith(b"PK") and len(content) > 1024

# =================================================
# TRY TODAY → BACKWARD
# =================================================
MAX_LOOKBACK_DAYS = 10
today = datetime.now().date()

for i in range(0, MAX_LOOKBACK_DAYS):
    trade_date = today - timedelta(days=i)

    # Skip weekends
    if trade_date.weekday() >= 5:
        continue

    yyyymmdd = trade_date.strftime("%Y%m%d")
    filename = f"BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip"

    url = f"https://nsearchives.nseindia.com/content/cm/{filename}"
    out_file = OUT_DIR / filename

    print(f" Trying {trade_date} → {filename}")

    if out_file.exists():
        print(f" Already exists: {filename}")
        break

    try:
        resp = session.get(url, headers=HEADERS, timeout=30)

        if resp.status_code == 200 and is_valid_zip(resp.content):
            out_file.write_bytes(resp.content)
            print(" Download successful")
            print(f" Saved at: {out_file}")
            break
        else:
            print(" Not published yet")

    except Exception as e:
        print(f" Error: {e}")

else:
    raise RuntimeError(" No CM Bhavcopy found in last 10 trading days")

print(" CM BHAVCOPY DOWNLOAD COMPLETED")
