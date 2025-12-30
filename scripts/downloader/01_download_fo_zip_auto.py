#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | AUTO NSE FO ZIP DOWNLOADER

Purpose:
- Fully automatic daily FO ZIP download
- Meant for scheduler / cron / PowerShell
- NO user input

Logic:
✔ Start from today
✔ Walk back till FO ZIP is found
✔ Skip weekends
✔ NSE archive only (stable)
✔ Safe if already downloaded
"""

import requests
from datetime import datetime, timedelta
from pathlib import Path

# =================================================
# PATHS (PROJECT ROOT SAFE)
# =================================================
ROOT = Path(__file__).resolve().parents[2]   # H:\MarketForge
SAVE_DIR = ROOT / "data" / "raw" / "futures"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# =================================================
# NSE ARCHIVE URL
# =================================================
BASE_URL = "https://nsearchives.nseindia.com/archives/fo/mkt/fo{date}.zip"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}

# =================================================
# ZIP VALIDATION
# =================================================
def is_valid_zip(content: bytes) -> bool:
    return content[:2] == b"PK" and len(content) > 50_000

# =================================================
# TRY DOWNLOAD
# =================================================
def try_download(trade_date: datetime) -> bool:
    date_str = trade_date.strftime("%d%m%Y")
    url = BASE_URL.format(date=date_str)
    out_file = SAVE_DIR / f"fo{date_str}.zip"

    if out_file.exists():
        print(f"Already exists: {out_file}")
        return True

    print(f"Trying: {url}")

    try:
        r = requests.get(url, headers=HEADERS, timeout=60)

        if r.status_code == 200 and is_valid_zip(r.content):
            out_file.write_bytes(r.content)
            print(f" Downloaded: {out_file}")
            return True

        print(f" Not available: fo{date_str}.zip")
        return False

    except requests.exceptions.RequestException as e:
        print(f" Network error {date_str}: {e}")
        return False

# =================================================
# AUTO MODE MAIN
# =================================================
if __name__ == "__main__":
    print("\n MarketForge | AUTO NSE FO ZIP DOWNLOADER")
    print(f" Save directory : {SAVE_DIR}\n")

    today = datetime.today()
    max_lookback = 15  # safe NSE window

    d = today
    for _ in range(max_lookback):
        if d.weekday() < 5:  # Monday–Friday
            if try_download(d):
                break
        d -= timedelta(days=1)
    else:
        raise RuntimeError(" AUTO MODE FAILED: No FO ZIP found")

    print("\n AUTO MODE DONE")
