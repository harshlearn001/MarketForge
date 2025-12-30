#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | MANUAL / FALLBACK NSE FO ZIP DOWNLOADER

✔ Correct project root
✔ Stable NSE archive
✔ Manual date input
✔ Safe fallback
"""

import requests
from datetime import datetime, timedelta
from pathlib import Path

# =================================================
# PATHS (FIXED ROOT)
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

    print(f" Trying: {url}")

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
# ASK DATE
# =================================================
def ask_date() -> datetime:
    user = input(" Enter FO trade date (DD-MM-YYYY) [Enter = today]: ").strip()
    if not user:
        return datetime.today()
    try:
        return datetime.strptime(user, "%d-%m-%Y")
    except ValueError:
        print(" Invalid format. Use DD-MM-YYYY")
        raise SystemExit(1)

# =================================================
# MAIN
# =================================================
if __name__ == "__main__":
    print("\n MarketForge | NSE FO ZIP MANUAL DOWNLOADER")

    start_date = ask_date()
    lookback_days = 15

    print(f" Starting from: {start_date.strftime('%d-%b-%Y')}")
    print(f" Save directory : {SAVE_DIR}\n")

    d = start_date
    for _ in range(lookback_days):
        if d.weekday() < 5:
            if try_download(d):
                break
        d -= timedelta(days=1)
    else:
        print(" No FO ZIP found in lookback window")

    print("\n DONE")
