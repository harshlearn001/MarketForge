#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | AUTO NSE DELIVERY MTO DAT DOWNLOADER

✔ Uses NSE ARCHIVE (authoritative)
✔ Referer header (required)
✔ No user input
✔ Auto fallback to previous trading days
✔ Scheduler safe
"""

import requests
from datetime import datetime, timedelta
from pathlib import Path

# =================================================
# PATHS (PROJECT ROOT SAFE)
# =================================================
ROOT = Path(__file__).resolve().parents[2]   # H:\MarketForge
SAVE_DIR = ROOT / "data" / "raw" / "equityDat"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# =================================================
# NSE ARCHIVE URL (CORRECT)
# =================================================
BASE_URL = "https://nsearchives.nseindia.com/archives/equities/mto/MTO_{date}.DAT"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nseindia.com/",
    "Accept": "*/*",
}

# =================================================
# DAT VALIDATION
# =================================================
def is_valid_dat(content: bytes) -> bool:
    # MTO files are always reasonably large
    return len(content) > 100_000

# =================================================
# TRY DOWNLOAD
# =================================================
def try_download(trade_date: datetime) -> bool:
    date_str = trade_date.strftime("%d%m%Y")
    url = BASE_URL.format(date=date_str)
    out_file = SAVE_DIR / f"MTO_{date_str}.DAT"

    if out_file.exists():
        print(f"Already exists: {out_file}")
        return True

    print(f" Trying: {url}")

    try:
        r = requests.get(url, headers=HEADERS, timeout=60)

        if r.status_code == 200 and is_valid_dat(r.content):
            out_file.write_bytes(r.content)
            print(f" Downloaded: {out_file}")
            return True

        print(f" Not available: MTO_{date_str}.DAT")
        return False

    except requests.exceptions.RequestException as e:
        print(f" Network error {date_str}: {e}")
        return False

# =================================================
# AUTO MODE MAIN
# =================================================
if __name__ == "__main__":
    print("\n MarketForge | AUTO NSE DELIVERY MTO DAT DOWNLOADER")
    print(f" Save directory : {SAVE_DIR}\n")

    today = datetime.today()
    max_lookback = 10  # delivery published same day / next day

    d = today
    for _ in range(max_lookback):
        if d.weekday() < 5:  # Mon–Fri
            if try_download(d):
                break
        d -= timedelta(days=1)
    else:
        raise RuntimeError("AUTO MODE FAILED: No MTO DAT found")

    print("\n AUTO MODE DONE")
