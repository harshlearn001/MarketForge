#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | MANUAL NSE DELIVERY MTO DAT DOWNLOADER

Purpose:
- Manual / fallback download of NSE MTO delivery file
- Used when AUTO mode or scheduler fails
- Debug & recovery friendly

Features:
✔ Asks trade date (DD-MM-YYYY)
✔ Uses NSE ARCHIVE (authoritative)
✔ Required Referer header
✔ Saves file AS-IS
✔ Clear save-path output
"""

import requests
from datetime import datetime
from pathlib import Path
import sys

# =================================================
# PATHS (PROJECT ROOT SAFE)
# =================================================
ROOT = Path(__file__).resolve().parents[2]   # H:\MarketForge
SAVE_DIR = ROOT / "data" / "raw" / "equityDat"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# =================================================
# NSE ARCHIVE URL (AUTHORITATIVE)
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
# ASK DATE
# =================================================
def ask_date() -> datetime:
    user = input(" Enter trading date (DD-MM-YYYY): ").strip()
    try:
        return datetime.strptime(user, "%d-%m-%Y")
    except ValueError:
        print(" Invalid date format. Use DD-MM-YYYY")
        sys.exit(1)

# =================================================
# DOWNLOAD
# =================================================
def download_mto(trade_date: datetime):
    date_str = trade_date.strftime("%d%m%Y")
    url = BASE_URL.format(date=date_str)
    out_file = SAVE_DIR / f"MTO_{date_str}.DAT"

    print(f"\nDownloading NSE MTO Delivery File")
    print(f" Trade date : {trade_date.date()}")
    print(f" URL        : {url}")
    print(f" Save path  : {out_file}")

    if out_file.exists():
        print(" File already exists — skipping download")
        return

    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)

        if resp.status_code == 200 and len(resp.content) > 100_000:
            out_file.write_bytes(resp.content)
            print(" Download successful")
        else:
            print(" MTO file not available on NSE")
            print(" Either holiday or file not yet published")
            print("Try after 6:30 PM IST")

    except requests.exceptions.RequestException as e:
        print(f" Network error: {e}")

# =================================================
# MAIN
# =================================================
if __name__ == "__main__":
    print("\n MarketForge | MANUAL NSE MTO DAT DOWNLOADER")

    trade_date = ask_date()
    download_mto(trade_date)

    print("\n MANUAL MODE DONE")
