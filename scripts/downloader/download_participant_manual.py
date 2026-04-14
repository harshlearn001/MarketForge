#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge PRO
Manual Participant Data Downloader (Plan B)

✔ Ask user date input
✔ Supports multiple formats
✔ Handles invalid / holiday
✔ Avoids duplicate
"""

import requests
from pathlib import Path
from datetime import datetime

print("📡 MANUAL PARTICIPANT DOWNLOAD (PLAN B)\n")

# ==============================
# PATH
# ==============================
RAW_DIR = Path(r"H:\MarketForge\data\raw\participant")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ==============================
# INPUT DATE
# ==============================
user_input = input("Enter date (DDMMYYYY or YYYY-MM-DD): ").strip()

# ==============================
# PARSE DATE
# ==============================
try:
    if "-" in user_input:
        dt = datetime.strptime(user_input, "%Y-%m-%d")
    else:
        dt = datetime.strptime(user_input, "%d%m%Y")

    date_str = dt.strftime("%d%m%Y")

except:
    print("❌ Invalid date format")
    exit()

# ==============================
# FILE CHECK
# ==============================
file_path = RAW_DIR / f"participant_raw_{date_str}.csv"

if file_path.exists():
    print(f"⏭️ Already exists → {file_path.name}")
    exit()

# ==============================
# URL
# ==============================
url = f"https://archives.nseindia.com/content/nsccl/fao_participant_vol_{date_str}.csv"

# ==============================
# REQUEST
# ==============================
headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nseindia.com"
}

print(f"📅 Downloading for date: {dt.strftime('%Y-%m-%d')}")

try:
    r = requests.get(url, headers=headers, timeout=10)

    if r.status_code == 200 and len(r.content) > 100:
        with open(file_path, "wb") as f:
            f.write(r.content)

        print(f"✅ Saved → {file_path}")

    else:
        print("📅 No data available (Holiday / Market Closed / Invalid date)")

except Exception as e:
    print("❌ Download error:", e)