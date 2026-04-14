#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge PRO
Participant Data Downloader (FINAL)

✔ Skips weekends
✔ Handles NSE holiday (no file)
✔ Avoids duplicate download
✔ Clean logs
✔ Production ready
"""

import requests
from pathlib import Path
from datetime import datetime

print("📡 DOWNLOADING PARTICIPANT DATA...\n")

# ==============================
# PATH
# ==============================
RAW_DIR = Path(r"H:\MarketForge\data\raw\participant")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ==============================
# DATE
# ==============================
today_dt = datetime.now()
today_str = today_dt.strftime("%d%m%Y")

file_path = RAW_DIR / f"participant_raw_{today_str}.csv"

# ==============================
# WEEKEND CHECK
# ==============================
if today_dt.weekday() >= 5:
    print("📅 Weekend → Market Closed → Skipped")
    exit()

# ==============================
# DUPLICATE CHECK
# ==============================
if file_path.exists():
    print(f"⏭️ Already downloaded → {file_path.name}")
    exit()

# ==============================
# URL
# ==============================
url = f"https://archives.nseindia.com/content/nsccl/fao_participant_vol_{today_str}.csv"

# ==============================
# REQUEST
# ==============================
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/csv",
    "Referer": "https://www.nseindia.com"
}

try:
    r = requests.get(url, headers=headers, timeout=10)

    if r.status_code == 200 and len(r.content) > 100:
        with open(file_path, "wb") as f:
            f.write(r.content)

        print(f"✅ Saved → {file_path}")

    else:
        print("📅 No data (Holiday / Market Closed) → Skipped")

except Exception as e:
    print("❌ Download error:", e)