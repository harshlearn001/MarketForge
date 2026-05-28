#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge PRO
Participant Data Downloader (FINAL - PRO)

✔ Skips weekends
✔ Handles NSE blocking (session fix)
✔ Handles NSE holiday (content validation)
✔ Retry logic (3 attempts)
✔ Avoids duplicate download
✔ Clean logs
✔ Production ready
"""

import requests
from pathlib import Path
from datetime import datetime
import sys

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
    sys.exit()

# ==============================
# DUPLICATE CHECK
# ==============================
if file_path.exists():
    print(f"⏭️ Already downloaded → {file_path.name}")
    sys.exit()

# ==============================
# URL
# ==============================
url = f"https://archives.nseindia.com/content/nsccl/fao_participant_vol_{today_str}.csv"

# ==============================
# SESSION (IMPORTANT FIX)
# ==============================
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nseindia.com"
})

try:
    # 🔥 Establish session (prevents blocking)
    session.get("https://www.nseindia.com", timeout=10)

    success = False

    # ==============================
    # RETRY LOGIC
    # ==============================
    for attempt in range(3):
        try:
            print(f"🔄 Attempt {attempt+1}...")

            r = session.get(url, timeout=10)

            content = r.content.decode(errors="ignore")

            # ==============================
            # VALIDATION (HOLIDAY / BLOCK CHECK)
            # ==============================
            if r.status_code == 200 and "Client Type" in content:

                with open(file_path, "wb") as f:
                    f.write(r.content)

                print(f"✅ Saved → {file_path}")
                success = True
                break

        except Exception as e:
            print(f"⚠️ Attempt {attempt+1} failed")

    # ==============================
    # FINAL STATUS
    # ==============================
    if not success:
        print("📅 No valid data → Holiday / Blocked")

except Exception as e:
    print("❌ Download error:", e)