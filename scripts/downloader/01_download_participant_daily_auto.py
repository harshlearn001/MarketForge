#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge PRO
Participant Daily Data Downloader (v1.1 - Smart Lookback Engine)
"""

import os
import time
import random
import requests
from pathlib import Path
from datetime import datetime, timedelta
import holidays

print("📡 INITIALIZING DAILY PARTICIPANT DOWNLOADER (SMART LOOKBACK)...\n")

# ==========================================
# CONFIGURATION & PATHS
# ==========================================
RAW_DAILY_DIR = Path(r"H:\MarketForge\data\raw\participant_daily")
RAW_DAILY_DIR.mkdir(parents=True, exist_ok=True)

# NSE Browser Handshake Setup
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com/all-reports-derivatives",
})

try:
    session.get("https://www.nseindia.com", timeout=10)
    time.sleep(1)
except Exception as e:
    print(f"  ⚠️ Cookie setup warning: {e}")

# ==========================================
# LOOKBACK ENGINE LOOP
# ==========================================
current_dt = datetime.now()
INDIAN_HOLIDAYS = holidays.India(years=[current_dt.year])

max_lookback_days = 7
found_data = False

for lookback in range(max_lookback_days):
    target_dt = current_dt - timedelta(days=lookback)
    date_iso = target_dt.strftime("%Y-%m-%d")
    date_str = target_dt.strftime("%d%m%Y")
    
    # Skip weekends safely
    if target_dt.weekday() >= 5:
        if lookback == 0:
            print(f"⏭️  Skipping {date_iso}: Weekend.")
        continue
        
    # Skip official holidays safely
    if date_iso in INDIAN_HOLIDAYS:
        if lookback == 0:
            print(f"⏭️  Skipping {date_iso}: Market Holiday ({INDIAN_HOLIDAYS.get(date_iso)}).")
        continue

    print(f"🔄 Checking session availability for date: {date_iso}...")
    
    vol_filename = f"participant_vol_{date_str}.csv"
    oi_filename = f"participant_oi_{date_str}.csv"
    
    vol_path = RAW_DAILY_DIR / vol_filename
    oi_path = RAW_DAILY_DIR / oi_filename

    # If both files already exist locally, no need to redownload
    if vol_path.exists() and oi_path.exists():
        print(f"  ✅ Files already downloaded locally for {date_iso}.\n")
        found_data = True
        break

    # Attempt to download both files for this day
    session_success = True
    for file_type in ["VOL", "OI"]:
        url = f"https://archives.nseindia.com/content/nsccl/fao_participant_{file_type.lower()}_{date_str}.csv"
        filename = f"participant_{file_type.lower()}_{date_str}.csv"
        target_path = RAW_DAILY_DIR / filename
        
        print(f"  ⬇️ Fetching {file_type}...", end="", flush=True)
        try:
            r = session.get(url, timeout=12)
            if r.status_code == 200 and ("client type" in r.text.lower() or "participant" in r.text.lower()):
                with open(target_path, "wb") as f:
                    f.write(r.content)
                print(" -> DONE")
                time.sleep(random.uniform(0.4, 0.8))
            else:
                print(f" -> 404 NOT FOUND")
                session_success = False
                break # Break inner loop, try previous calendar day
        except Exception as e:
            print(f" -> ERROR: {e}")
            session_success = False
            break

    if session_success:
        print(f"\n🎉 [SUCCESS] Successfully secured data files for trading date: {date_iso}\n")
        found_data = True
        break
    else:
        # If files were missing, delete any partial download to keep directory clean
        if vol_path.exists(): vol_path.unlink()
        if oi_path.exists(): oi_path.unlink()
        print(f"  ❌ {date_iso} is unavailable. Rolling back to previous date...\n")

if not found_data:
    print(f"🛑 [ABORTED] Tried past {max_lookback_days} days. No active data packages available.")