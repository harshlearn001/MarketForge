#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge PRO
Participant Historical Data Downloader (v7.1 - Reverse Scan Edition)

Requirements:
    pip install requests holidays
"""

import os
import json
import time
import random
import requests
from pathlib import Path
from datetime import datetime, timedelta
import holidays

print("⚙️ INITIALIZING MARKETFORGE REVERSE HISTORICAL DOWNLOADER (v7.1)...\n")

# ==========================================
# CONFIGURATION & PATHS
# ==========================================
RAW_DIR = Path(r"H:\MarketForge\data\raw\participant_historical")
RAW_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE = RAW_DIR / "download_state.json"

# Setting this to 2018 since that is roughly when public web records start
START_YEAR = 2018 
END_DATE = datetime.now()

INDIAN_HOLIDAYS = holidays.India(years=list(range(START_YEAR, END_DATE.year + 1)))

# ==========================================
# STATE MANAGEMENT
# ==========================================
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_state(processed_dates: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(processed_dates, f, indent=4)

# ==========================================
# REVERSE TIMELINE GENERATOR
# ==========================================
def generate_reverse_trading_days(start_year: int, end_dt: datetime) -> list:
    """Generates trading days moving BACKWARDS from today down to start_year."""
    start_dt = datetime(start_year, 1, 1)
    current_dt = end_dt
    trading_days = []

    print("📅 Constructing reverse timeline framework...")
    while current_dt >= start_dt:
        # Filter Weekends
        if current_dt.weekday() >= 5:
            current_dt -= timedelta(days=1)
            continue
            
        # Filter Holidays
        if current_dt.strftime("%Y-%m-%d") in INDIAN_HOLIDAYS:
            current_dt -= timedelta(days=1)
            continue

        trading_days.append(current_dt)
        current_dt -= timedelta(days=1)
        
    return trading_days

# ==========================================
# BROWSER SIMULATOR
# ==========================================
def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent"      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept"          : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language" : "en-US,en;q=0.9",
        "Accept-Encoding" : "gzip, deflate, br",
        "Connection"      : "keep-alive",
        "Referer"         : "https://www.nseindia.com/all-reports-derivatives",
    })
    
    print("🔐 Securing initial session cookies from NSE...")
    try:
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(2)
        session.get("https://www.nseindia.com/all-reports-derivatives", timeout=15)
        time.sleep(1)
    except Exception as e:
        print(f"   ⚠️  Initial handshake warning: {e}")
    return session

def download_file(session: requests.Session, url: str, target_path: Path) -> str:
    if target_path.exists() and target_path.stat().st_size > 500:
        return "EXISTS"

    base_backoff = 3
    for attempt in range(1, 4):
        try:
            r = session.get(url, timeout=15)
            
            if r.status_code == 200:
                content_sample = r.content[:500].decode(errors="ignore").lower()
                if "client type" in content_sample or "participant" in content_sample:
                    with open(target_path, "wb") as f:
                        f.write(r.content)
                    return "DOWNLOADED"
                else:
                    return "EMPTY_OR_INVALID"

            elif r.status_code == 404:
                return "404_NOT_FOUND"

            elif r.status_code in [403, 429]:
                sleep_time = base_backoff * attempt * 2
                print(f"\n   ⚠️ Rate-limited ({r.status_code}). Waiting {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                time.sleep(base_backoff)

        except Exception:
            time.sleep(base_backoff * attempt)

    return "FAILED"

# ==========================================
# ENGINE
# ==========================================
def main():
    session = build_session()
    state = load_state()
    
    # Notice this is now reversed!
    timeline = generate_reverse_trading_days(START_YEAR, END_DATE)
    total_days = len(timeline)
    
    print(f"🎯 Found {total_days} potential trading dates within target frame.")
    print(f"📊 Tracking state registry contains: {len(state)} items.\n")

    success_count = 0
    skipped_count = 0
    missing_count = 0
    consecutive_404s = 0

    for index, dt in enumerate(timeline, 1):
        date_key = dt.strftime("%Y-%m-%d")
        date_str = dt.strftime("%d%m%Y")
        
        if state.get(date_key) in ["COMPLETE", "NO_DATA"]:
            skipped_count += 2
            continue

        print(f"🔄 [{index}/{total_days}] Processing Date: {date_key} -> ", end="", flush=True)

        statuses = {}
        for file_type in ["VOL", "OI"]:
            url = f"https://archives.nseindia.com/content/nsccl/fao_participant_{file_type.lower()}_{date_str}.csv"
            filename = f"participant_{file_type.lower()}_{date_str}.csv"
            target_path = RAW_DIR / filename

            result = download_file(session, url, target_path)
            statuses[file_type] = result

            if result == "DOWNLOADED":
                success_count += 1
                time.sleep(random.uniform(0.6, 1.2))
            elif result == "EXISTS":
                skipped_count += 1
            elif result == "404_NOT_FOUND":
                missing_count += 1

        print(f"VOL: {statuses['VOL']} | OI: {statuses['OI']}")

        # Keep tracking state
        if statuses["VOL"] in ["DOWNLOADED", "EXISTS"] or statuses["OI"] in ["DOWNLOADED", "EXISTS"]:
            state[date_key] = "COMPLETE"
            consecutive_404s = 0  # Reset counter if data exists
        else:
            state[date_key] = "NO_DATA"
            consecutive_404s += 1

        # 🚨 THE CIRCUIT BREAKER
        # If we hit 30 consecutive trading days of pure 404s going backward, we found the archive wall!
        if consecutive_404s >= 30:
            print(f"\n🛑 [CIRCUIT BREAKER] Hit 30 consecutive days of 404s near {date_key}.")
            print("   We have reached the historical limit of NSE's public web archive server.")
            break

        if index % 10 == 0:
            save_state(state)

    save_state(state)
    print("\n🏁 REVERSE RUN COMPLETION SUMMARY")
    print(f" ✅ Newly Saved : {success_count} | ⏭️  Skipped : {skipped_count} | 🔍 Missing : {missing_count}")

if __name__ == "__main__":
    main()