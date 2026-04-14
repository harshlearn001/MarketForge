#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge PRO
FII / DII TRADING ACTIVITY DOWNLOADER (DATA ONLY - FINAL)

✔ Handles NSE blocking
✔ Handles list/dict response
✔ Cleans date format
✔ Uses TRADING DATE (not system date)
✔ Numeric cleaning
✔ Production ready
"""

import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

print("📡 DOWNLOADING FII/DII ACTIVITY...\n")

# ==============================
# PATH CONFIG
# ==============================
RAW_DIR = Path(r"H:\MarketForge\data\raw\fii_dii")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ==============================
# NSE SESSION (ANTI-BLOCK)
# ==============================
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/reports/fii-dii"
}

session = requests.Session()
session.headers.update(headers)

# Step 1: Get cookies
try:
    session.get("https://www.nseindia.com", timeout=10)
except Exception as e:
    print("⚠️ Cookie setup warning:", e)

# ==============================
# API CALL
# ==============================
url = "https://www.nseindia.com/api/fiiDiiTradeReact"

try:
    response = session.get(url, timeout=10)
    response.raise_for_status()
    json_data = response.json()

    # Handle both response types
    if isinstance(json_data, dict):
        records = json_data.get("data", [])
    elif isinstance(json_data, list):
        records = json_data
    else:
        print("❌ Unknown response format")
        exit()

    if not records:
        print("❌ No data received")
        exit()

    df = pd.DataFrame(records)

except Exception as e:
    print("❌ Download failed:", e)
    exit()

# ==============================
# RENAME COLUMNS
# ==============================
df = df.rename(columns={
    "buyValue": "buy",
    "sellValue": "sell",
    "netValue": "net",
    "date": "date",
    "category": "participant"
})

# ==============================
# CLEAN NUMERIC DATA
# ==============================
for col in ["buy", "sell", "net"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

# ==============================
# FIX DATE FORMAT
# ==============================
if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

# ==============================
# NORMALIZE PARTICIPANT
# ==============================
if "participant" in df.columns:
    df["participant"] = df["participant"].str.upper()
    df["participant"] = df["participant"].replace({
        "FII/FPI": "FII"
    })

# ==============================
# REMOVE BAD ROWS
# ==============================
df = df.dropna(subset=["date", "net"])

# ==============================
# SORT
# ==============================
df = df.sort_values(["date", "participant"])

# ==============================
# USE TRADING DATE FOR FILENAME
# ==============================
if "date" in df.columns and not df.empty:
    trade_date = df["date"].min()
else:
    trade_date = datetime.now().strftime("%Y-%m-%d")

file_path = RAW_DIR / f"fii_dii_raw_{trade_date}.csv"

# ==============================
# SAVE
# ==============================
df.to_csv(file_path, index=False)

print(f"✅ Saved → {file_path}")

# ==============================
# PREVIEW
# ==============================
print("\n📊 DATA PREVIEW:\n")
print(df)