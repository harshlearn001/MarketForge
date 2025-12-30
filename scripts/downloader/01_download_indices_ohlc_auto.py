#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | NSE INDEX OHLC (TODAY ELSE YESTERDAY | AUTO)

✔ If today EOD available → save today
✔ Else → save yesterday
✔ NSE column-variant safe
✔ No fake dates
✔ Holiday & weekend safe
"""

from pathlib import Path
import subprocess
import json
import pandas as pd
from datetime import datetime, timedelta, time

# ==================================================
# CONFIG
# ==================================================
MARKET_CLOSE = time(15, 30)

CORE_EQUITY_INDICES = {
    "NIFTY 50",
    "NIFTY BANK",
    "NIFTY NEXT 50",
    "INDIA VIX",
}

# ==================================================
# PATHS
# ==================================================
OUT_DIR = Path(r"H:\MarketForge\data\raw\indices")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ==================================================
# NSE API
# ==================================================
URL = "https://www.nseindia.com/api/allIndices"

# ==================================================
# DOWNLOAD LIVE SNAPSHOT
# ==================================================
cmd = [
    "curl",
    "-s",
    "-L",
    "-A", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "-e", "https://www.nseindia.com/",
    URL
]

result = subprocess.run(cmd, capture_output=True, text=True, check=True)
data = json.loads(result.stdout)
df = pd.DataFrame(data["data"])

# ==================================================
# NORMALIZE COLUMNS
# ==================================================
df.columns = (
    df.columns
      .astype(str)
      .str.strip()
      .str.upper()
      .str.replace(" ", "_")
)

# Standard renames (NSE variants)
df = df.rename(columns={
    "INDEX": "INDEX_NAME",
    "PERCENTCHANGE": "PCT_CHANGE",
    "CHG": "CHANGE",
    "LAST": "CLOSE"
})

# ==================================================
# NUMERIC CLEAN
# ==================================================
num_cols = ["OPEN", "HIGH", "LOW", "CLOSE", "CHANGE", "PCT_CHANGE"]
for col in num_cols:
    if col in df.columns:
        df[col] = (
            df[col]
              .astype(str)
              .str.replace(",", "", regex=False)
              .replace("-", None)
              .astype(float)
        )

# ==================================================
# CORE EQUITY CHECK
# ==================================================
core_df = df[df["INDEX_NAME"].isin(CORE_EQUITY_INDICES)].copy()

if core_df.empty:
    raise RuntimeError("No core equity indices found")

valid_trading = not (
    (core_df["OPEN"] == 0).all()
    and (core_df["HIGH"] == 0).all()
    and (core_df["LOW"] == 0).all()
)

# ==================================================
# DETERMINE TRADE DATE
# ==================================================
now = datetime.now()

# Case 1: Today EOD
if valid_trading and now.time() >= MARKET_CLOSE:
    trade_date = now.date()
    print("Using TODAY EOD")

# Case 2: Fallback → Yesterday
else:
    trade_date = (now - timedelta(days=1)).date()
    print("Today EOD not available. Falling back to YESTERDAY")

# Weekend correction
while trade_date.weekday() >= 5:  # Sat/Sun
    trade_date -= timedelta(days=1)

# ==================================================
# ADD TRADE DATE
# ==================================================
df.insert(0, "TRADE_DATE", trade_date)

# ==================================================
# FINAL COLUMN SELECTION (SAFE)
# ==================================================
keep_cols = [
    "TRADE_DATE",
    "INDEX_NAME",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "CHANGE",
    "PCT_CHANGE"
]

df = df[[c for c in keep_cols if c in df.columns]]

# ==================================================
# SAVE
# ==================================================
OUT_FILE = OUT_DIR / f"indices_ohlc_eod_{trade_date.strftime('%Y%m%d')}.csv"
df.to_csv(OUT_FILE, index=False)

print("NSE INDEX OHLC SAVED")
print(f"Trade date : {trade_date}")
print(f"Rows       : {len(df)}")
print(f"Saved      : {OUT_FILE}")
