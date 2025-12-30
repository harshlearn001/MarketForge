#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | NSE INDEX OHLC (EOD | ASK DATE)

✔ Ask user for trade date
✔ Blank = today (after 15:30 IST only)
✔ Equity-truth date logic
✔ Holiday-safe
✔ No intraday pollution
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
# ASK DATE
# ==================================================
user_date = input(
    "Enter EOD trade date (DD-MM-YYYY) or press ENTER for today: "
).strip()

if user_date:
    try:
        trade_date = datetime.strptime(user_date, "%d-%m-%Y").date()
        manual_mode = True
    except ValueError:
        raise ValueError("Invalid date format. Use DD-MM-YYYY")
else:
    trade_date = None
    manual_mode = False

# ==================================================
# TIME GATE (ONLY IF AUTO MODE)
# ==================================================
now = datetime.now()

if not manual_mode and now.time() < MARKET_CLOSE:
    raise RuntimeError(
        f"Market not closed yet ({now.time()}). "
        "EOD data available only after 15:30 IST."
    )

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
# DOWNLOAD (curl = NSE safe)
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
# NORMALIZE
# ==================================================
df.columns = (
    df.columns
      .astype(str)
      .str.strip()
      .str.upper()
      .str.replace(" ", "_")
)

df = df.rename(columns={
    "INDEX": "INDEX_NAME",
    "PERCENTCHANGE": "PCT_CHANGE"
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
# CORE EQUITY VALIDATION
# ==================================================
core_df = df[df["INDEX_NAME"].isin(CORE_EQUITY_INDICES)].copy()

if core_df.empty:
    raise RuntimeError("No core equity indices found")

# Holiday detection
if (
    (core_df["OPEN"] == 0).all()
    and (core_df["HIGH"] == 0).all()
    and (core_df["LOW"] == 0).all()
):
    raise RuntimeError("Equity market closed (holiday)")

# ==================================================
# DETERMINE TRUE TRADE DATE
# ==================================================
if not manual_mode:
    # Prefer NSE timestamp from core indices
    detected_date = None
    for col in df.columns:
        if "time" in col.lower():
            try:
                ts = core_df[col].dropna().iloc[0]
                detected_date = pd.to_datetime(ts, dayfirst=True).date()
                break
            except Exception:
                pass

    if detected_date:
        trade_date = detected_date
    else:
        trade_date = now.date()

# ==================================================
# ADD TRADE DATE
# ==================================================
df.insert(0, "TRADE_DATE", trade_date)

# ==================================================
# FINAL COLUMNS
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

print("NSE INDEX OHLC EOD SAVED")
print(f"Trade date : {trade_date}")
print(f"Rows       : {len(df)}")
print(f"Saved      : {OUT_FILE}")
