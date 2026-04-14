#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | NSE INDEX OHLC (TODAY ELSE YESTERDAY | AUTO)

✔ Uses native Python requests (no curl dependency)
✔ If today EOD available → save today
✔ Else → save previous trading day
✔ NSE column-variant safe
✔ No fake dates
✔ Holiday & weekend safe
"""

from datetime import datetime, timedelta, time
from pathlib import Path
import re
import pandas as pd
import requests

# ==================================================
# CONFIG
# ==================================================
MARKET_CLOSE = time(15, 30)
ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "raw" / "indices"
OUT_DIR.mkdir(parents=True, exist_ok=True)
EQUITY_RAW_DIR = ROOT / "data" / "raw" / "equity"
FUTURES_RAW_DIR = ROOT / "data" / "raw" / "futures"
MTO_RAW_DIR = ROOT / "data" / "raw" / "equityDat"

CORE_EQUITY_INDICES = {
    "NIFTY 50",
    "NIFTY BANK",
    "NIFTY NEXT 50",
    "INDIA VIX",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

URL = "https://www.nseindia.com/api/allIndices"


def fetch_indices() -> pd.DataFrame:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.get("https://www.nseindia.com/", timeout=10)

    response = session.get(URL, timeout=30)
    response.raise_for_status()

    payload = response.json()
    data = payload.get("data", [])
    if not data:
        raise RuntimeError("NSE allIndices API returned no rows")

    return pd.DataFrame(data)


def previous_trading_day(day):
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def latest_date_from_files(folder: Path, pattern: str, date_format: str):
    dates = []
    for path in folder.glob(pattern):
        match = re.search(r"(\d{8})", path.name)
        if not match:
            continue
        try:
            dates.append(datetime.strptime(match.group(1), date_format).date())
        except ValueError:
            continue
    return max(dates) if dates else None


def latest_fully_published_trade_date():
    equity_date = latest_date_from_files(
        EQUITY_RAW_DIR,
        "BhavCopy_NSE_CM_*.zip",
        "%Y%m%d",
    )
    futures_date = latest_date_from_files(
        FUTURES_RAW_DIR,
        "fo*.zip",
        "%d%m%Y",
    )
    mto_date = latest_date_from_files(
        MTO_RAW_DIR,
        "MTO_*.DAT",
        "%d%m%Y",
    )

    available = [d for d in [equity_date, futures_date, mto_date] if d is not None]
    if len(available) < 3:
        return None

    return min(available)


df = fetch_indices()

# ==================================================
# NORMALIZE COLUMNS
# ==================================================
df.columns = (
    df.columns.astype(str)
    .str.strip()
    .str.upper()
    .str.replace(" ", "_")
)

df = df.rename(
    columns={
        "INDEX": "INDEX_NAME",
        "PERCENTCHANGE": "PCT_CHANGE",
        "CHG": "CHANGE",
        "LAST": "CLOSE",
    }
)

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
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ==================================================
# CORE EQUITY CHECK
# ==================================================
if "INDEX_NAME" not in df.columns:
    raise RuntimeError("INDEX_NAME column missing from NSE allIndices payload")

core_df = df[df["INDEX_NAME"].isin(CORE_EQUITY_INDICES)].copy()
if core_df.empty:
    raise RuntimeError("No core equity indices found")

valid_trading = not (
    (core_df["OPEN"].fillna(0) == 0).all()
    and (core_df["HIGH"].fillna(0) == 0).all()
    and (core_df["LOW"].fillna(0) == 0).all()
)

# ==================================================
# DETERMINE TRADE DATE
# ==================================================
now = datetime.now()
if valid_trading and now.time() >= MARKET_CLOSE:
    trade_date = now.date()
    print("Using TODAY EOD")
else:
    trade_date = previous_trading_day((now - timedelta(days=1)).date())
    print("Today EOD not available. Falling back to previous trading day")

published_trade_date = latest_fully_published_trade_date()
if published_trade_date and trade_date > published_trade_date:
    trade_date = published_trade_date
    print(f"Aligning indices to latest fully published market date: {trade_date}")

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
    "PCT_CHANGE",
]

df = df[[c for c in keep_cols if c in df.columns]]

# ==================================================
# SAVE
# ==================================================
out_file = OUT_DIR / f"indices_ohlc_eod_{trade_date.strftime('%Y%m%d')}.csv"
df.to_csv(out_file, index=False)

print("NSE INDEX OHLC SAVED")
print(f"Trade date : {trade_date}")
print(f"Rows       : {len(df)}")
print(f"Saved      : {out_file}")
