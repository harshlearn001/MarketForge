#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | CLEAN NSE INDEX OHLC (FILTERED + FINAL)

✔ TRADE_DATE → YYYYMMDD (int)
✔ Index names preserved (NSE authoritative)
✔ Numeric columns strict float64
✔ Only selected indices processed (CONTROLLED)
✔ Schema-stable
✔ Zero warnings
✔ Ready for master append
"""

from pathlib import Path
from datetime import datetime
import re
import pandas as pd

# ==================================================
# PATHS
# ==================================================
RAW_DIR = Path(r"H:\MarketForge\data\raw\indices")
OUT_DIR = Path(r"H:\MarketForge\data\processed\indices_daily")
OUT_DIR.mkdir(parents=True, exist_ok=True)
ROOT = Path(r"H:\MarketForge")
EQUITY_RAW_DIR = ROOT / "data" / "raw" / "equity"
FUTURES_RAW_DIR = ROOT / "data" / "raw" / "futures"
MTO_RAW_DIR = ROOT / "data" / "raw" / "equityDat"

# ==================================================
# TARGET INDICES (CONTROL PANEL)
# ==================================================
TARGET_INDICES = {
    # CORE
    "NIFTY 50",
    "NIFTY BANK",
    "NIFTY NEXT 50",
    "NIFTY 100",
    "NIFTY 200",
    "NIFTY 500",

    # SECTOR
    "NIFTY IT",
    "NIFTY FMCG",
    "NIFTY AUTO",
    "NIFTY METAL",
    "NIFTY PHARMA",
    "NIFTY REALTY",
    "NIFTY ENERGY",

    # FACTOR
    "NIFTY ALPHA 50",
    "NIFTY LOW VOLATILITY 50",

    # MACRO
    "INDIA VIX",
}


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

# ==================================================
# PICK LATEST FILE
# ==================================================
raw_files = list(RAW_DIR.glob("indices_ohlc_eod_*.csv"))
if not raw_files:
    raise RuntimeError("No raw index OHLC files found")

published_trade_date = latest_fully_published_trade_date()
if published_trade_date is not None:
    raw_files = [
        p for p in raw_files
        if re.search(r"(\d{8})", p.name)
        and datetime.strptime(re.search(r"(\d{8})", p.name).group(1), "%Y%m%d").date() <= published_trade_date
    ]

if not raw_files:
    raise RuntimeError("No aligned raw index OHLC files found")

latest_file = max(
    raw_files,
    key=lambda p: datetime.strptime(re.search(r"(\d{8})", p.name).group(1), "%Y%m%d")
)

OUT_FILE = OUT_DIR / latest_file.name.replace(
    "indices_ohlc_eod_", "indices_ohlc_clean_"
)

if OUT_FILE.exists():
    print(f"📊 Processing: {latest_file.name}")
    print(f" Already cleaned, skipping → {OUT_FILE.name}")
    raise SystemExit(0)

print(f"📊 Processing: {latest_file.name}")

# ==================================================
# LOAD
# ==================================================
df = pd.read_csv(latest_file, low_memory=False)

# ==================================================
# COLUMN NORMALIZATION
# ==================================================
df.columns = (
    df.columns
      .astype(str)
      .str.strip()
      .str.upper()
)

df = df.rename(columns={
    "INDEX": "INDEX_NAME",
    "PERCENTCHANGE": "PCT_CHANGE",
    "LAST": "CLOSE",
})

# ==================================================
# TRADE_DATE STANDARDIZATION (YYYYMMDD INT)
# ==================================================
if "TRADE_DATE" not in df.columns:
    raise RuntimeError("❌ TRADE_DATE missing in index EOD file")

df["TRADE_DATE"] = pd.to_datetime(
    df["TRADE_DATE"], errors="coerce"
)

df = df[df["TRADE_DATE"].notna()]

df["TRADE_DATE"] = (
    df["TRADE_DATE"]
    .dt.strftime("%Y%m%d")
    .astype("int64")
)

# ==================================================
# INDEX NAME CLEAN
# ==================================================
df["INDEX_NAME"] = (
    df["INDEX_NAME"]
    .astype(str)
    .str.strip()
)

df = df[df["INDEX_NAME"] != ""]

# ==================================================
# FILTER REQUIRED INDICES (🔥 KEY STEP)
# ==================================================
df = df[df["INDEX_NAME"].isin(TARGET_INDICES)]

if df.empty:
    raise RuntimeError("❌ No matching indices found after filtering")

# ==================================================
# NUMERIC STANDARDIZATION
# ==================================================
FLOAT_COLS = ["OPEN", "HIGH", "LOW", "CLOSE", "PCT_CHANGE", "CHANGE"]

for col in FLOAT_COLS:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
        )
        df[col] = pd.to_numeric(
            df[col], errors="coerce"
        ).astype("float64")

# ==================================================
# FINAL COLUMN ORDER
# ==================================================
FINAL_COLS = [
    "TRADE_DATE",
    "INDEX_NAME",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "PCT_CHANGE",
]

# keep CHANGE only if exists
if "CHANGE" in df.columns:
    FINAL_COLS.insert(-1, "CHANGE")

df = df[FINAL_COLS]

# ==================================================
# SORT & SAVE
# ==================================================
df = df.sort_values("INDEX_NAME").reset_index(drop=True)

df.to_csv(OUT_FILE, index=False)

# ==================================================
# DEBUG INFO
# ==================================================
available = set(df["INDEX_NAME"].unique())
missing = TARGET_INDICES - available

print("\n✅ INDEX OHLC CLEAN COMPLETED (FILTERED)")
print(f"📁 Raw file   : {latest_file.name}")
print(f"📅 Trade date : {df['TRADE_DATE'].iloc[0]}")
print(f"📊 Rows       : {len(df)}")
print(f"💾 Saved      : {OUT_FILE}")

if missing:
    print("\n⚠ Missing indices:")
    for m in sorted(missing):
        print(" -", m)
