#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | CLEAN NSE INDEX OHLC (FINAL LOCKED)

✔ TRADE_DATE → YYYYMMDD (int)
✔ Index names preserved (NSE authoritative)
✔ Numeric columns strict float64
✔ Schema-stable
✔ Zero warnings
✔ Ready for master append
"""

from pathlib import Path
import pandas as pd

# ==================================================
# PATHS
# ==================================================
RAW_DIR = Path(r"H:\MarketForge\data\raw\indices")
OUT_DIR = Path(r"H:\MarketForge\data\processed\indices_daily")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ==================================================
# PICK LATEST FILE
# ==================================================
latest_file = max(
    RAW_DIR.glob("indices_ohlc_eod_*.csv"),
    key=lambda p: p.stat().st_mtime
)

OUT_FILE = OUT_DIR / latest_file.name.replace(
    "indices_ohlc_eod_", "indices_ohlc_clean_"
)

print(f" Processing: {latest_file.name}")

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
    raise RuntimeError(" TRADE_DATE missing in index EOD file")

df["TRADE_DATE"] = (
    pd.to_datetime(df["TRADE_DATE"], errors="coerce")
    .dt.strftime("%Y%m%d")
    .astype("int64")
)

df = df[df["TRADE_DATE"].notna()]

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
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

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

print("\n INDEX OHLC CLEAN COMPLETED (LOCKED)")
print(f" Raw file   : {latest_file.name}")
print(f" Trade date : {df['TRADE_DATE'].iloc[0]}")
print(f" Rows       : {len(df)}")
print(f" Saved      : {OUT_FILE}")
