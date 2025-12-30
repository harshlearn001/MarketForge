#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | EQUITY STOCK MASTER BUILDER (FINAL, STABLE)

✔ Reads cleaned NSE CM Bhavcopy
✔ Equity only (CM + STK + EQ/BE)
✔ Excludes ETF / GB / SGB / junk
✔ One master CSV + Parquet per symbol
✔ Append-safe & idempotent
✔ Date-safe (no warnings)
✔ Production hardened
"""

from pathlib import Path
import pandas as pd
import sys

# ==================================================
# PATHS
# ==================================================
ROOT = Path(r"H:\MarketForge")

IN_DIR = ROOT / "data" / "processed" / "equity_daily"
OUT_DIR = ROOT / "data" / "master" / "Equity_stock_master" / "STOCKS"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ==================================================
# DISCOVER LATEST CLEANED BHAVCOPY
# ==================================================
files = sorted(
    IN_DIR.glob("BhavCopy_NSE_CM_*.csv"),
    key=lambda f: f.stat().st_mtime,
    reverse=True
)

if not files:
    print(" No processed equity bhavcopy found")
    sys.exit(0)

csv_file = files[0]
print(f" Using bhavcopy: {csv_file.name}")

# ==================================================
# LOAD DATA
# ==================================================
df = pd.read_csv(csv_file, low_memory=False)

# ==================================================
# NORMALIZE COLUMN NAMES
# ==================================================
df.columns = (
    df.columns
      .astype(str)
      .str.strip()
      .str.upper()
)

# ==================================================
# FILTER — EQUITY ONLY (CRITICAL)
# ==================================================
df = df[
    (df["SGMT"] == "CM") &
    (df["FININSTRMTP"] == "STK") &
    (df["SCTYSRS"].isin({"EQ", "BE"}))
]

# ==================================================
# RENAME TO MASTER SCHEMA
# ==================================================
df = df.rename(columns={
    "TCKRSYMB": "SYMBOL",
    "SCTYSRS": "SERIES",
    "OPNPRIC": "OPEN",
    "HGHPRIC": "HIGH",
    "LWPRIC": "LOW",
    "CLSPRIC": "CLOSE",
    "LASTPRIC": "LAST",
    "PRVSCLSGPRIC": "PREVCLOSE",
    "TTLTRADGVOL": "TOTTRDQTY",
    "TTLTRFVAL": "TOTTRDVAL",
    "TTLNBOFTXSEXCTD": "TOTALTRADES",
})

KEEP_COLS = [
    "DATE",
    "SYMBOL",
    "SERIES",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "LAST",
    "PREVCLOSE",
    "TOTTRDQTY",
    "TOTTRDVAL",
    "TOTALTRADES",
    "ISIN",
]

df = df[KEEP_COLS]

# ==================================================
# TYPE CLEANING (FINAL, WARNING-FREE)
# ==================================================
df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

FLOAT_COLS = [
    "OPEN", "HIGH", "LOW",
    "CLOSE", "LAST", "PREVCLOSE", "TOTTRDVAL"
]

INT_COLS = [
    "TOTTRDQTY", "TOTALTRADES"
]

df[FLOAT_COLS] = df[FLOAT_COLS].apply(
    pd.to_numeric, errors="coerce"
).astype("float32")

df[INT_COLS] = (
    df[INT_COLS]
    .apply(pd.to_numeric, errors="coerce")
    .fillna(0)
    .astype("int64")
)

df = df.dropna(subset=["SYMBOL", "DATE"])

if df.empty:
    print(" No equity rows after filter — aborting")
    sys.exit(0)

print(f" Equity rows after filter: {len(df)}")
print(f" Symbols found: {df['SYMBOL'].nunique()}")

# ==================================================
# APPEND PER-SYMBOL MASTER (IDEMPOTENT)
# ==================================================
for symbol, g in df.groupby("SYMBOL"):
    g = g.sort_values("DATE")

    csv_out = OUT_DIR / f"{symbol}.csv"
    pq_out  = OUT_DIR / f"{symbol}.parquet"

    if csv_out.exists():
        old = pd.read_csv(csv_out, parse_dates=["DATE"])
        merged = (
            pd.concat([old, g], ignore_index=True)
            .drop_duplicates(subset=["DATE"], keep="last")
            .sort_values("DATE")
        )
    else:
        merged = g

    merged.to_csv(csv_out, index=False)
    merged.to_parquet(pq_out, index=False)

# ==================================================
# DONE
# ==================================================
print("\nEQUITY STOCK MASTER UPDATED SUCCESSFULLY")
print(f"Output path: {OUT_DIR}")
