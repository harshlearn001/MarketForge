#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | EQUITY STOCK MASTER BUILDER (CSV ONLY)

✔ Uses CLEANED equity_daily files
✔ SERIES = EQ only
✔ CSV ONLY (Parquet removed)
✔ Per-symbol master CSV
✔ Append-safe & idempotent
✔ DATE dtype hardened
✔ Production safe
"""

from pathlib import Path
import pandas as pd
import sys

# ==================================================
# PATHS
# ==================================================
ROOT = Path(r"H:\MarketForge")

IN_DIR = ROOT / "data" / "processed" / "equity_daily"
OUT_DIR = ROOT / "data" / "master" / "Equity_stock_master"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ==================================================
# REMOVE ANY EXISTING PARQUET FILES (HARD POLICY)
# ==================================================
for p in OUT_DIR.glob("*.parquet"):
    p.unlink()
    print(f"Removed parquet file: {p.name}")

# ==================================================
# DISCOVER LATEST CLEANED EQUITY FILE
# ==================================================
files = sorted(
    IN_DIR.glob("BhavCopy_NSE_CM_*.csv"),
    key=lambda f: f.stat().st_mtime,
    reverse=True
)

if not files:
    print(" No cleaned equity files found")
    sys.exit(0)

csv_file = files[0]
print(f" Using cleaned file: {csv_file.name}")

# ==================================================
# LOAD CLEAN DATA
# ==================================================
df = pd.read_csv(csv_file)

# ==================================================
# NORMALIZE COLUMNS
# ==================================================
df.columns = (
    df.columns
      .astype(str)
      .str.strip()
      .str.upper()
)

# ==================================================
# STRICT SERIES FILTER (EQ ONLY)
# ==================================================
if "SERIES" not in df.columns:
    raise RuntimeError("SERIES column missing in cleaned equity file")

df["SERIES"] = df["SERIES"].astype(str).str.strip()
df = df[df["SERIES"] == "EQ"]

if df.empty:
    print(" No EQ rows found after filter")
    sys.exit(0)

# ==================================================
# REQUIRED COLUMNS
# ==================================================
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

missing = [c for c in KEEP_COLS if c not in df.columns]
if missing:
    raise RuntimeError(f"Missing required columns: {missing}")

df = df[KEEP_COLS]

# ==================================================
# TYPE ENFORCEMENT (GLOBAL)
# ==================================================
df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
df = df[df["DATE"].notna()]

FLOAT_COLS = [
    "OPEN", "HIGH", "LOW", "CLOSE",
    "LAST", "PREVCLOSE", "TOTTRDVAL"
]

INT_COLS = ["TOTTRDQTY", "TOTALTRADES"]

for c in FLOAT_COLS:
    df[c] = pd.to_numeric(df[c], errors="coerce")

for c in INT_COLS:
    df[c] = (
        pd.to_numeric(df[c], errors="coerce")
        .fillna(0)
        .astype("int64")
    )

df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip()

print(f" EQ rows        : {len(df)}")
print(f" Symbols found  : {df['SYMBOL'].nunique()}")

# ==================================================
# APPEND PER SYMBOL (CSV ONLY, DATE SAFE)
# ==================================================
for symbol, g in df.groupby("SYMBOL"):
    g = g.sort_values("DATE")

    csv_out = OUT_DIR / f"{symbol}.csv"

    if csv_out.exists():
        old = pd.read_csv(csv_out)

        merged = pd.concat([old, g], ignore_index=True)

        #  HARD DATE STANDARD (CRITICAL FIX)
        merged["DATE"] = pd.to_datetime(
            merged["DATE"], errors="coerce"
        )

        merged = (
            merged
                .drop_duplicates(subset=["DATE"], keep="last")
                .sort_values("DATE")
        )
    else:
        merged = g.copy()
        merged["DATE"] = pd.to_datetime(
            merged["DATE"], errors="coerce"
        )

    merged.to_csv(csv_out, index=False)

print("\n EQUITY STOCK MASTER UPDATED (CSV ONLY)")
print(f" Output path: {OUT_DIR}")
