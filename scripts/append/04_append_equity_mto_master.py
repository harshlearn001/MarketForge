#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | APPEND DAILY EQUITY MTO → SYMBOLWISE MASTER (LOCKED)

✔ Appends into EXISTING symbol CSVs
✔ No new folder creation
✔ EQ only (NO N1 / NC / N4)
✔ TRADE_DATE = YYYYMMDD (int)
✔ Duplicate-safe
✔ CSV ONLY
✔ ZERO data loss
"""

from pathlib import Path
import pandas as pd
import sys

# ==================================================
# PATHS
# ==================================================
ROOT = Path(r"H:\MarketForge")

DAILY_DIR  = ROOT / "data" / "processed" / "equityDat_daily"
MASTER_DIR = ROOT / "data" / "master" / "EqiutyDat_master"

if not MASTER_DIR.exists():
    raise RuntimeError(" Master folder does not exist")

# ==================================================
# FIND LATEST DAILY FILE
# ==================================================
files = sorted(
    DAILY_DIR.glob("mto_*.csv"),
    key=lambda f: f.stat().st_mtime,
    reverse=True
)

if not files:
    print(" No daily MTO files found")
    sys.exit(0)

DAILY_FILE = files[0]
print(f"Using daily file: {DAILY_FILE.name}")

# ==================================================
# LOAD DAILY
# ==================================================
df = pd.read_csv(DAILY_FILE, low_memory=False)

FINAL_COLS = [
    "TRADE_DATE",
    "RECORD_TYPE",
    "SR_NO",
    "SYMBOL",
    "SERIES",
    "TRADED_QTY",
    "DELIVERABLE_QTY",
    "DELIVERY_PCT",
]

missing = set(FINAL_COLS) - set(df.columns)
if missing:
    raise RuntimeError(f" Daily file missing columns: {sorted(missing)}")

df = df[FINAL_COLS]

# ==================================================
# TYPE ENFORCEMENT
# ==================================================
df["TRADE_DATE"] = pd.to_numeric(df["TRADE_DATE"], errors="coerce").astype("int64")

for c in ["RECORD_TYPE", "SR_NO", "TRADED_QTY", "DELIVERABLE_QTY"]:
    df[c] = (
        pd.to_numeric(df[c], errors="coerce")
        .fillna(0)
        .astype("int64")
    )

df["DELIVERY_PCT"] = pd.to_numeric(
    df["DELIVERY_PCT"], errors="coerce"
).astype("float64")

df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
df["SERIES"] = df["SERIES"].astype(str).str.strip()

# ==================================================
# STRICT EQ ONLY
# ==================================================
df = df[df["SERIES"] == "EQ"]

if df.empty:
    print(" No EQ rows in daily MTO — nothing to append")
    sys.exit(0)

# ==================================================
# APPEND PER SYMBOL (CORRECT WAY)
# ==================================================
symbols_updated = 0

for symbol, g in df.groupby("SYMBOL"):
    out_file = MASTER_DIR / f"{symbol}.csv"

    # If symbol master doesn't exist → SKIP (no silent creation)
    if not out_file.exists():
        continue

    old = pd.read_csv(out_file, low_memory=False)

    old.columns = old.columns.str.strip().str.upper()
    old = old[FINAL_COLS]

    # Enforce types again
    old["TRADE_DATE"] = pd.to_numeric(
        old["TRADE_DATE"], errors="coerce"
    ).astype("int64")

    combined = (
        pd.concat([old, g], ignore_index=True)
        .drop_duplicates(subset=["TRADE_DATE", "SYMBOL"], keep="last")
        .sort_values("TRADE_DATE")
    )

    combined.to_csv(out_file, index=False)
    symbols_updated += 1

# ==================================================
# DONE
# ==================================================
print("\n SYMBOLWISE EQUITY MTO APPEND COMPLETED")
print(f" Master folder : {MASTER_DIR}")
print(f" Symbols updated : {symbols_updated}")
