#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | NSE EQUITY DAILY CLEANER (CM BHAVCOPY — EQ ONLY)

✔ NEW + OLD NSE CM schema
✔ STRICT SERIES = EQ
✔ Explicit DATE / INT / FLOAT standards
✔ Production safe
"""

from pathlib import Path
import pandas as pd

# =================================================
# PATHS
# =================================================
ROOT = Path(__file__).resolve().parents[2]

SRC_DIR = ROOT / "data" / "unzip_daily" / "equty_daily_unzip"
OUT_DIR = ROOT / "data" / "processed" / "equity_daily"
OUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"Scanning source dir : {SRC_DIR}")

# =================================================
# STANDARD OUTPUT SCHEMA
# =================================================
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

# =================================================
# NSE → STANDARD COLUMN MAP
# =================================================
COL_MAP = {
    "TCKRSYMB": "SYMBOL",
    "SCTYSRS": "SERIES",
    "ISIN": "ISIN",

    "OPNPRIC": "OPEN",
    "HGHPRIC": "HIGH",
    "LWPRIC": "LOW",
    "CLSPRIC": "CLOSE",
    "LASTPRIC": "LAST",
    "PRVSCLSGPRIC": "PREVCLOSE",

    "TTLTRADGVOL": "TOTTRDQTY",
    "TTLTRFVAL": "TOTTRDVAL",
    "TTLNBOFTXSEXCTD": "TOTALTRADES",

    # old schema safety
    "OPEN": "OPEN",
    "HIGH": "HIGH",
    "LOW": "LOW",
    "CLOSE": "CLOSE",
    "LAST": "LAST",
    "PREVCLOSE": "PREVCLOSE",
    "TOTTRDQTY": "TOTTRDQTY",
    "TOTTRDVAL": "TOTTRDVAL",
    "TOTALTRADES": "TOTALTRADES",
    "SYMBOL": "SYMBOL",
    "SERIES": "SERIES",
}

# =================================================
# DISCOVER FILES
# =================================================
files = list(SRC_DIR.glob("BhavCopy_NSE_CM*.csv"))
print(f"Found {len(files)} equity CSV files")

if not files:
    raise RuntimeError("No equity CSV files found")

# =================================================
# CLEAN LOOP
# =================================================
for file in files:
    print(f"\nCleaning: {file.name}")

    df = pd.read_csv(file, low_memory=False)

    # ---------------------------------
    # NORMALIZE HEADERS
    # ---------------------------------
    df.columns = (
        df.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
        .str.upper()
    )

    # ---------------------------------
    # DATE (STRICT)
    # ---------------------------------
    if "TRADDT" in df.columns:
        df["DATE"] = pd.to_datetime(df["TRADDT"], format="%Y-%m-%d", errors="coerce")
    elif "BIZDT" in df.columns:
        df["DATE"] = pd.to_datetime(df["BIZDT"], format="%Y-%m-%d", errors="coerce")
    elif "TIMESTAMP" in df.columns:
        df["DATE"] = pd.to_datetime(df["TIMESTAMP"], format="%d-%b-%Y", errors="coerce")
    else:
        raise RuntimeError(f"No DATE column found in {file.name}")

    df = df[df["DATE"].notna()]
    df["DATE"] = df["DATE"].dt.date

    # ---------------------------------
    # RENAME TO STANDARD
    # ---------------------------------
    df = df.rename(columns=COL_MAP)

    # ---------------------------------
    #  STRICT SERIES FILTER (EQ ONLY)
    # ---------------------------------
    if "SERIES" not in df.columns:
        raise RuntimeError(f"SERIES column missing in {file.name}")

    df["SERIES"] = df["SERIES"].astype(str).str.strip()
    df = df[df["SERIES"] == "EQ"]

    if df.empty:
        print(" No EQ series rows found, file skipped")
        continue

    # ---------------------------------
    # TEXT COLUMNS
    # ---------------------------------
    for col in ("SYMBOL", "ISIN"):
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # ---------------------------------
    # FLOAT64 COLUMNS
    # ---------------------------------
    FLOAT_COLS = {
        "OPEN", "HIGH", "LOW", "CLOSE",
        "LAST", "PREVCLOSE", "TOTTRDVAL"
    }

    for col in FLOAT_COLS & set(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # ---------------------------------
    # INT64 COLUMNS
    # ---------------------------------
    INT_COLS = {"TOTTRDQTY", "TOTALTRADES"}

    for col in INT_COLS & set(df.columns):
        df[col] = (
            pd.to_numeric(df[col], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

    # ---------------------------------
    # STRICT COLUMN GATE
    # ---------------------------------
    df = df[[c for c in KEEP_COLS if c in df.columns]]

    # ---------------------------------
    # SORT + SAVE
    # ---------------------------------
    df = df.sort_values("DATE")

    out_file = OUT_DIR / file.name
    df.to_csv(out_file, index=False)

    print(f"Saved EQ-only standardized file → {out_file}")

print("\nEQUITY DAILY CLEANING COMPLETED (EQ SERIES ONLY)")
