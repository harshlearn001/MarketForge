#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CLEAN FII/DII DATA (RAW → CLEAN) [PRO VERSION]

✔ Processes ALL new raw files
✔ Skips already cleaned dates
✔ Cleans date + numeric
✔ Normalizes participant
✔ Deduplicates
✔ Safe & robust
"""

import pandas as pd
from pathlib import Path

print("🧹 CLEANING FII/DII DATA...\n")

# ==============================
# PATHS
# ==============================
RAW_DIR = Path(r"H:\MarketForge\data\raw\fii_dii")
CLEAN_DIR = Path(r"H:\MarketForge\data\processed\fii_dii\clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

# ==============================
# GET RAW FILES
# ==============================
raw_files = sorted(RAW_DIR.glob("fii_dii_raw_*.csv"))

if not raw_files:
    print("❌ No raw files found")
    exit()

# ==============================
# EXISTING CLEAN DATES
# ==============================
existing_clean_files = list(CLEAN_DIR.glob("fii_dii_clean_*.csv"))
existing_dates = set()

for f in existing_clean_files:
    try:
        date_part = f.stem.split("_")[-1]
        existing_dates.add(date_part)
    except:
        pass

print(f"📁 Existing clean dates: {len(existing_dates)}")

# ==============================
# PROCESS EACH RAW FILE
# ==============================
new_files_processed = 0

for file in raw_files:

    print(f"\n📄 Processing: {file.name}")

    df = pd.read_csv(file)

    # ==========================
    # CLEAN DATE
    # ==========================
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # ==========================
    # CLEAN NUMBERS
    # ==========================
    for col in ["buy", "sell", "net"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # ==========================
    # NORMALIZE PARTICIPANT
    # ==========================
    df["participant"] = df["participant"].str.upper()
    df["participant"] = df["participant"].replace({
        "FII/FPI": "FII"
    })

    # ==========================
    # REMOVE BAD ROWS
    # ==========================
    df = df.dropna(subset=["date", "net"])

    if df.empty:
        print("⚠️ Empty after cleaning, skipping...")
        continue

    # ==========================
    # EXTRACT TRADE DATE
    # ==========================
    trade_date = df["date"].min()

    # Skip if already cleaned
    if trade_date in existing_dates:
        print(f"⏭️ Already cleaned: {trade_date}")
        continue

    # ==========================
    # REMOVE DUPLICATES (SAFETY)
    # ==========================
    df = df.drop_duplicates(subset=["date", "participant"])

    # ==========================
    # SORT
    # ==========================
    df = df.sort_values(["date", "participant"])

    # ==========================
    # SAVE
    # ==========================
    out_file = CLEAN_DIR / f"fii_dii_clean_{trade_date}.csv"
    df.to_csv(out_file, index=False)

    print(f"✅ Saved → {out_file}")
    new_files_processed += 1

# ==============================
# FINAL STATUS
# ==============================
print("\n📊 CLEANING SUMMARY:")
print(f"New files processed: {new_files_processed}")
print(f"Total raw files: {len(raw_files)}")