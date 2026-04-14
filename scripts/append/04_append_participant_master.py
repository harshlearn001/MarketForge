#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

print("📊 APPENDING PARTICIPANT MASTER (SMART)...\n")

CLEAN_DIR = Path(r"H:\MarketForge\data\processed\participant")
MASTER_FILE = Path(r"H:\MarketForge\data\master\participant\participant_master.csv")

MASTER_FILE.parent.mkdir(parents=True, exist_ok=True)

files = sorted(CLEAN_DIR.glob("participant_clean_*.csv"))

if not files:
    print("❌ No clean files")
    exit()

# ==============================
# LOAD EXISTING MASTER
# ==============================
if MASTER_FILE.exists():
    master_df = pd.read_csv(MASTER_FILE)
    existing_dates = set(master_df["date"].astype(str))
    before = len(master_df)
    print(f"📁 Existing rows: {before}")
else:
    master_df = pd.DataFrame()
    existing_dates = set()
    before = 0
    print("📁 Creating new master")

new_data = []

# ==============================
# LOAD ONLY NEW FILES
# ==============================
for file in files:
    df = pd.read_csv(file)

    if df.empty:
        continue

    file_date = str(df["date"].iloc[0])

    if file_date in existing_dates:
        print(f"⏭️ Skipping existing: {file_date}")
        continue

    print(f"📄 Adding: {file_date}")
    new_data.append(df)

# ==============================
# APPEND
# ==============================
if not new_data:
    print("⏭️ No new data to append")
    exit()

new_df = pd.concat(new_data, ignore_index=True)

combined = pd.concat([master_df, new_df], ignore_index=True)

# ==============================
# FINAL CLEAN
# ==============================
combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
combined["date"] = combined["date"].dt.strftime("%Y-%m-%d")

combined = combined.drop_duplicates(subset=["date", "client_type"])
combined = combined.sort_values(["date", "client_type"])

# SAFETY CHECK
if combined.empty:
    print("❌ ERROR: Data became empty — abort")
    exit()

# ==============================
# SAVE
# ==============================
combined.to_csv(MASTER_FILE, index=False)

after = len(combined)

print("\n===================================")
print(f"✅ MASTER UPDATED → {MASTER_FILE}")
print(f"📊 Previous rows: {before}")
print(f"📊 Current rows: {after}")
print(f"➕ New rows added: {after - before}")
print("===================================")