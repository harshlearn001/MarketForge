#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
APPEND FII/DII MASTER (INCREMENTAL UPDATE - PRO)

✔ Appends only NEW data
✔ Avoids full reload
✔ Prevents duplicates
✔ Fast & scalable
"""

import pandas as pd
from pathlib import Path

print("📊 APPENDING FII/DII MASTER (INCREMENTAL)...\n")

# ==============================
# PATHS
# ==============================
CLEAN_DIR = Path(r"H:\MarketForge\data\processed\fii_dii\clean")
MASTER_FILE = Path(r"H:\MarketForge\data\master\fii_dii\fii_dii_master.csv")

clean_files = sorted(CLEAN_DIR.glob("fii_dii_clean_*.csv"))

if not clean_files:
    print("❌ No clean files found")
    exit()

# ==============================
# LOAD EXISTING MASTER
# ==============================
if MASTER_FILE.exists():
    master_df = pd.read_csv(MASTER_FILE)
    existing_keys = set(
        zip(master_df["date"], master_df["participant"])
    )
    print(f"📁 Existing master rows: {len(master_df)}")
else:
    master_df = pd.DataFrame()
    existing_keys = set()
    print("📁 No master file found → creating new")

# ==============================
# PROCESS NEW FILES
# ==============================
new_rows = []

for file in clean_files:
    df = pd.read_csv(file)

    for _, row in df.iterrows():
        key = (row["date"], row["participant"])

        if key not in existing_keys:
            new_rows.append(row)

# ==============================
# APPEND NEW DATA
# ==============================
if new_rows:
    new_df = pd.DataFrame(new_rows)
    master_df = pd.concat([master_df, new_df], ignore_index=True)

    print(f"➕ New rows added: {len(new_df)}")
else:
    print("⏭️ No new data to append")

# ==============================
# FINAL CLEANUP
# ==============================
if not master_df.empty:
    master_df = master_df.drop_duplicates(
        subset=["date", "participant"]
    )
    master_df = master_df.sort_values(["date", "participant"])

# ==============================
# SAVE MASTER
# ==============================
master_df.to_csv(MASTER_FILE, index=False)

print(f"\n✅ MASTER UPDATED → {MASTER_FILE}")
print(f"📊 Total rows: {len(master_df)}")