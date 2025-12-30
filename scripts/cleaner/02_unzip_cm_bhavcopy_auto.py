#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | UNZIP NSE CM BHAVCOPY

✔ Unzips BhavCopy_NSE_CM*.zip
✔ Keeps original CSV name
✔ Resume-safe
"""

from pathlib import Path
import zipfile

# =================================================
# PATHS
# =================================================
ROOT = Path(__file__).resolve().parents[2]   # H:\MarketForge

SRC_DIR = ROOT / "data" / "raw" / "equity"
OUT_DIR = ROOT / "data" / "unzip_daily" / "equty_daily_unzip"

print(f"ZIP source : {SRC_DIR}")

# =================================================
# FIND ZIP FILES
# =================================================
zip_files = list(SRC_DIR.glob("BhavCopy_NSE_CM*.zip"))

print(f" Found {len(zip_files)} ZIP files")

if not zip_files:
    raise RuntimeError(" No BhavCopy ZIP files found")

# =================================================
# UNZIP LOOP
# =================================================
for zip_path in zip_files:
    print(f" Unzipping: {zip_path.name}")

    with zipfile.ZipFile(zip_path, "r") as z:
        for member in z.namelist():
            out_file = OUT_DIR / member

            if out_file.exists():
                print(f" Exists: {member}")
                continue

            z.extract(member, OUT_DIR)
            print(f" Extracted: {member}")

print(" UNZIP COMPLETED")
