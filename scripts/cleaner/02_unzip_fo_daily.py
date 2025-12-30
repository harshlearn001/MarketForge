#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | UNZIP NSE FO DAILY ZIP

✔ Unzips foDDMMYYYY.zip
✔ Preserves original CSV names
✔ Resume-safe
✔ Production safe
"""

from pathlib import Path
import zipfile

# =================================================
# PATHS
# =================================================
ROOT = Path(__file__).resolve().parents[2]   # H:\MarketForge

SRC_DIR = ROOT / "data" / "raw" / "futures"
OUT_DIR = ROOT / "data" / "unzip_daily" / "future_daily_unzip"

OUT_DIR.mkdir(parents=True, exist_ok=True)

print(f" ZIP source : {SRC_DIR}")
print(f" Output dir : {OUT_DIR}")

# =================================================
# DISCOVER FO ZIP FILES
# =================================================
zip_files = sorted(SRC_DIR.glob("fo*.zip"))
print(f" Found {len(zip_files)} FO ZIP files")

if not zip_files:
    raise RuntimeError(" No FO ZIP files found")

# =================================================
# UNZIP LOOP
# =================================================
for zip_path in zip_files:
    print(f"\n Unzipping: {zip_path.name}")

    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            for member in z.namelist():
                out_file = OUT_DIR / member

                if out_file.exists():
                    print(f" Exists: {member}")
                    continue

                z.extract(member, OUT_DIR)
                print(f" Extracted: {member}")

    except zipfile.BadZipFile:
        print(f" Bad ZIP file: {zip_path.name}")

print("\n FO DAILY UNZIP COMPLETED")
