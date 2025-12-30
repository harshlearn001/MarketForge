#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | APPEND NIFTY → MASTER_NIFTY (LOCKED)

✔ Uses standardized index OHLC
✔ Filters NIFTY 50 (authoritative)
✔ TRADE_DATE = YYYYMMDD (int)
✔ Schema locked
✔ Append-safe & duplicate-safe
✔ CSV ONLY (index master)
"""

from pathlib import Path
import pandas as pd

# ==================================================
# PATHS
# ==================================================
ROOT = Path(r"H:\MarketForge")

CLEAN_DIR = ROOT / "data" / "processed" / "indices_daily"
MASTER_DIR = ROOT / "data" / "master" / "Indices_master"
MASTER_DIR.mkdir(parents=True, exist_ok=True)

MASTER_FILE = MASTER_DIR / "master_nifty.csv"

# ==================================================
# PICK LATEST CLEAN INDEX FILE
# ==================================================
daily_file = max(
    CLEAN_DIR.glob("indices_ohlc_clean_*.csv"),
    key=lambda p: p.stat().st_mtime
)

print(f" Daily file : {daily_file.name}")

# ==================================================
# LOAD DAILY CLEAN
# ==================================================
daily = pd.read_csv(daily_file, low_memory=False)

# ==================================================
# FILTER NIFTY 50 (AUTHORITATIVE)
# ==================================================
daily = daily[daily["INDEX_NAME"] == "NIFTY 50"]

if daily.empty:
    raise RuntimeError(" No NIFTY 50 data found in daily index file")

# ==================================================
# MAP → MASTER_NIFTY SCHEMA
# ==================================================
mapped = pd.DataFrame({
    "TRADE_DATE": daily["TRADE_DATE"].astype("int64"),
    "SYMBOL": "NIFTY",
    "OPEN": daily["OPEN"].astype("float64"),
    "HIGH": daily["HIGH"].astype("float64"),
    "LOW": daily["LOW"].astype("float64"),
    "CLOSE": daily["CLOSE"].astype("float64"),
})

# ==================================================
# LOAD OR INIT MASTER
# ==================================================
if MASTER_FILE.exists():
    master = pd.read_csv(MASTER_FILE, low_memory=False)

    master["TRADE_DATE"] = master["TRADE_DATE"].astype("int64")
else:
    master = pd.DataFrame(columns=mapped.columns)

# ==================================================
# APPEND + TRUE DEDUPE
# ==================================================
combined = (
    pd.concat([master, mapped], ignore_index=True)
    .drop_duplicates(subset=["TRADE_DATE", "SYMBOL"], keep="last")
    .sort_values("TRADE_DATE")
    .reset_index(drop=True)
)

# ==================================================
# SAVE
# ==================================================
combined.to_csv(MASTER_FILE, index=False)

print("\n NIFTY MASTER APPEND COMPLETED (LOCKED)")
print(f" Master file : {MASTER_FILE}")
print(f" Total rows : {len(combined)}")
print(
    f" Date range : "
    f"{combined['TRADE_DATE'].min()} → {combined['TRADE_DATE'].max()}"
)
