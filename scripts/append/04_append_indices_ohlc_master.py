#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | APPEND INDICES → INDIVIDUAL FILES (FINAL FIXED)

✔ One CSV per index
✔ TRADE_DATE = YYYYMMDD (int)
✔ SYMBOL column fixed (no blanks)
✔ Old blank SYMBOL fixed
✔ Column order enforced
✔ Append-safe & duplicate-safe
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

# ==================================================
# TARGET INDICES (CONTROL)
# ==================================================
TARGET_INDICES = {
    "NIFTY 50": "NIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "NIFTY NEXT 50": "NIFTYNEXT50",
    "INDIA VIX": "VIX",

    # sector
    "NIFTY IT": "NIFTYIT",
    "NIFTY FMCG": "NIFTYFMCG",
    "NIFTY AUTO": "NIFTYAUTO",
    "NIFTY METAL": "NIFTYMETAL",
    "NIFTY PHARMA": "NIFTYPHARMA",
    "NIFTY REALTY": "NIFTYREALTY",
}

# ==================================================
# PICK LATEST CLEAN FILE
# ==================================================
daily_file = max(
    CLEAN_DIR.glob("indices_ohlc_clean_*.csv"),
    key=lambda p: p.stat().st_mtime
)

print(f"📊 Daily file : {daily_file.name}")

# ==================================================
# LOAD DAILY
# ==================================================
daily = pd.read_csv(daily_file, low_memory=False)

# ==================================================
# FILTER REQUIRED INDICES
# ==================================================
daily = daily[daily["INDEX_NAME"].isin(TARGET_INDICES.keys())]

if daily.empty:
    raise RuntimeError("❌ No matching indices found")

# ==================================================
# PROCESS EACH INDEX
# ==================================================
for index_name, symbol in TARGET_INDICES.items():

    df_idx = daily[daily["INDEX_NAME"] == index_name]

    if df_idx.empty:
        print(f"⚠ Skipping {symbol} (not in today file)")
        continue

    # ==================================================
    # MAP SCHEMA (🔥 SYMBOL INCLUDED)
    # ==================================================
    mapped = pd.DataFrame({
        "TRADE_DATE": df_idx["TRADE_DATE"].astype("int64"),
        "SYMBOL": symbol,
        "OPEN": df_idx["OPEN"].astype("float64"),
        "HIGH": df_idx["HIGH"].astype("float64"),
        "LOW": df_idx["LOW"].astype("float64"),
        "CLOSE": df_idx["CLOSE"].astype("float64"),
    })

    file_path = MASTER_DIR / f"{symbol}.csv"

    # ==================================================
    # LOAD EXISTING
    # ==================================================
    if file_path.exists():
        master = pd.read_csv(file_path, low_memory=False)

        master["TRADE_DATE"] = master["TRADE_DATE"].astype("int64")

        # 🔥 FIX OLD BLANK SYMBOL
        if "SYMBOL" in master.columns:
            master["SYMBOL"] = master["SYMBOL"].fillna(symbol)

    else:
        master = pd.DataFrame(columns=mapped.columns)

    # ==================================================
    # APPEND + DEDUPE
    # ==================================================
    combined = (
        pd.concat([master, mapped], ignore_index=True)
        .drop_duplicates(subset=["TRADE_DATE"], keep="last")
        .sort_values("TRADE_DATE")
        .reset_index(drop=True)
    )

    # ==================================================
    # FORCE COLUMN ORDER (🔥 IMPORTANT)
    # ==================================================
    combined = combined[
        ["TRADE_DATE", "SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE"]
    ]

    # ==================================================
    # SAVE
    # ==================================================
    combined.to_csv(file_path, index=False)

    print(f"✅ Updated {symbol} → {len(combined)} rows")

print("\n🚀 ALL INDICES UPDATED SUCCESSFULLY")