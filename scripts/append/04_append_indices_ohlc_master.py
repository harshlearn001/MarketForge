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
from datetime import datetime
import re
import pandas as pd

# ==================================================
# PATHS
# ==================================================
ROOT = Path(r"H:\MarketForge")

CLEAN_DIR = ROOT / "data" / "processed" / "indices_daily"
MASTER_DIR = ROOT / "data" / "master" / "Indices_master"
STATE_DIR = MASTER_DIR / "_state"
MASTER_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)
EQUITY_RAW_DIR = ROOT / "data" / "raw" / "equity"
FUTURES_RAW_DIR = ROOT / "data" / "raw" / "futures"
MTO_RAW_DIR = ROOT / "data" / "raw" / "equityDat"

# ==================================================
# TARGET INDICES (CONTROL)
# ==================================================
TARGET_INDICES = {
    "NIFTY 50": "NIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "NIFTY NEXT 50": "NIFTYNEXT50",
    "NIFTY 100": "NIFTY100",
    "NIFTY 200": "NIFTY200",
    "NIFTY 500": "NIFTY500",
    "INDIA VIX": "VIX",

    # sector
    "NIFTY IT": "NIFTYIT",
    "NIFTY FMCG": "NIFTYFMCG",
    "NIFTY AUTO": "NIFTYAUTO",
    "NIFTY METAL": "NIFTYMETAL",
    "NIFTY PHARMA": "NIFTYPHARMA",
    "NIFTY REALTY": "NIFTYREALTY",
    "NIFTY ENERGY": "NIFTYENERGY",

    # factor
    "NIFTY ALPHA 50": "NIFTYALPHA50",
    "NIFTY LOW VOLATILITY 50": "NIFTYLOWVOL50",
}


def latest_date_from_files(folder: Path, pattern: str, date_format: str):
    dates = []
    for path in folder.glob(pattern):
        match = re.search(r"(\d{8})", path.name)
        if not match:
            continue
        try:
            dates.append(datetime.strptime(match.group(1), date_format).date())
        except ValueError:
            continue
    return max(dates) if dates else None


def latest_fully_published_trade_date():
    equity_date = latest_date_from_files(
        EQUITY_RAW_DIR,
        "BhavCopy_NSE_CM_*.zip",
        "%Y%m%d",
    )
    futures_date = latest_date_from_files(
        FUTURES_RAW_DIR,
        "fo*.zip",
        "%d%m%Y",
    )
    mto_date = latest_date_from_files(
        MTO_RAW_DIR,
        "MTO_*.DAT",
        "%d%m%Y",
    )

    available = [d for d in [equity_date, futures_date, mto_date] if d is not None]
    if len(available) < 3:
        return None

    return min(available)


def load_processed(state_file: Path) -> set[str]:
    if not state_file.exists():
        return set()

    return {
        line.strip()
        for line in state_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def save_processed(state_file: Path, processed: set[str]) -> None:
    state_file.write_text(
        "\n".join(sorted(processed)) + ("\n" if processed else ""),
        encoding="utf-8",
    )

# ==================================================
# PICK LATEST CLEAN FILE
# ==================================================
clean_files = list(CLEAN_DIR.glob("indices_ohlc_clean_*.csv"))
if not clean_files:
    raise RuntimeError("No cleaned index OHLC files found")

published_trade_date = latest_fully_published_trade_date()
if published_trade_date is not None:
    clean_files = [
        p for p in clean_files
        if re.search(r"(\d{8})", p.name)
        and datetime.strptime(re.search(r"(\d{8})", p.name).group(1), "%Y%m%d").date() <= published_trade_date
    ]

if not clean_files:
    raise RuntimeError("No aligned cleaned index OHLC files found")

daily_file = max(
    clean_files,
    key=lambda p: datetime.strptime(re.search(r"(\d{8})", p.name).group(1), "%Y%m%d")
)

print(f"📊 Daily file : {daily_file.name}")

state_file = STATE_DIR / "processed_index_clean_files.txt"
processed_files = load_processed(state_file)
if daily_file.name in processed_files:
    print(f" Already appended, skipping → {daily_file.name}")
    raise SystemExit(0)

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
    if master.empty:
        combined = mapped.sort_values("TRADE_DATE").reset_index(drop=True)
    else:
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

processed_files.add(daily_file.name)
save_processed(state_file, processed_files)
