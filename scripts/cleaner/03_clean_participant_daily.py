#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge PRO
Participant Daily Data Cleaner (v1.7 - Vertical Stack Engine)
"""

import pandas as pd
import re
from pathlib import Path

print("🧼 INITIALIZING DAILY PARTICIPANT CLEANER (DYNAMIC DATE)...\n")

# CONFIGURATION
RAW_DAILY_DIR   = Path(r"H:\MarketForge\data\raw\participant_daily")
CLEAN_DAILY_DIR = Path(r"H:\MarketForge\data\processed\participant_daily")
CLEAN_DAILY_DIR.mkdir(parents=True, exist_ok=True)

# 1. Automatically find the newest downloaded file pair on disk
all_raw_files = sorted(list(RAW_DAILY_DIR.glob("participant_vol_*.csv")))

if not all_raw_files:
    print("❌ Aborting: No raw daily files found on disk. Run the downloader first.")
    exit()

# Extract the date string (DDMMYYYY) from the newest file's name
latest_raw_file = all_raw_files[-1]
match = re.search(r'(\d{8})', latest_raw_file.name)

if not match:
    print("❌ Aborting: Could not parse date format from raw files.")
    exit()

date_str = match.group(1)
date_iso = pd.to_datetime(date_str, format="%d%m%Y").strftime("%Y-%m-%d")

# 2. Assign file paths dynamically based on discovered date
vol_file = RAW_DAILY_DIR / f"participant_vol_{date_str}.csv"
oi_file = RAW_DAILY_DIR / f"participant_oi_{date_str}.csv"

print(f"📋 Detected most recent downloaded session on disk: {date_iso}")

if not vol_file.exists() or not oi_file.exists():
    print(f"❌ Aborting: Component pair missing for date {date_iso}.")
    exit()

def calculate_sentiment(net_val):
    if net_val > 50000:  return "STRONG LONG"
    if net_val > 0:      return "LONG"
    if net_val < -50000: return "STRONG SHORT"
    if net_val < 0:      return "SHORT"
    return "NEUTRAL"

# We will collect both processed dataframes here to stack them at the end
processed_frames = []

for file_path in [vol_file, oi_file]:
    # Identify if current file is Volume or Open Interest
    label_type = "VOLUME" if "_vol_" in file_path.name else "OI"
    
    df = pd.read_csv(file_path, skiprows=1)
    if df.empty or "Participant wise" in df.columns[0]:
        df = pd.read_csv(file_path)
        
    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    
    if "client_type" not in df.columns:
        continue
        
    df["client_type"] = df["client_type"].astype(str).str.upper().str.strip()
    df = df[df["client_type"].isin(["FII", "DII", "PRO", "CLIENT", "TOTAL"])].copy()
    
    numeric_targets = [
        "future_index_long", "future_index_short", "future_stock_long", "future_stock_short",
        "option_index_call_long", "option_index_put_long", "option_index_call_short", "option_index_put_short",
        "option_stock_call_long", "option_stock_put_long", "option_stock_call_short", "option_stock_put_short",
        "total_long_contracts", "total_short_contracts"
    ]
    
    # Cast integers cleanly to remove trailing decimals (.0)
    for col in numeric_targets:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        else:
            df[col] = 0

    # Calculate net transformations as integers
    df["net_index_futures"] = (df.get("future_index_long", 0) - df.get("future_index_short", 0)).astype(int)
    df["net_stock_futures"] = (df.get("future_stock_long", 0) - df.get("future_stock_short", 0)).astype(int)
    df["net_index_options"] = (
        df.get("option_index_call_long", 0) + df.get("option_index_put_long", 0) -
        df.get("option_index_call_short", 0) - df.get("option_index_put_short", 0)
    ).astype(int)
    df["net_total"] = (df.get("total_long_contracts", 0) - df.get("total_short_contracts", 0)).astype(int)
    
    df["sentiment_index"] = df["net_index_futures"].apply(calculate_sentiment)
    df["sentiment_stock"] = df["net_stock_futures"].apply(calculate_sentiment)
    
    # Tag this row slice so you know if it's Volume or Open Interest data
    df["data_type"] = label_type
    df["date"] = str(date_iso)
    
    processed_frames.append(df)

if processed_frames:
    # Stack the files row upon row vertically
    combined_df = pd.concat(processed_frames, ignore_index=True)

    # Organized Schema structure with the data_type column added
    schema = [
        "date", "data_type", "client_type", "future_index_long", "future_index_short", "net_index_futures",
        "future_stock_long", "future_stock_short", "net_stock_futures", "option_index_call_long",
        "option_index_put_long", "option_index_call_short", "option_index_put_short", "net_index_options",
        "total_long_contracts", "total_short_contracts", "net_total", "sentiment_index", "sentiment_stock"
    ]
    
    clean_df = combined_df[[c for c in schema if c in combined_df.columns]]
    clean_daily_file = CLEAN_DAILY_DIR / f"participant_clean_{date_iso}.csv"
    clean_df.to_csv(clean_daily_file, index=False)
    
    print(f"✅ Clean daily file generated successfully for: {date_iso}")
    print(f"📁 Location: {clean_daily_file}")
else:
    print("❌ Error: Could not construct daily structured data matrix frames.")

    