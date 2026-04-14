#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

print("🧹 CLEANING PARTICIPANT DATA (DAILY PRO)...\n")

RAW_DIR = Path(r"H:\MarketForge\data\raw\participant")
CLEAN_DIR = Path(r"H:\MarketForge\data\processed\participant")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

files = sorted(RAW_DIR.glob("participant_raw_*.csv"))

if not files:
    print("❌ No raw files")
    exit()

latest = files[-1]
print(f"📄 Using: {latest.name}")

# ==============================
# LOAD
# ==============================
df = pd.read_csv(latest, skiprows=1)

if df.empty:
    print("❌ Empty file — skipping")
    exit()

# ==============================
# CLEAN COLUMN NAMES
# ==============================
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

print("\n📊 Columns Found:")
print(df.columns.tolist())

# ==============================
# VALIDATE REQUIRED
# ==============================
required_cols = [
    "client_type",
    "future_index_long",
    "future_index_short"
]

for col in required_cols:
    if col not in df.columns:
        print(f"❌ Missing column: {col}")
        exit()

# ==============================
# FILTER FII / DII
# ==============================
df["client_type"] = df["client_type"].astype(str).str.upper().str.strip()
df = df[df["client_type"].isin(["FII", "DII"])]

if df.empty:
    print("❌ No FII/DII data")
    exit()

# ==============================
# DATE
# ==============================
date_str = latest.name.split("_")[-1].replace(".csv", "")
date_fmt = pd.to_datetime(date_str, format="%d%m%Y").strftime("%Y-%m-%d")

df["date"] = date_fmt

# ==============================
# NUMERIC CLEANING (IMPORTANT)
# ==============================
numeric_cols = [
    "future_index_long",
    "future_index_short",
    "future_stock_long",
    "future_stock_short"
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ==============================
# CALCULATE NET
# ==============================
df["net_index_futures"] = df["future_index_long"] - df["future_index_short"]

if "future_stock_long" in df.columns and "future_stock_short" in df.columns:
    df["net_stock_futures"] = df["future_stock_long"] - df["future_stock_short"]
else:
    df["net_stock_futures"] = 0.0

# ==============================
# FINAL SELECT
# ==============================
df = df[[
    "date",
    "client_type",
    "net_index_futures",
    "net_stock_futures"
]]

# ==============================
# FINAL STANDARDIZATION
# ==============================
df["net_index_futures"] = df["net_index_futures"].astype(float)
df["net_stock_futures"] = df["net_stock_futures"].astype(float)

# Remove bad rows
df = df.dropna(subset=["date"])

# ==============================
# SAFETY CHECK
# ==============================
if df.empty:
    print("❌ ERROR: Data became empty — not saving")
    exit()

# ==============================
# SAVE
# ==============================
out_file = CLEAN_DIR / f"participant_clean_{date_fmt}.csv"
df.to_csv(out_file, index=False)

print("\n📊 CLEAN PREVIEW:")
print(df)

print(f"\n✅ Saved → {out_file}")