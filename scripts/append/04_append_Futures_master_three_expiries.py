#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd
import shutil

# ==================================================
# DEBUG
# ==================================================
DEBUG_MODE = True

# ==================================================
# PATHS
# ==================================================
ROOT = Path(r"H:\MarketForge")

SOURCE_MASTER = ROOT / "data" / "master" / "Futures_master"
TARGET_MASTER = ROOT / "data" / "master" / "Futures_master_three_expiries"

FUTSTK_SRC = SOURCE_MASTER / "FUTSTK"
FUTIDX_SRC = SOURCE_MASTER / "FUTIDX"

FUTSTK_OUT = TARGET_MASTER / "FUTSTK"
FUTIDX_OUT = TARGET_MASTER / "FUTIDX"

# ==================================================
# RESET
# ==================================================
if DEBUG_MODE:
    print("⚠️ RESETTING 3-EXPIRY MASTER")
    if TARGET_MASTER.exists():
        shutil.rmtree(TARGET_MASTER)

FUTSTK_OUT.mkdir(parents=True, exist_ok=True)
FUTIDX_OUT.mkdir(parents=True, exist_ok=True)

# ==================================================
# CORE FUNCTION (APPEND VERSION)
# ==================================================
def build_three_expiry(src_folder, out_folder):

    for file in src_folder.glob("*.csv"):

        print(f"→ Processing {file.name}")

        df = pd.read_csv(file, low_memory=False)

        if df.empty:
            continue

        # -----------------------------
        # CLEAN TYPES
        # -----------------------------
        df["TRADE_DATE"] = pd.to_numeric(df["TRADE_DATE"], errors="coerce")
        df["EXP_DATE"] = pd.to_numeric(df["EXP_DATE"], errors="coerce")

        df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip()

        df = df.dropna(subset=["TRADE_DATE", "EXP_DATE", "SYMBOL"])

        # -----------------------------
        # 3 EXPIRY LOGIC
        # -----------------------------
        df = df.sort_values(["SYMBOL", "TRADE_DATE", "EXP_DATE"])

        df["EXP_RANK"] = df.groupby(
            ["SYMBOL", "TRADE_DATE"]
        )["EXP_DATE"].rank(method="first")

        df = df[df["EXP_RANK"] <= 3]

        df["EXPIRY_TYPE"] = df["EXP_RANK"].map({
            1: "NEAR",
            2: "NEXT",
            3: "FAR"
        })

        df = df.drop(columns=["EXP_RANK"])

        # =================================================
        # 🔥 SYMBOL-WISE APPEND (IMPORTANT FIX)
        # =================================================
        for symbol, g in df.groupby("SYMBOL"):

            symbol = str(symbol).strip()
            out_file = out_folder / f"{symbol}.csv"

            if out_file.exists():
                old = pd.read_csv(out_file, low_memory=False)

                old["TRADE_DATE"] = pd.to_numeric(old["TRADE_DATE"], errors="coerce")
                old["EXP_DATE"] = pd.to_numeric(old["EXP_DATE"], errors="coerce")

                merged = pd.concat([old, g], ignore_index=True)

            else:
                merged = g.copy()

            # remove duplicates
            merged = (
                merged
                .drop_duplicates(
                    subset=["SYMBOL", "TRADE_DATE", "EXP_DATE"],
                    keep="last"
                )
                .sort_values(["TRADE_DATE", "EXP_DATE"])
            )

            merged.to_csv(out_file, index=False)

# ==================================================
# RUN
# ==================================================
print("\n📊 BUILDING FUTSTK 3-EXPIRY (APPEND)")
build_three_expiry(FUTSTK_SRC, FUTSTK_OUT)

print("\n📊 BUILDING FUTIDX 3-EXPIRY (APPEND)")
build_three_expiry(FUTIDX_SRC, FUTIDX_OUT)

print("\n🎯 DONE — APPEND WORKING PERFECTLY")