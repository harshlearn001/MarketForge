# MarketForge Project Data Summary

**Project Location**: `H:\MarketForge`  
**Current Date**: April 14, 2026  
**Last Updated**: April 14, 2026

---

## Project Overview

**MarketForge** is a production-grade NSE market data engine for Indian markets with:
- Deterministic downloader → cleaner → append pipelines
- Append-only master datasets
- Idempotent daily workflows
- Incremental rerun protection for major daily steps
- Coverage across equities, MTO delivery data, indices, futures, options, participant, and FII/DII activity

---

## Data Inventory

### Master Dataset Counts
```text
Equity Masters (stocks):        2,578 files
Indices Masters:                   19 files
Futures Masters:
  FUTSTK:                         361 files
  FUTIDX:                          13 files
Options Masters:
  STOCKS:                         293 files
  INDICES:                          5 files
Equity MTO Masters:               501 files
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TOTAL MASTER CSV FILES:         3,770 files
```

### Other Active Masters
```text
Participant Master:                1 file
FII/DII Master:                    1 file
```

### Master Directories
```text
data/master/
├── Equity_stock_master/        ← 2,578 symbol CSVs
├── Indices_master/             ← 19 index CSVs
├── Futures_master/
│   ├── FUTSTK/                 ← 361 stock futures masters
│   ├── FUTIDX/                 ← 13 index futures masters
│   └── _state/                 ← Incremental append state
├── option_master/
│   ├── STOCKS/                 ← 293 stock options masters
│   ├── INDICES/                ← 5 index options masters
│   └── _state/                 ← Incremental append state
├── EquityDat_master/           ← 501 symbol MTO masters (legacy folder still supported)
├── participant/
│   └── participant_master.csv
└── fii_dii/
    └── fii_dii_master.csv
```

---

## Current Coverage

### Equity Masters
- **2,578 equity stock master files**
- Example latest coverage:
  - `INFY.csv` → `2026-04-13`
  - `RELIANCE.csv` and other liquid names are up to the latest aligned market date
- Sample files:

```text
20MICRONS.csv      (0.01 MB)
21STCENMGM.csv     (0.01 MB)
360ONE.csv         (0.07 MB)
3IINFOLTD.csv      (0.01 MB)
3MINDIA.csv        (0.08 MB)
3PLAND.csv         (0.01 MB)
5PAISA.csv         (0.01 MB)
63MOONS.csv        (0.01 MB)
A2ZINFRA.csv       (0.00 MB)
AAATECH.csv        (0.00 MB)
...
```

### Indices Masters
- **19 index master files**
- Examples present:
  - `NIFTY.csv`
  - `BANKNIFTY.csv`
  - `FINNIFTY.csv`
  - `NIFTY100.csv`
  - `NIFTY200.csv`
  - `NIFTY500.csv`
  - `NIFTYIT.csv`
  - `NIFTYFMCG.csv`
  - `NIFTYALPHA50.csv`
  - `NIFTYLOWVOL50.csv`
- Example latest coverage:
  - `NIFTY.csv` → `TRADE_DATE=20260413`

### Futures Masters
- **361 stock futures masters** in `FUTSTK/`
- **13 index futures masters** in `FUTIDX/`
- Example latest coverage:
  - `FUTSTK/RELIANCE.csv` → `TRADE_DATE=20260413`
  - `FUTIDX/NIFTY.csv` → `TRADE_DATE=20260413`

### Options Masters
- **293 stock options masters** in `STOCKS/`
- **5 index options masters** in `INDICES/`
- Example latest coverage:
  - `STOCKS/RELIANCE.csv` → `TRADE_DATE=20260413`
  - `INDICES/NIFTY.csv` → `TRADE_DATE=20260413`

### MTO / Delivery Masters
- **501 symbol delivery masters** in `EquityDat_master/`
- Example latest coverage:
  - `EquityDat_master/RELIANCE.csv` → `TRADE_DATE=20260413`

### Institutional and Participant Data
- `participant_master.csv` latest date: `2026-04-13`
- `fii_dii_master.csv` latest date: `2026-04-13`

---

## Directory Structure

```text
H:\MarketForge\
│
├── data/
│   ├── raw/                        # Downloaded source files
│   │   ├── equity/
│   │   ├── futures/
│   │   ├── equityDat/
│   │   ├── indices/
│   │   ├── fii_dii/
│   │   └── participant/
│   │
│   ├── processed/                  # Cleaned daily outputs
│   │   ├── equity_daily/
│   │   ├── futures_daily/
│   │   ├── options_daily/
│   │   ├── equityDat_daily/
│   │   ├── indices_daily/
│   │   ├── participant/
│   │   └── fii_dii/
│   │
│   ├── master/                     # Production masters
│   │   ├── Equity_stock_master/
│   │   ├── Futures_master/
│   │   ├── Indices_master/
│   │   ├── option_master/
│   │   ├── EquityDat_master/
│   │   ├── participant/
│   │   └── fii_dii/
│   │
│   └── unzip_daily/
│       ├── equity_daily_unzip/     # Canonical unzip path
│       ├── equty_daily_unzip/      # Legacy-compatible path
│       └── future_daily_unzip/
│
├── scripts/
│   ├── downloader/
│   ├── cleaner/
│   ├── append/
│   ├── master_merge/
│   ├── 99_check_master_last_rows.py
│   └── daily_run_equity.ps1
│
├── config/
├── ARCHITECTURE.md
├── README.md
├── requirements.txt
└── MARKETFORGE_DATA_SUMMARY.md
```

---

## Pipeline Flow

```text
Step 1: DOWNLOAD
  └─ NSE archive / API endpoints
     └─ CM bhavcopy ZIP
     └─ FO ZIP
     └─ MTO DAT
     └─ Indices OHLC
     └─ FII/DII activity
     └─ Participant data

Step 2: CLEAN
  └─ Unzip CM / FO archives
  └─ Normalize NSE schema variants
  └─ Split futures/options by segment
  └─ Standardize date and numeric fields
  └─ Write processed daily CSVs

Step 3: APPEND
  └─ Build or extend master CSVs
  └─ Deduplicate by date/contract keys
  └─ Preserve append-only historical datasets

Result
  └─ Symbol-wise and index-wise master datasets
  └─ Ready for analytics, ML, screening, and backtesting
```

---

## Pipeline Status As Of April 14, 2026

### Daily Alignment
- The daily pipeline is currently aligned to the latest fully published market date across CM, FO, and MTO.
- On April 14, 2026 evening runs, the aligned market date was **April 13, 2026** because April 14 FO and MTO files were not yet published.
- Index processing now respects this alignment and no longer runs ahead of the rest of the market data.

### Incremental Rerun Behavior
- The following steps now skip already-processed daily files:
  - `03_clean_cm_bhavcopy_daily_auto.py`
  - `03_clean_futures_daily.py`
  - `03_clean_options_daily.py`
  - `03_clean_mto_daily.py`
- The following master appenders now track processed daily inputs and skip repeat appends:
  - `04_append_equity_stock_master.py`
  - `04_append_futures_master.py`
  - `04_append_options_master.py`
  - `04_append_indices_ohlc_master.py`

### State Files
```text
data/master/Equity_stock_master/_state/
  processed_equity_files.txt

data/master/Indices_master/_state/
  processed_index_clean_files.txt

data/master/Futures_master/_state/
  processed_futstk_files.txt
  processed_futidx_files.txt

data/master/option_master/_state/
  processed_stocks_files.txt
  processed_indices_files.txt
```

---

## Key Scripts

### Downloaders
- `scripts/downloader/01_download_cm_bhavcopy_auto.py`
- `scripts/downloader/01_download_fo_zip_auto.py`
- `scripts/downloader/01_download_mto_dat_auto.py`
- `scripts/downloader/01_download_indices_ohlc_auto.py`
- `scripts/downloader/01_download_fii_dii_activity.py`
- `scripts/downloader/01_download_participant_data.py`

### Cleaners
- `scripts/cleaner/02_unzip_cm_bhavcopy_auto.py`
- `scripts/cleaner/02_unzip_fo_daily.py`
- `scripts/cleaner/03_clean_cm_bhavcopy_daily_auto.py`
- `scripts/cleaner/03_clean_futures_daily.py`
- `scripts/cleaner/03_clean_options_daily.py`
- `scripts/cleaner/03_clean_mto_daily.py`
- `scripts/cleaner/03_clean_indices_ohlc.py`
- `scripts/cleaner/03_clean_fii_dii_daily.py`
- `scripts/cleaner/03_clean_participant_daily.py`

### Master Builders / Appenders
- `scripts/append/04_append_equity_stock_master.py`
- `scripts/append/04_append_equity_mto_master.py`
- `scripts/append/04_append_futures_master.py`
- `scripts/append/04_append_options_master.py`
- `scripts/append/04_append_indices_ohlc_master.py`
- `scripts/append/04_append_fii_dii_master.py`
- `scripts/append/04_append_participant_master.py`

### Utilities
- `scripts/99_check_master_last_rows.py`
- `scripts/daily_run_equity.ps1`

---

## Data Quality Notes

### Strengths
- Append-only master design
- Deduplication built into appenders
- Stable archive usage for CM, FO, and MTO
- Aligned market-date handling for index data
- Incremental rerun protection across most major daily stages

### Known Legacy Quirks
- Legacy folder typos may still exist on disk:
  - `data/master/EquityDat_master`
  - `data/unzip_daily/equty_daily_unzip`
- Newer code prefers canonical names where possible:
  - `data/master/EquityDat_master`
  - `data/unzip_daily/equity_daily_unzip`
- Some older documentation assumed futures/options were empty; that is no longer true.
- A few master-building scripts still keep historical naming/layout decisions for compatibility.

---

## Usage Examples

### Run the Daily Pipeline
```powershell
.\scripts\daily_run_equity.ps1
```

### Inspect Master Files
```powershell
python scripts/99_check_master_last_rows.py
```

### Run Individual Stages
```powershell
python scripts/downloader/01_download_cm_bhavcopy_auto.py
python scripts/cleaner/02_unzip_cm_bhavcopy_auto.py
python scripts/cleaner/03_clean_cm_bhavcopy_daily_auto.py
python scripts/append/04_append_equity_stock_master.py
```

---

## Summary

**MarketForge** is now a broader and more mature data engine than this file previously described:
- **2,578** equity masters
- **19** index masters
- **374** futures masters
- **298** options masters
- **501** MTO masters
- Participant and FII/DII masters active
- Daily pipeline stabilized around aligned market dates
- Incremental reruns in place for the main heavy steps

The repository is operational for daily NSE market ingestion and significantly closer to a scheduler-friendly production workflow than the earlier February 2026 snapshot suggested.
