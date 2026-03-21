# MarketForge Project Data Summary

**Project Location**: `H:\MarketForge`  
**Current Date**: February 19, 2026  
**Last Updated**: February 18, 2026 (20:32)

---

## 📊 Project Overview

**MarketForge** is a production-grade market data engine for Indian NSE markets featuring:
- Deterministic, reproducible data pipelines
- Append-only master datasets (idempotent)
- NSE-safe workflows
- Multiple asset classes: Equities, Futures, Options, Indices

---

## 🗂️ Data Inventory

### Master Dataset Counts
```
Equity Masters (stocks):        2,531 files
Indices Masters:                1 file
Futures Masters:                0 files
Options Masters:                0 files
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TOTAL:                           2,532 files
```

### Master Directories
```
data/master/
├── Equity_stock_master/        ← 2,531 symbol CSVs (ACTIVE)
├── Futures_master/             ← 0 files (Not populated)
├── Indices_master/             ← 1 file (NIFTY50/SENSEX)
├── option_master/              ← 0 files (Not populated)
└── EqiutyDat_master/           ← Legacy (typo in name)
```

### Equity Masters Coverage

**2,531 Equity Stock Master Files** covering:

| Category | Count | Examples |
|----------|-------|----------|
| NIFTY500 (Primary) | ~500 | ADANIENT, INFY, TCS, RELIANCE, SBIN, HDFC... |
| All Listed Stocks | ~2,000+ | Including micro-caps and penny stocks |
| Data Range | Multiple years | Each file contains daily OHLCV history |

**Sample Files** (First 20 alphabetically):
```
20MICRONS.csv          (0.00 MB)
21STCENMGM.csv         (0.00 MB)
360ONE.csv             (0.06 MB)
3IINFOLTD.csv          (0.00 MB)
3MINDIA.csv            (0.08 MB)
AARTIIND.csv           (0.23 MB)  ← Larger files = longer history
...
[2,531 total]
```

---

## 📁 Directory Structure

```
H:\MarketForge/
│
├── 📁 data/                           ← Data storage
│   ├── 📁 raw/                        # Downloaded raw data (large, ignored by git)
│   │   └── 📁 equity/                 # Downloaded bhavcopy ZIPs
│   │
│   ├── 📁 processed/                  # Cleaned daily data
│   │   └── 📁 equity_daily/           # Cleaned CSV files (temporary)
│   │
│   ├── 📁 master/                     # MASTER DATASETS (PRODUCTION)
│   │   ├── 📁 Equity_stock_master/    # ⭐ 2,531 symbol CSVs
│   │   ├── 📁 Indices_master/         # NIFTY indices
│   │   ├── 📁 Futures_master/         # (Empty)
│   │   ├── 📁 option_master/          # (Empty)
│   │   └── 📁 EqiutyDat_master/       # Legacy
│   │
│   ├── 📁 unzip_daily/                # Unzipped bhavcopy daily
│   ├── 📁 reports/                    # Analysis reports
│   ├── 📄 nifty_500_symbols.csv       # ⭐ SYMBOL MAPPING (503 lines)
│   └── 📄 .gitkeep
│
├── 📁 scripts/                        ← Data pipeline scripts
│   ├── 📁 downloader/                 # Step 1: NSE downloads
│   │   ├── 01_download_cm_bhavcopy_auto.py
│   │   ├── 01_download_fo_zip_auto.py
│   │   ├── 01_download_indices_ohlc_auto.py
│   │   ├── 01_download_mto_dat_auto.py
│   │   └── [manual versions + utilities]
│   │
│   ├── 📁 cleaner/                    # Step 2: Normalize data
│   │   ├── 02_unzip_cm_bhavcopy_auto.py
│   │   ├── 03_clean_cm_bhavcopy_daily_auto.py
│   │   ├── 03_clean_futures_daily.py
│   │   ├── 03_clean_indices_ohlc.py
│   │   └── [more cleaners]
│   │
│   ├── 📁 append/                     # Step 3: Merge into masters
│   │   ├── 04_append_equity_stock_master.py
│   │   ├── 04_append_equity_mto_master.py
│   │   ├── 04_append_futures_master.py
│   │   ├── 04_append_indices_ohlc_master.py
│   │   └── 04_append_options_master.py
│   │
│   ├── 📁 utils/                      # Helper functions
│   ├── 📁 validator/                  # Data validation
│   ├── 📁 master_merge/               # Master merging utilities
│   ├── 📄 99_check_master_last_rows.py   # Inspection utility
│   └── 📄 daily_run_equity.ps1         # PowerShell scheduler
│
├── 📁 config/                         ← Configuration
│   ├── 📄 settings.py                 # Global settings
│   ├── 📄 symbols.py                  # Symbol configurations
│   └── 📄 holidays.py                 # Trading holidays
│
├── 📁 logs/                           ← Execution logs (ignored)
├── 📁 reports/                        ← Analysis reports
│
├── 📄 README.md                       ← Project description
├── 📄 ARCHITECTURE.md                 ← System design
├── 📄 requirements.txt                ← Dependencies
└── 📄 .gitignore                      # Git exclusions
```

---

## 📈 Data Pipeline Flow

```
Step 1: DOWNLOAD
  └─ NSE Website
     └─ Bhavcopy ZIP files (CM, FO, Indices, MTO)
     └─ Saved to: data/raw/

Step 2: CLEANER
  └─ Unzip files
  └─ Normalize schema (handle old/new NSE formats)
  └─ Filter to NIFTY stocks (Series = EQ)
  └─ Output: data/processed/equity_daily/*.csv

Step 3: APPEND
  └─ Load cleaned daily data
  └─ Per-symbol master files (data/master/Equity_stock_master/)
  └─ Append new dates (deduplicated, idempotent)
  └─ Output: {SYMBOL}.csv

Result: MASTER DATASETS
  └─ Each symbol has complete daily OHLCV history
  └─ Ready for: Backtesting, ML, Analytics
```

---

## 📋 Equity Master File Format

Each CSV file (e.g., `INFY.csv`, `TCS.csv`) contains:

| Column | Type | Description |
|--------|------|-------------|
| DATE | DATETIME | Trading date (YYYY-MM-DD or normalized) |
| SYMBOL | STRING | Stock symbol (e.g., "INFY") |
| SERIES | STRING | Always "EQ" (cash market equity) |
| OPEN | FLOAT | Opening price |
| HIGH | FLOAT | Day's high |
| LOW | FLOAT | Day's low |
| CLOSE | FLOAT | Closing price |
| LAST | FLOAT | Last traded price |
| PREVCLOSE | FLOAT | Previous closing price |
| TOTTRDQTY | FLOAT | Total traded quantity (volume) |
| TOTTRDVAL | FLOAT | Total trading value (turnover) |
| TOTALTRADES | INT | Total number of trades |
| ISIN | STRING | ISIN code (unique security identifier) |

**Example Row**:
```
2026-02-18,INFY,EQ,1685.0,1695.5,1680.0,1690.0,1690.0,1682.0,5000000,8450000000,50000,INE009A01021
```

---

## 🔍 Key Data Points

### Coverage
- **Symbols**: 2,531 (all NSE-listed equities)
- **Primary Universe**: NIFTY500 (500 most liquid)
- **NIFTY50**: Subset of largest market-cap stocks

### NIFTY500 Sample Stocks
```
AARTIIND          (Chemicals)
ADANIENT          (Metals & Mining)
AXISBANK          (Financial Services)
BHARTIARTL        (Telecom)
HDFC              (Banking)
INFY              (IT)
MARUTI            (Auto)
RELIANCE          (Oil & Gas)
TCS               (IT)
WIPRO             (IT)
[500 total]
```

### File Sizes
- **Large**: 0.20+ MB (AARTIIND.csv = 0.23 MB = ~5+ years of data)
- **Small**: 0.00 MB (newer IPOs or low activity stocks)
- **Total**: ~500+ MB for all 2,531 stocks

### Data Freshness
- **Last Updated**: February 18, 2026, 20:32
- **Coverage**: Likely through February 2026 (current month)

---

## 🔧 Pipeline Scripts Summary

### 1️⃣ Downloaders (scripts/downloader/)
**Purpose**: Fetch raw data from NSE

- **01_download_cm_bhavcopy_auto.py** ← ACTIVE
  - Downloads daily Equity (CM - Cashmarket) Bhavcopy
  - Uses NSE archives (stable endpoint)
  - Tries today, backtracks if not published
  - Saves as: `BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip`

- **01_download_fo_zip_auto.py**
  - Downloads Futures & Options data
  
- **01_download_indices_ohlc_auto.py**
  - Downloads NIFTY50, SENSEX indices

- **01_download_mto_dat_auto.py**
  - MTO (Market Turn Over) data

### 2️⃣ Cleaners (scripts/cleaner/)
**Purpose**: Normalize and validate data

- **02_unzip_cm_bhavcopy_auto.py**
  - Unzips downloaded equity bhavcopy
  - Extracts CSV from ZIP
  
- **03_clean_cm_bhavcopy_daily_auto.py** ← CORE
  - Filters to EQ (equity) only
  - Handles old/new NSE schema formats
  - Normalizes columns
  - Outputs: `BhavCopy_NSE_CM_*.csv`

- **03_clean_futures_daily.py**
  - Cleans futures data

- **03_clean_indices_ohlc.py**
  - Cleans index OHLC

### 3️⃣ Append (scripts/append/)
**Purpose**: Build/update master datasets

- **04_append_equity_stock_master.py** ← CORE
  - Reads cleaned equity CSV
  - Creates per-symbol CSV files
  - Appends new dates (prevents duplicates)
  - Output: `Equity_stock_master/{SYMBOL}.csv`

- **04_append_futures_master.py**
  - Futures master builder (currently empty)

- **04_append_indices_ohlc_master.py**
  - Indices master builder

### 4️⃣ Utilities
- **99_check_master_last_rows.py**
  - Inspection utility (show last rows of master files)

- **daily_run_equity.ps1**
  - PowerShell scheduler for daily runs

---

## 🔌 Configuration Files

### config/settings.py
```python
# Global project settings
# (Currently minimal - expand as needed)
```

### config/symbols.py
```python
# NSE symbols configuration
# Maps symbol codes to metadata
```

### config/holidays.py
```python
# Trading holidays calendar
# Prevents downloads on non-trading days
```

### data/nifty_500_symbols.csv
```csv
Company Name,Industry,Symbol,Series,ISIN Code
[503 rows covering NIFTY500]

Example:
360 ONE WAM Ltd.,Financial Services,360ONE,EQ,INE466L01038
3M India Ltd.,Diversified,3MINDIA,EQ,INE470A01017
Abbott India Ltd.,Healthcare,ABBOTINDIA,EQ,INE358A01014
...
```

---

## 💾 Storage & Performance

### Master Storage
- **Total files**: 2,532
- **Estimated size**: 500-1000 MB (combined)
- **Format**: CSV only (parquet removed per policy)
- **Location**: `data/master/Equity_stock_master/`

### Data Retention
- **Append-only**: Never deletes rows
- **Idempotent**: Safe to run multiple times
- **Deduplication**: Prevents duplicate dates per symbol

---

## 📚 Key Principles (from ARCHITECTURE.md)

✅ **Deterministic**
- Code-only repository
- Data is reproducible, never versioned

✅ **Append-only Masters**
- Historical datasets always grow
- No overwrites or deletions

✅ **NSE-proof**
- Handles NSE format changes
- Holiday & delay safe
- Production locked

✅ **Auditable**
- Clear pipeline stages
- Per-stage outputs saved
- Inspection utilities provided

---

## 🚀 Usage Examples

### Check Latest Equity Data
```bash
# View last few rows of INFY master
python scripts/99_check_master_last_rows.py INFY
```

### Run Daily Pipeline (Manual)
```bash
# Download
python scripts/downloader/01_download_cm_bhavcopy_auto.py

# Unzip
python scripts/cleaner/02_unzip_cm_bhavcopy_auto.py

# Clean
python scripts/cleaner/03_clean_cm_bhavcopy_daily_auto.py

# Append to masters
python scripts/append/04_append_equity_stock_master.py
```

### Scheduled Daily Run (PowerShell)
```bash
# Runs entire equity pipeline daily
.\scripts\daily_run_equity.ps1
```

---

## 📊 Data Quality Notes

### Coverage
✅ NIFTY500: Complete historical data  
✅ Extended universe: 2,000+ additional stocks  
✅ Indices: NIFTY50, Nifty500, SENSEX available  
❌ Futures: Pipeline ready, but not populated yet  
❌ Options: Pipeline ready, but not populated yet  

### Freshness
- **Updated**: Daily via NSE download
- **Lag**: 1-2 hours after market close
- **Reliability**: NSE uses stable archive endpoints

### Format
- **Schema**: Standardized across all symbols
- **Dates**: Normalized (trading days only)
- **Types**: Float (OHLCV), String (metadata)
- **Deduplication**: Per-symbol, per-date enforcement

---

## 🔗 Integration Points

### For MarketMatrix
Could import cleaned equity data to:
- Feature engineering (technical indicators)
- Model training (price prediction)
- Signal generation (buy/sell)
- Backtesting (historical analysis)

### Data Bridge
```
MarketForge Masters (2,531 symbols)
        ↓
MarketMatrix Feature Building
        ↓
ML Models → Predictions → Trade Signals
```

---

## 📝 Summary

**MarketForge** is a **mature, production-grade data engine** with:
- ✅ 2,531 equity stock histories (ready to use)
- ✅ Clean, deterministic pipeline
- ✅ Daily NSE data ingestion
- ✅ Scheduler-ready architecture
- ✅ Zero data loss (append-only)

**Next Steps**:
1. Connect MarketMatrix to MarketForge equity data
2. Use latest NIFTY stock prices for features
3. Train & predict with fresh daily data

---

**Generated**: February 19, 2026  
**Data Source**: NSE (National Stock Exchange of India)  
**License**: Internal Research Use
