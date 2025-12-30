# MarketForge Architecture

MarketForge is a deterministic, append-only market data engine for NSE markets.

## Core Principles
- Code-only repository
- Data is reproducible, never versioned
- Append-only masters (idempotent)
- NSE format drift safe
- Scheduler-safe (never breaks on holidays)

## Pipeline Flow

Downloader
  ↓
Cleaner
  ↓
Append
  ↓
Master Datasets

## Modules

### 1. Downloader
- Downloads raw NSE data (CM, FO, Indices, MTO)
- Auto / manual modes
- Holiday & delay safe

### 2. Cleaner
- Normalizes NSE formats
- Fixes dates, symbols, headers
- Outputs clean daily CSV/Parquet

### 3. Append
- Appends daily clean data into master datasets
- Deduplicated
- Schema-safe

### 4. Master
- Long-term historical datasets
- Used by strategies, ML, analytics

## What MarketForge Is NOT
- Not a strategy engine
- Not a backtester
- Not a signal generator
