# ===============================
# MARKETFORGE MASTER PIPELINE (FINAL PRO++)
# ===============================

Write-Host "====================================="
Write-Host " MarketForge | DAILY EQUITY PIPELINE"
Write-Host (" Start Time : {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
Write-Host "====================================="

# --------------------------------------------------
# ENV (FIXED PYTHON PATH)
# --------------------------------------------------
$PYTHON = "H:\envs\trading_env\Scripts\python.exe"
$BASE   = "H:\MarketForge\scripts"

if (!(Test-Path $PYTHON)) {
    Write-Host "[ERROR] Python not found: $PYTHON" -ForegroundColor Red
    exit 1
}

# --------------------------------------------------
# FUNCTION: SAFE RUN STEP
# --------------------------------------------------
function Run-Step {
    param (
        [string]$Title,
        [string]$ScriptPath
    )

    Write-Host ""
    Write-Host ("STEP : {0}" -f $Title)
    Write-Host "-------------------------------------"

    if (!(Test-Path $ScriptPath)) {
        Write-Host "[ERROR] Script not found: $ScriptPath" -ForegroundColor Red
        exit 1
    }

    $stepStart = Get-Date

    & $PYTHON $ScriptPath

    $stepEnd = Get-Date
    $timeTaken = [int]($stepEnd - $stepStart).TotalSeconds

    if ($LASTEXITCODE -ne 0) {
        Write-Host ("FAILED : {0}" -f $Title) -ForegroundColor Red
        exit 1
    }

    Write-Host ("DONE   : {0} (Time: {1}s)" -f $Title, $timeTaken) -ForegroundColor Green
}

# --------------------------------------------------
# DOWNLOAD
# --------------------------------------------------
Run-Step "Download CM Bhavcopy" `
    "$BASE\downloader\01_download_cm_bhavcopy_auto.py"

Run-Step "Download FO ZIP (Derivatives)" `
    "$BASE\downloader\01_download_fo_zip_auto.py"

Run-Step "Download MTO Data" `
    "$BASE\downloader\01_download_mto_dat_auto.py"

Run-Step "Download Index OHLC" `
    "$BASE\downloader\01_download_indices_ohlc_auto.py"

Run-Step "Download FII/DII Activity" `
    "$BASE\downloader\01_download_fii_dii_activity_auto.py"

Run-Step "Download Participant Data" `
    "$BASE\downloader\01_download_participant_auto.py"

# --------------------------------------------------
# UNZIP
# --------------------------------------------------
Run-Step "Unzip CM Bhavcopy" `
    "$BASE\cleaner\02_unzip_cm_bhavcopy_daily.py"

Run-Step "Unzip FO Daily" `
    "$BASE\cleaner\02_unzip_fo_daily.py"

# --------------------------------------------------
# CLEAN
# --------------------------------------------------
Run-Step "Clean CM Equity Bhavcopy (EQ ONLY)" `
    "$BASE\cleaner\03_clean_cm_bhavcopy_daily.py"

Run-Step "Clean Futures Daily" `
    "$BASE\cleaner\03_clean_futures_daily.py"

Run-Step "Clean Options Daily" `
    "$BASE\cleaner\03_clean_options_daily.py"

Run-Step "Clean MTO Daily" `
    "$BASE\cleaner\03_clean_mto_daily.py"

Run-Step "Clean Index OHLC" `
    "$BASE\cleaner\03_clean_indices_ohlc_daily.py"

Run-Step "Clean FII/DII Data" `
    "$BASE\cleaner\03_clean_fii_dii_daily.py"

Run-Step "Clean Participant Data" `
    "$BASE\cleaner\03_clean_participant_daily.py"

# --------------------------------------------------
# APPEND / BUILD MASTER
# --------------------------------------------------
Run-Step "Append Equity Stock Master (Symbolwise)" `
    "$BASE\append\04_append_equity_stock_master.py"

Run-Step "Append Equity MTO Master" `
    "$BASE\append\04_append_equity_mto_master.py"

Run-Step "Append Futures Master" `
    "$BASE\append\04_append_futures_master.py"

Run-Step "Append Options Master" `
    "$BASE\append\04_append_options_master.py"

Run-Step "Append Index OHLC Master (NIFTY)" `
    "$BASE\append\04_append_indices_ohlc_master.py"

Run-Step "Append FII/DII Master" `
    "$BASE\append\04_append_fii_dii_master.py"

Run-Step "Append Participant Master" `
    "$BASE\append\04_append_participant_master.py"

Run-Step "Append Futures_master_three_expiries" `
    "$BASE\append\04_append_futures_master_three_expiries.py"

# --------------------------------------------------
# DATA VALIDATION (FIXED)
# --------------------------------------------------
Write-Host "`n[DATA VALIDATION]" -ForegroundColor Cyan

$paths = @(
    "H:\MarketForge\data\processed\equity_daily",
    "H:\MarketForge\data\processed\futures_daily",
    "H:\MarketForge\data\master\Futures_master"
)

foreach ($p in $paths) {
    if (Test-Path $p) {

        $file = Get-ChildItem $p -File -Recurse |
                Where-Object { $_.Extension -eq ".csv" } |
                Sort-Object LastWriteTime -Descending |
                Select-Object -First 1

        if ($file) {
            Write-Host "[OK] $p → $($file.Name) ($($file.LastWriteTime))" -ForegroundColor Green
        } else {
            Write-Host "[WARNING] No CSV files found in $p" -ForegroundColor Yellow
        }

    } else {
        Write-Host "[ERROR] Missing path $p" -ForegroundColor Red
    }
}

# --------------------------------------------------
# FINAL OUTPUT
# --------------------------------------------------
Write-Host ""
Write-Host "====================================="
Write-Host " PIPELINE COMPLETED SUCCESSFULLY"
Write-Host (" End Time : {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
Write-Host "====================================="