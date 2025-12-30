Write-Host "====================================="
Write-Host " MarketForge | DAILY EQUITY PIPELINE"
Write-Host (" Start Time : {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
Write-Host "====================================="

# --------------------------------------------------
# ENV
# --------------------------------------------------
$PYTHON = "python"
$BASE   = "H:\MarketForge\scripts"

function Run-Step {
    param (
        [string]$Title,
        [string]$ScriptPath
    )

    Write-Host ""
    Write-Host ("STEP : {0}" -f $Title)
    Write-Host "-------------------------------------"

    & $PYTHON $ScriptPath

    if ($LASTEXITCODE -ne 0) {
        Write-Host ("FAILED : {0}" -f $Title)
        exit 1
    }

    Write-Host ("DONE   : {0}" -f $Title)
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

# --------------------------------------------------
# UNZIP
# --------------------------------------------------
Run-Step "Unzip CM Bhavcopy" `
    "$BASE\cleaner\02_unzip_cm_bhavcopy_auto.py"

Run-Step "Unzip FO Daily" `
    "$BASE\cleaner\02_unzip_fo_daily.py"

# --------------------------------------------------
# CLEAN
# --------------------------------------------------
Run-Step "Clean CM Equity Bhavcopy (EQ ONLY)" `
    "$BASE\cleaner\03_clean_cm_bhavcopy_daily_auto.py"

Run-Step "Clean Futures Daily" `
    "$BASE\cleaner\03_clean_futures_daily.py"

Run-Step "Clean Options Daily" `
    "$BASE\cleaner\03_clean_options_daily.py"

Run-Step "Clean MTO Daily" `
    "$BASE\cleaner\03_clean_mto_daily.py"

Run-Step "Clean Index OHLC" `
    "$BASE\cleaner\03_clean_indices_ohlc.py"

# --------------------------------------------------
# APPEND / BUILD MASTERS
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

# --------------------------------------------------
# DONE
# --------------------------------------------------
Write-Host ""
Write-Host "====================================="
Write-Host " PIPELINE COMPLETED SUCCESSFULLY"
Write-Host (" End Time : {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
Write-Host "====================================="
