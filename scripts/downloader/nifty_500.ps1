$OutDir = "H:\MarketForge\data"
$OutFile = "$OutDir\nifty_500_symbols.csv"

if (!(Test-Path $OutDir)) {
    New-Item -ItemType Directory -Path $OutDir | Out-Null
}

$Headers = @{
    "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    "Referer"    = "https://www.niftyindices.com/"
}

Invoke-WebRequest `
    -Uri "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv" `
    -Headers $Headers `
    -OutFile $OutFile `
    -UseBasicParsing

Write-Host "NIFTY 500 CSV DOWNLOADED"
Write-Host "Saved at $OutFile"
