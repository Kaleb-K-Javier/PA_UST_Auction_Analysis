# ============================================================================
# PA DEP eFACTS Scraper - Parallel Launcher (Final)
# ============================================================================

# YOUR R PATH (Hardcoded)
$RSCRIPT_PATH = "C:/PROGRA~1/R/R-44~1.3/bin/x64/Rscript.exe"

$SCRIPT_PATH = "R/etl/02b_efacts_scrape.R"
$CHUNK_DIR = "data/external/efacts"
$LOG_DIR = "data/external/efacts"

# Verify R exists before starting
if (-not (Test-Path $RSCRIPT_PATH)) {
    Write-Error "CRITICAL ERROR: Rscript not found at: $RSCRIPT_PATH"
    exit
}

Write-Host "---------------------------------------------------"
Write-Host "Searching for ID chunks in $CHUNK_DIR..."

$chunkFiles = Get-ChildItem -Path $CHUNK_DIR -Filter "ids_chunk_*.txt"

if ($chunkFiles.Count -eq 0) {
    Write-Error "No chunk files found! Run the R generator snippet first."
    exit
}

foreach ($file in $chunkFiles) {
    $workerNum = $file.Name -replace '\D+(\d+)\.txt', '$1'
    $logFileOut = Join-Path $LOG_DIR "worker_${workerNum}_out.log"
    $logFileErr = Join-Path $LOG_DIR "worker_${workerNum}_err.log"
    
    $cleanChunkPath = $file.FullName -replace '\\', '/'
    $cleanScriptPath = $SCRIPT_PATH -replace '\\', '/'
    
    Write-Host "Launching Worker $workerNum processing $($file.Name)..."

    $rCode = "WORKER_ID <- 'worker${workerNum}'; cat(sprintf('Starting Worker %s\n', WORKER_ID)); ids <- scan('${cleanChunkPath}', quiet=TRUE, what=character()); source('${cleanScriptPath}'); run_scraper(ids);"
    
    $argumentList = "-e ""$rCode"""
    
    # Launch Process
    Start-Process -FilePath $RSCRIPT_PATH `
                  -ArgumentList $argumentList `
                  -RedirectStandardOutput $logFileOut `
                  -RedirectStandardError $logFileErr `
                  -WindowStyle Hidden
    
    Write-Host "  -> Out Log: $logFileOut"
}

Write-Host "---------------------------------------------------"
Write-Host "All workers launched! Run this command to watch Worker 1:"
Write-Host "Get-Content data/external/efacts/worker_1_out.log -Wait"