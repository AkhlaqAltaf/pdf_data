# PowerShell script to run Army Contracts Filter with FULL DATA
param(
    [string]$OutputFile = "army_contracts_full_data.xlsx",
    [string]$Keywords = "India Army,HQ,Headquarters,Armd,ARMD,army,ARMY,headquarters",
    [int]$MinFields = 3
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Army Contracts Filter Script - FULL DATA" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Python is available
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "Error: Python is not installed or not in PATH" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

# Check if database file exists
if (-not (Test-Path "db.sqlite3")) {
    Write-Host "Error: Database file db.sqlite3 not found!" -ForegroundColor Red
    Write-Host "Current directory: $(Get-Location)" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Database file found: db.sqlite3" -ForegroundColor Green

# Check if simple script exists
if (-not (Test-Path "simple_army_filter.py")) {
    Write-Host "Error: simple_army_filter.py not found!" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Simple script found: simple_army_filter.py" -ForegroundColor Green
Write-Host ""
Write-Host "Running Army Contracts Filter with FULL DATA extraction..." -ForegroundColor Yellow
Write-Host "Output file: $OutputFile" -ForegroundColor Gray
Write-Host "Keywords: $Keywords" -ForegroundColor Gray
Write-Host "Minimum fields: $MinFields" -ForegroundColor Gray
Write-Host ""

# Run the simple Python script with parameters
try {
    python simple_army_filter.py --output $OutputFile --keywords $Keywords --min-fields $MinFields
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Green
        Write-Host "Script completed successfully!" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
        
        # Check if output file exists
        if (Test-Path $OutputFile) {
            $fileSize = (Get-Item $OutputFile).Length / 1KB
            Write-Host "Output file: $OutputFile" -ForegroundColor Green
            Write-Host "File size: $([math]::Round($fileSize, 1)) KB" -ForegroundColor Green
            
            # Try to open the file
            Write-Host ""
            Write-Host "Opening Excel file..." -ForegroundColor Yellow
            Start-Process $OutputFile
        } else {
            Write-Host "Warning: Output file was not created!" -ForegroundColor Yellow
        }
    } else {
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Red
        Write-Host "Script failed with exit code: $LASTEXITCODE" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
    }
} catch {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "Error running script: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
}

Write-Host ""
Read-Host "Press Enter to exit"
