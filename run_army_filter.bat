@echo off
echo ========================================
echo Army Contracts Filter Script
echo ========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    pause
    exit /b 1
)

echo Python found and working...
echo.

REM Check if database file exists
if not exist "db.sqlite3" (
    echo Error: Database file db.sqlite3 not found!
    echo Current directory: %CD%
    echo.
    pause
    exit /b 1
)

echo Database file found: db.sqlite3
echo.

REM Check if fixed script exists
if not exist "simple_army_filter_fixed.py" (
    echo Error: simple_army_filter_fixed.py not found!
    echo.
    pause
    exit /b 1
)

echo Running Army Contracts Filter with full data extraction...
echo.

REM Run the fixed Python script with default parameters
python simple_army_filter_fixed.py --output "army_contracts_full_data.xlsx" --min-fields 3

if errorlevel 1 (
    echo.
    echo ========================================
    echo Script failed! Check the error messages above.
    echo ========================================
    pause
    exit /b 1
)

echo.
echo ========================================
echo Script completed successfully!
echo ========================================
echo.
echo Output file: army_contracts_full_data.xlsx
echo.
pause
