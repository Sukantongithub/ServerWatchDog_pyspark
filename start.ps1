# Quick Start Script for Windows PowerShell
# Run this script to start the entire Log Anomaly Detection System

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Log Anomaly Detection System - Startup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if virtual environment exists
if (Test-Path ".venv") {
    Write-Host "[1/5] Activating virtual environment..." -ForegroundColor Green
    & ".\.venv\Scripts\Activate.ps1"
} elseif (Test-Path "venv") {
    Write-Host "[1/5] Activating virtual environment..." -ForegroundColor Green
    & ".\venv\Scripts\Activate.ps1"
} else {
    Write-Host "[!] Virtual environment not found. Creating one..." -ForegroundColor Yellow
    python -m venv .venv
    & ".\.venv\Scripts\Activate.ps1"
    Write-Host "[1/5] Installing dependencies..." -ForegroundColor Green
    pip install --upgrade pip
    pip install -r requirements.txt
}

Write-Host ""
Write-Host "[2/5] Checking Java installation..." -ForegroundColor Green
try {
    $javaOutput = java -version 2>&1
    $javaVersion = $javaOutput | Select-String "version" | Select-Object -First 1
    Write-Host "    Java found: $javaVersion" -ForegroundColor Gray
} catch {
    Write-Host "    [!] Java not found! Please install Java 8 or higher." -ForegroundColor Red
    Write-Host "    Download from: https://www.oracle.com/java/technologies/downloads/" -ForegroundColor Yellow
    Write-Host "    Or install OpenJDK: https://openjdk.org/" -ForegroundColor Yellow
    Read-Host "Press Enter to continue anyway or Ctrl+C to exit"
}

Write-Host ""
Write-Host "[3/5] Checking Python dependencies..." -ForegroundColor Green
try {
    python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
    python -c "import flask; print(f'Flask version: {flask.__version__}')"
} catch {
    Write-Host "    [!] Missing dependencies. Installing..." -ForegroundColor Yellow
    pip install -r requirements.txt
}

Write-Host ""
Write-Host "[4/5] Creating necessary directories..." -ForegroundColor Green
$directories = @("logs", "models", "static", "templates", "uploads", "checkpoint", "spark-warehouse")
foreach ($dir in $directories) {
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        Write-Host "    Created: $dir" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "[5/5] Starting the system..." -ForegroundColor Green
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "System will start in 3 seconds..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Start-Sleep -Seconds 3

Write-Host ""
Write-Host "Starting main application..." -ForegroundColor Yellow
Write-Host ""
Write-Host "Once started, access the dashboard at:" -ForegroundColor Green
Write-Host "  - Local: http://localhost:5000" -ForegroundColor White
Write-Host "  - Spark UI: http://localhost:4040" -ForegroundColor White
Write-Host "  - Public URL will be displayed by ngrok" -ForegroundColor White
Write-Host ""
Write-Host "Press Ctrl+C to stop the system" -ForegroundColor Yellow
Write-Host ""

# Set environment variables for better Spark performance
$env:PYSPARK_PYTHON = "python"
$env:PYSPARK_DRIVER_PYTHON = "python"

try {
    python main.py
} catch {
    Write-Host ""
    Write-Host "Error starting the application:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host ""
    Write-Host "Troubleshooting tips:" -ForegroundColor Yellow
    Write-Host "1. Ensure Java 8+ is installed" -ForegroundColor White
    Write-Host "2. Check that all dependencies are installed: pip install -r requirements.txt" -ForegroundColor White
    Write-Host "3. Verify Python version (3.8+ required)" -ForegroundColor White
    Write-Host "4. Check firewall settings for ports 5000 and 4040" -ForegroundColor White
    Read-Host "Press Enter to exit"
}
