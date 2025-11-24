# Quick Start Script for Windows PowerShell
# Run this script to start the entire Log Anomaly Detection System

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Log Anomaly Detection System - Startup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if virtual environment exists
if (Test-Path "venv") {
    Write-Host "[1/4] Activating virtual environment..." -ForegroundColor Green
    .\venv\Scripts\Activate.ps1
} else {
    Write-Host "[!] Virtual environment not found. Creating one..." -ForegroundColor Yellow
    python -m venv venv
    .\venv\Scripts\Activate.ps1
    Write-Host "[1/4] Installing dependencies..." -ForegroundColor Green
    pip install -r requirements.txt
}

Write-Host ""
Write-Host "[2/4] Checking Java installation..." -ForegroundColor Green
try {
    $javaVersion = java -version 2>&1 | Select-String "version"
    Write-Host "    Java found: $javaVersion" -ForegroundColor Gray
} catch {
    Write-Host "    [!] Java not found! Please install Java 8 or higher." -ForegroundColor Red
    Write-Host "    Download from: https://www.oracle.com/java/technologies/downloads/" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "[3/4] Creating necessary directories..." -ForegroundColor Green
New-Item -ItemType Directory -Force -Path "logs" | Out-Null
New-Item -ItemType Directory -Force -Path "models" | Out-Null
New-Item -ItemType Directory -Force -Path "static" | Out-Null
New-Item -ItemType Directory -Force -Path "templates" | Out-Null
New-Item -ItemType Directory -Force -Path "checkpoint" | Out-Null

Write-Host ""
Write-Host "[4/4] Starting the system..." -ForegroundColor Green
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

python main.py
