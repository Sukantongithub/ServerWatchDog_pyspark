# Generate logs only
# Use this to generate sample log data without detection

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Log Generator - Standalone Mode" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Activate virtual environment
if (Test-Path "venv") {
    .\venv\Scripts\Activate.ps1
}

Write-Host "Starting log generation..." -ForegroundColor Green
Write-Host "Logs will be written to: logs/server_logs.log" -ForegroundColor White
Write-Host "Press Ctrl+C to stop" -ForegroundColor Yellow
Write-Host ""

python log_generator.py
