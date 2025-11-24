# Train the ML model separately
# Use this to retrain the model without starting the full system

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Training Anomaly Detection Model" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Activate virtual environment
if (Test-Path "venv") {
    .\venv\Scripts\Activate.ps1
}

Write-Host "This will:" -ForegroundColor Yellow
Write-Host "  1. Generate training log data (30 seconds)" -ForegroundColor White
Write-Host "  2. Process and extract features" -ForegroundColor White
Write-Host "  3. Train KMeans clustering model" -ForegroundColor White
Write-Host "  4. Save model to models/ directory" -ForegroundColor White
Write-Host ""
Write-Host "Press Ctrl+C to cancel, or wait to continue..." -ForegroundColor Yellow
Start-Sleep -Seconds 3

Write-Host ""
Write-Host "Starting training process..." -ForegroundColor Green
Write-Host ""

python -c "from main import AnomalyDetectionSystem; system = AnomalyDetectionSystem(); system.initialize(); system.train_model()"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Training Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
