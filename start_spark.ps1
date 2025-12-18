# Startup script for Spark-based Anomaly Detection
# Sets JAVA_HOME and runs the application

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Starting Spark Log Anomaly Detection System" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Set JAVA_HOME
$env:JAVA_HOME = "M:\ProgramFiles\jdk17"
$env:HADOOP_HOME = "$PSScriptRoot\.venv\Lib\site-packages\pyspark\bin"
$env:SPARK_HOME = "$PSScriptRoot\.venv\Lib\site-packages\pyspark"
$env:PYSPARK_PYTHON = "$PSScriptRoot\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PSScriptRoot\.venv\Scripts\python.exe"

Write-Host "[1/4] Setting JAVA_HOME to: $env:JAVA_HOME" -ForegroundColor Green
Write-Host "[2/4] Setting SPARK_HOME to: $env:SPARK_HOME" -ForegroundColor Green

# Activate virtual environment  
Write-Host "[3/4] Activating virtual environment..." -ForegroundColor Green
.\.venv\Scripts\Activate.ps1

# Start the application
Write-Host "[4/4] Starting Apache Spark application..." -ForegroundColor Green
Write-Host ""
Write-Host "Dashboard will be available at:" -ForegroundColor Yellow
Write-Host "  - Local: http://localhost:5000" -ForegroundColor White
Write-Host "  - Spark UI: http://localhost:4040" -ForegroundColor White
Write-Host ""

python main_spark.py
