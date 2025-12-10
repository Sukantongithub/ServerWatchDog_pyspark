"""
Configuration file for Log Anomaly Detection System
"""
import os

# Spark Configuration
SPARK_APP_NAME = "LogAnomalyDetection"
SPARK_MASTER = "local[*]"  # Use all available cores
SPARK_UI_PORT = 4040

# Ngrok Configuration
NGROK_AUTH_TOKEN = "355ua2ZoUcz3SY22A82p4d188QY_7akJfpT6RZr2aMzkSG2cD"
NGROK_PORT = 5000

# Log Generation Configuration
LOG_GENERATION_INTERVAL = 1  # seconds
LOG_FILE_PATH = "logs/server_logs.log"
ANOMALY_RATE = 0.05  # 5% of logs will be anomalous

# Model Configuration
MODEL_PATH = "models/anomaly_detection_model"
KMEANS_K = 5
ISOLATION_FOREST_CONTAMINATION = 0.1

# Web Application Configuration
FLASK_HOST = "0.0.0.0"
FLASK_PORT = 5000
FLASK_DEBUG = False  # Changed to False for production stability

# Data Processing Configuration
BATCH_INTERVAL = 5  # seconds
WINDOW_DURATION = 30  # seconds
SLIDE_DURATION = 10  # seconds

# Alert Thresholds
ANOMALY_THRESHOLD = 0.7
CRITICAL_THRESHOLD = 0.9

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
MODEL_DIR = os.path.join(BASE_DIR, "models")
STATIC_DIR = os.path.join(BASE_DIR, "static")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

# Ensure directories exist
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)
os.makedirs(TEMPLATES_DIR, exist_ok=True)
