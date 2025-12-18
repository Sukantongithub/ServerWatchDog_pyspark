"""
Flask Web Application for Real-time Log Anomaly Detection Dashboard

This module provides the backend for the anomaly detection dashboard. It includes:
- Real-time anomaly updates via Socket.IO
- REST APIs for statistics, recent anomalies, graph data, and anomaly distribution
- File upload and processing capabilities
- System status monitoring
- Integration with Apache Spark

Author: [Your Name]
Date: November 26, 2025
"""
from flask import Flask, render_template, jsonify, request, send_file
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from werkzeug.utils import secure_filename
import threading
import time
import json
import os
from datetime import datetime
from collections import deque
from config import FLASK_HOST, FLASK_PORT, FLASK_DEBUG, SPARK_UI_PORT

# Import Spark components for uploaded dataset analysis
from pyspark.sql import SparkSession
from log_parser import LogParser
from anomaly_detector import AnomalyDetector

app = Flask(__name__)
app.config['SECRET_KEY'] = 'anomaly-detection-secret-key'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = 'uploads'

CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Initialize Spark Session (shared across all uploaded dataset analyses)
spark_session = None
spark_lock = threading.Lock()

def get_spark_session():
    """Get or create Spark session for uploaded dataset analysis"""
    global spark_session
    with spark_lock:
        if spark_session is None:
            print("🚀 Initializing Spark Session for uploaded dataset analysis...")
            spark_session = SparkSession.builder \
                .appName("Uploaded Dataset Anomaly Detection") \
                .master("local[*]") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.ui.port", str(SPARK_UI_PORT)) \
                .config("spark.ui.showConsoleProgress", "true") \
                .getOrCreate()
            
            spark_session.sparkContext.setLogLevel("WARN")
            print(f"✅ Spark Session initialized - Web UI at http://localhost:{SPARK_UI_PORT}")
        
        return spark_session

# Store recent anomalies and statistics
recent_anomalies = deque(maxlen=100)
statistics = {
    "total_logs": 0,
    "total_anomalies": 0,
    "anomaly_rate": 0.0,
    "critical_alerts": 0,
    "services_affected": set(),
    "suspicious_ips": set()
}

# ========== UPLOADED DATASET ANALYSIS SECTION ==========
# Separate tracking for uploaded dataset analysis
uploaded_dataset_results = {}  # Store results by file_id
uploaded_anomalies = {}  # Store anomalies by file_id
uploaded_alerts = {}  # Store alerts by file_id
uploaded_statistics = {}  # Store statistics by file_id

# Active dataset analysis tracking
active_analyses = {}  # Track ongoing analyses

class UploadedDatasetAnalyzer:
    """Handles analysis of uploaded log datasets using Spark MLlib"""
    
    @staticmethod
    def generate_file_id(filepath):
        """Generate unique ID for uploaded file"""
        filename = os.path.basename(filepath)
        timestamp = os.path.getmtime(filepath)
        return f"{filename}_{int(timestamp)}"
    
    @staticmethod
    def analyze_uploaded_logs_with_spark(filepath, file_id, options=None):
        """
        Analyze uploaded log file for anomalies using Spark MLlib
        This makes the processing visible in Spark Web UI
        """
        if options is None:
            options = {}
        
        use_spark_mllib = options.get('use_spark_mllib', True)
        generate_alerts = options.get('generate_alerts', True)
        anomaly_threshold = options.get('anomaly_threshold', 0.4)
        
        try:
            print(f"\n{'='*80}")
            print(f"🔍 Starting Spark-based analysis for: {os.path.basename(filepath)}")
            print(f"📊 Options: MLlib={use_spark_mllib}, Alerts={generate_alerts}, Threshold={anomaly_threshold}")
            print(f"{'='*80}\n")
            
            start_time = time.time()
            
            # Get Spark session
            spark = get_spark_session()
            
            # Update job description for Spark UI
            spark.sparkContext.setJobDescription(f"Analyzing Uploaded Dataset: {os.path.basename(filepath)}")
            
            # Read log file into Spark DataFrame
            print("📖 Reading log file into Spark DataFrame...")
            log_df = spark.read.text(filepath)
            total_logs = log_df.count()
            print(f"✅ Loaded {total_logs} log entries")
            
            # Emit progress update
            socketio.emit('dataset_analysis_progress', {
                'file_id': file_id,
                'progress': 20,
                'status': f'Loaded {total_logs} log entries',
                'details': 'Parsing log entries...'
            })
            
            if use_spark_mllib:
                # Use Spark MLlib pipeline
                anomalies, alerts, stats = UploadedDatasetAnalyzer._analyze_with_spark_mllib(
                    spark, log_df, file_id, filepath, total_logs, anomaly_threshold, generate_alerts
                )
            else:
                # Use pattern-based analysis (still with Spark for processing)
                anomalies, alerts, stats = UploadedDatasetAnalyzer._analyze_with_patterns(
                    spark, log_df, file_id, filepath, total_logs, anomaly_threshold, generate_alerts
                )
            
            # Calculate processing time
            processing_time = round(time.time() - start_time, 2)
            stats["processing_time"] = processing_time
            stats["analysis_method"] = "Spark MLlib" if use_spark_mllib else "Pattern Matching (Spark-accelerated)"
            
            # Convert sets to lists for JSON serialization
            if isinstance(stats.get("services_analyzed"), set):
                stats["services_analyzed"] = list(stats["services_analyzed"])
            if isinstance(stats.get("suspicious_ips_found"), set):
                stats["suspicious_ips_found"] = list(stats["suspicious_ips_found"])
            
            # Store results
            uploaded_dataset_results[file_id] = {
                "filepath": filepath,
                "filename": os.path.basename(filepath),
                "analysis_completed": datetime.now().isoformat(),
                "stats": stats,
                "status": "completed",
                "spark_job_url": f"http://localhost:{SPARK_UI_PORT}"
            }
            
            uploaded_anomalies[file_id] = anomalies
            uploaded_alerts[file_id] = alerts
            uploaded_statistics[file_id] = stats
            
            print(f"\n{'='*80}")
            print(f"✅ Analysis Complete!")
            print(f"📊 Total Logs: {stats['total_logs']}")
            print(f"🚨 Anomalies Found: {stats['anomalies_found']}")
            print(f"⚠️  Critical Alerts: {stats['critical_alerts']}")
            print(f"⏱️  Processing Time: {processing_time}s")
            print(f"🌐 View Spark Jobs: http://localhost:{SPARK_UI_PORT}")
            print(f"{'='*80}\n")
            
            return True, stats, anomalies, alerts
            
        except Exception as e:
            print(f"❌ Error in Spark analysis: {str(e)}")
            import traceback
            traceback.print_exc()
            return False, str(e), [], []
    
    @staticmethod
    def _analyze_with_spark_mllib(spark, log_df, file_id, filepath, total_logs, threshold, generate_alerts):
        """Analyze using Spark MLlib KMeans clustering"""
        print("🤖 Using Spark MLlib for anomaly detection...")
        
        # Update Spark job description
        spark.sparkContext.setJobDescription(f"MLlib Analysis: {os.path.basename(filepath)} - Parsing Logs")
        
        # Parse logs
        parser = LogParser(spark)
        parsed_df = parser.parse_log_line(log_df)
        
        socketio.emit('dataset_analysis_progress', {
            'file_id': file_id,
            'progress': 40,
            'status': 'Extracting features',
            'details': 'Creating feature vectors for ML model...'
        })
        
        # Extract features
        spark.sparkContext.setJobDescription(f"MLlib Analysis: {os.path.basename(filepath)} - Feature Extraction")
        feature_df = parser.extract_features(parsed_df)
        
        # Create feature vectors and scale
        spark.sparkContext.setJobDescription(f"MLlib Analysis: {os.path.basename(filepath)} - Feature Scaling")
        scaled_df, scaler_model = parser.create_feature_vector(feature_df)
        
        socketio.emit('dataset_analysis_progress', {
            'file_id': file_id,
            'progress': 60,
            'status': 'Training ML model',
            'details': 'Running KMeans clustering...'
        })
        
        # Train and detect anomalies
        spark.sparkContext.setJobDescription(f"MLlib Analysis: {os.path.basename(filepath)} - Anomaly Detection")
        detector = AnomalyDetector(spark)
        detector.train_kmeans(scaled_df, k=5)
        
        anomaly_df = detector.ensemble_anomaly_detection(scaled_df)
        
        socketio.emit('dataset_analysis_progress', {
            'file_id': file_id,
            'progress': 80,
            'status': 'Processing results',
            'details': 'Collecting anomalies and generating alerts...'
        })
        
        # Add line numbers using monotonically_increasing_id
        from pyspark.sql.functions import monotonically_increasing_id
        anomaly_df = anomaly_df.withColumn("line_number", monotonically_increasing_id() + 1)
        
        # Collect results
        spark.sparkContext.setJobDescription(f"MLlib Analysis: {os.path.basename(filepath)} - Collecting Results")
        results = anomaly_df.filter(anomaly_df.final_anomaly_score >= threshold).collect()
        
        # Convert to anomalies and alerts
        anomalies = []
        alerts = []
        stats = UploadedDatasetAnalyzer._initialize_stats(total_logs)
        
        for row in results:
            anomaly_data = {
                'timestamp': str(row.timestamp) if row.timestamp else datetime.now().isoformat(),
                'level': row.level if hasattr(row, 'level') else 'UNKNOWN',
                'service': row.service if hasattr(row, 'service') else 'unknown',
                'message': row.message if hasattr(row, 'message') else '',
                'ip_address': row.ip_address if hasattr(row, 'ip_address') else 'N/A',
                'user': row.user if hasattr(row, 'user') else 'N/A',
                'response_time': row.response_time if hasattr(row, 'response_time') else 0,
                'anomaly_score': float(row.final_anomaly_score),
                'file_id': file_id,
                'analysis_type': 'spark_mllib',
                'cluster': int(row.cluster) if hasattr(row, 'cluster') else None,
                'line_number': int(row.line_number) if hasattr(row, 'line_number') else 0
            }
            
            anomalies.append(anomaly_data)
            
            # Update statistics
            UploadedDatasetAnalyzer._update_stats(stats, anomaly_data)
            
            # Generate alerts
            if generate_alerts and anomaly_data['anomaly_score'] >= 0.5:
                alert = UploadedDatasetAnalyzer.create_alert(anomaly_data, anomaly_data['anomaly_score'])
                if alert:
                    alerts.append(alert)
                    UploadedDatasetAnalyzer._update_alert_stats(stats, alert)
        
        stats["anomalies_found"] = len(anomalies)
        
        return anomalies, alerts, stats
    
    @staticmethod
    def _analyze_with_patterns(spark, log_df, file_id, filepath, total_logs, threshold, generate_alerts):
        """Analyze using pattern-based detection (Spark-accelerated)"""
        print("🔍 Using pattern-based analysis with Spark acceleration...")
        
        from pyspark.sql.functions import col, lower, when, lit, monotonically_increasing_id
        
        # Update Spark job description
        spark.sparkContext.setJobDescription(f"Pattern Analysis: {os.path.basename(filepath)} - Detecting Patterns")
        
        socketio.emit('dataset_analysis_progress', {
            'file_id': file_id,
            'progress': 50,
            'status': 'Pattern detection',
            'details': 'Analyzing log patterns with Spark...'
        })
        
        # Parse logs first
        parser = LogParser(spark)
        parsed_df = parser.parse_log_line(log_df)
        
        # Add line numbers
        parsed_df = parsed_df.withColumn("line_number", monotonically_increasing_id() + 1)
        
        # Add anomaly scoring based on patterns
        error_keywords = ['timeout', 'error', 'exception', 'failed', 'critical', 
                         'denied', 'exhausted', 'deadlock', 'breach', 'attack']
        
        # Calculate anomaly score using Spark SQL
        anomaly_expr = lit(0.0)
        
        # Level-based scoring
        anomaly_expr = when(col("level") == "ERROR", anomaly_expr + 0.4) \
                      .when(col("level") == "WARN", anomaly_expr + 0.2) \
                      .otherwise(anomaly_expr)
        
        # Response time scoring
        anomaly_expr = when(col("response_time") > 5000, anomaly_expr + 0.3) \
                      .when(col("response_time") > 1000, anomaly_expr + 0.1) \
                      .otherwise(anomaly_expr)
        
        # Message pattern scoring
        message_lower = lower(col("message"))
        for keyword in error_keywords:
            anomaly_expr = when(message_lower.contains(keyword), anomaly_expr + 0.2) \
                          .otherwise(anomaly_expr)
        
        parsed_df = parsed_df.withColumn("anomaly_score", anomaly_expr)
        
        # Filter anomalies
        anomaly_df = parsed_df.filter(col("anomaly_score") >= threshold)
        
        socketio.emit('dataset_analysis_progress', {
            'file_id': file_id,
            'progress': 80,
            'status': 'Collecting results',
            'details': 'Gathering anomalies from Spark...'
        })
        
        # Collect results
        results = anomaly_df.collect()
        
        # Convert to anomalies and alerts
        anomalies = []
        alerts = []
        stats = UploadedDatasetAnalyzer._initialize_stats(total_logs)
        
        for row in results:
            anomaly_data = {
                'timestamp': str(row.timestamp) if row.timestamp else datetime.now().isoformat(),
                'level': row.level if hasattr(row, 'level') else 'UNKNOWN',
                'service': row.service if hasattr(row, 'service') else 'unknown',
                'message': row.message if hasattr(row, 'message') else '',
                'ip_address': row.ip_address if hasattr(row, 'ip_address') else 'N/A',
                'user': row.user if hasattr(row, 'user') else 'N/A',
                'response_time': row.response_time if hasattr(row, 'response_time') else 0,
                'anomaly_score': float(row.anomaly_score),
                'file_id': file_id,
                'analysis_type': 'pattern_spark',
                'line_number': int(row.line_number) if hasattr(row, 'line_number') else 0
            }
            
            anomalies.append(anomaly_data)
            UploadedDatasetAnalyzer._update_stats(stats, anomaly_data)
            
            # Generate alerts
            if generate_alerts and anomaly_data['anomaly_score'] >= 0.5:
                alert = UploadedDatasetAnalyzer.create_alert(anomaly_data, anomaly_data['anomaly_score'])
                if alert:
                    alerts.append(alert)
                    UploadedDatasetAnalyzer._update_alert_stats(stats, alert)
        
        stats["anomalies_found"] = len(anomalies)
        
        return anomalies, alerts, stats
    
    @staticmethod
    def _initialize_stats(total_logs):
        """Initialize statistics dictionary"""
        return {
            "total_logs": total_logs,
            "anomalies_found": 0,
            "critical_alerts": 0,
            "high_risk_alerts": 0,
            "medium_risk_alerts": 0,
            "low_risk_alerts": 0,
            "services_analyzed": [],  # Changed from set() to list
            "suspicious_ips_found": [],  # Changed from set() to list
            "error_patterns": {},
            "time_range": {"start": None, "end": None},
            "processing_time": 0
        }
    
    @staticmethod
    def _update_stats(stats, anomaly_data):
        """Update statistics with anomaly data"""
        if anomaly_data.get('service'):
            service = anomaly_data['service']
            if service not in stats["services_analyzed"]:
                stats["services_analyzed"].append(service)
        
        if anomaly_data.get('ip_address'):
            ip = anomaly_data['ip_address']
            if ip not in stats["suspicious_ips_found"]:
                stats["suspicious_ips_found"].append(ip)
        
        # Track time range
        if anomaly_data.get('timestamp'):
            if not stats["time_range"]["start"]:
                stats["time_range"]["start"] = anomaly_data['timestamp']
            stats["time_range"]["end"] = anomaly_data['timestamp']
        
        # Track error patterns
        if anomaly_data.get('level') == 'ERROR':
            error_type = anomaly_data.get('message', 'Unknown Error')[:50]
            stats["error_patterns"][error_type] = stats["error_patterns"].get(error_type, 0) + 1
    
    @staticmethod
    def _update_alert_stats(stats, alert):
        """Update alert statistics"""
        severity = alert.get('severity', '').upper()
        if severity == 'CRITICAL':
            stats["critical_alerts"] += 1
        elif severity == 'HIGH':
            stats["high_risk_alerts"] += 1
        elif severity == 'MEDIUM':
            stats["medium_risk_alerts"] += 1
        else:
            stats["low_risk_alerts"] += 1
    
    @staticmethod
    def analyze_uploaded_logs(filepath, file_id):
        """Legacy pattern-based analysis (fallback)"""
        # ...existing code...
        pass
    
    # ...existing helper methods (parse_log_line, calculate_anomaly_score, create_alert)...
    
    @staticmethod
    def create_alert(anomaly_data, anomaly_score):
        """Create alert based on anomaly data"""
        if anomaly_score < 0.5:
            return None
        
        # Determine severity
        if anomaly_score >= 0.9:
            severity = 'CRITICAL'
            priority = 1
        elif anomaly_score >= 0.7:
            severity = 'HIGH'
            priority = 2
        elif anomaly_score >= 0.5:
            severity = 'MEDIUM'
            priority = 3
        else:
            severity = 'LOW'
            priority = 4
        
        # Generate alert message
        service = anomaly_data.get('service', 'Unknown')
        message = anomaly_data.get('message', 'Unknown error')
        ip = anomaly_data.get('ip_address', 'Unknown')
        
        alert_message = f"{severity} anomaly in {service}: {message[:100]}"
        
        return {
            'id': f"alert_{int(time.time() * 1000)}_{anomaly_data.get('line_number', 0)}",
            'severity': severity,
            'priority': priority,
            'message': alert_message,
            'service': service,
            'ip_address': ip,
            'timestamp': anomaly_data.get('timestamp', datetime.now().isoformat()),
            'anomaly_score': anomaly_score,
            'line_number': anomaly_data.get('line_number'),
            'file_id': anomaly_data.get('file_id'),
            'created_at': datetime.now().isoformat(),
            'details': {
                'level': anomaly_data.get('level'),
                'user': anomaly_data.get('user'),
                'response_time': anomaly_data.get('response_time'),
                'analysis_type': anomaly_data.get('analysis_type', 'unknown')
            }
        }

# System status tracking
system_status = {
    "is_running": False,
    "start_time": None,
    "uptime": 0,
    "spark_status": "Offline",
    "logs_processed_today": 0,
    "current_processing": False,
    "last_anomaly": None
}

# Lock for thread-safe operations
stats_lock = threading.Lock()

# Create upload directory
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html', spark_ui_port=SPARK_UI_PORT)

@app.route('/api/stats')
def get_stats():
    """Get current statistics"""
    with stats_lock:
        return jsonify({
            "total_logs": statistics["total_logs"],
            "total_anomalies": statistics["total_anomalies"],
            "anomaly_rate": statistics["anomaly_rate"],
            "critical_alerts": statistics["critical_alerts"],
            "services_affected": list(statistics["services_affected"]),
            "suspicious_ips": list(statistics["suspicious_ips"])
        })

@app.route('/api/recent_anomalies')
def get_recent_anomalies():
    """Get recent anomaly detections"""
    with stats_lock:
        return jsonify(list(recent_anomalies))

@app.route('/api/graph_data')
def get_graph_data():
    """Get graph data for network visualization"""
    try:
        with open('static/graph_data.json', 'r') as f:
            graph_data = json.load(f)
        return jsonify(graph_data)
    except FileNotFoundError:
        return jsonify({"nodes": [], "links": []})
    except json.JSONDecodeError:
        return jsonify({"error": "Malformed graph data"}), 400

@app.route('/api/timeline_data')
def get_timeline_data():
    """Get timeline data for anomaly trends"""
    with stats_lock:
        # Group anomalies by time
        timeline = {}
        for anomaly in recent_anomalies:
            timestamp = anomaly.get('timestamp', '')[:16]  # Group by minute
            if timestamp not in timeline:
                timeline[timestamp] = 0
            timeline[timestamp] += 1
        
        return jsonify({
            "timestamps": list(timeline.keys()),
            "counts": list(timeline.values())
        })

@app.route('/api/anomaly_distribution')
def get_anomaly_distribution():
    """Get anomaly distribution by service"""
    with stats_lock:
        # Aggregate anomalies by service
        service_counts = {}
        for anomaly in recent_anomalies:
            service = anomaly.get('service', 'Unknown')
            service_counts[service] = service_counts.get(service, 0) + 1

        # Prepare data for pie chart
        services = list(service_counts.keys())
        counts = list(service_counts.values())

        return jsonify({
            "services": services,
            "counts": counts
        })

@app.route('/api/system_status')
def get_system_status():
    """Get current system status"""
    with stats_lock:
        status = system_status.copy()
        # Calculate uptime if running
        if status["is_running"] and status["start_time"]:
            status["uptime"] = int(time.time() - status["start_time"])
        status["services_affected"] = list(statistics["services_affected"])
        status["suspicious_ips"] = list(statistics["suspicious_ips"])
        return jsonify(status)

@app.route('/api/system/start', methods=['POST'])
def start_system():
    """Start the anomaly detection system"""
    with stats_lock:
        if not system_status["is_running"]:
            system_status["is_running"] = True
            system_status["start_time"] = time.time()
            system_status["spark_status"] = "Online"
            socketio.emit('system_status_update', system_status)
            return jsonify({"status": "success", "message": "System started"})
        return jsonify({"status": "info", "message": "System already running"}), 409

@app.route('/api/system/stop', methods=['POST'])
def stop_system():
    """Stop the anomaly detection system"""
    with stats_lock:
        if system_status["is_running"]:
            system_status["is_running"] = False
            system_status["spark_status"] = "Offline"
            socketio.emit('system_status_update', system_status)
            return jsonify({"status": "success", "message": "System stopped"})
        return jsonify({"status": "info", "message": "System not running"}), 409

@app.route('/api/system/status', methods=['GET'])
def system_info():
    """Get detailed system information"""
    with stats_lock:
        return jsonify({
            "is_running": system_status["is_running"],
            "uptime": system_status["uptime"],
            "spark_status": system_status["spark_status"],
            "logs_processed_today": system_status["logs_processed_today"],
            "current_processing": system_status["current_processing"],
            "last_anomaly": system_status["last_anomaly"],
            "total_logs": statistics["total_logs"],
            "total_anomalies": statistics["total_anomalies"],
            "anomaly_rate": statistics["anomaly_rate"]
        })

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Upload and process log file"""
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    if not file.filename.endswith(('.log', '.txt')):
        return jsonify({"error": "Only .log and .txt files are allowed"}), 400
    
    try:
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{timestamp}_{filename}")
        file.save(filepath)
        
        # Emit processing started event
        socketio.emit('file_upload_started', {
            "filename": filename,
            "timestamp": timestamp,
            "filepath": filepath
        })
        
        return jsonify({
            "status": "success",
            "message": "File uploaded successfully",
            "filepath": filepath
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/process_log', methods=['POST'])
def process_log():
    """Process uploaded or existing log file"""
    data = request.get_json()
    filepath = data.get('filepath', 'logs/server_logs.log')
    
    if not os.path.exists(filepath):
        return jsonify({"error": "File not found"}), 404
    
    try:
        with stats_lock:
            system_status["current_processing"] = True
            socketio.emit('processing_started', {"filepath": filepath})
        
        # Simulate processing (actual processing would be done here)
        line_count = 0
        with open(filepath, 'r') as f:
            line_count = sum(1 for _ in f)
        
        with stats_lock:
            system_status["current_processing"] = False
            system_status["logs_processed_today"] += line_count
            socketio.emit('processing_completed', {
                "filepath": filepath,
                "lines_processed": line_count
            })
        
        return jsonify({
            "status": "success",
            "message": f"Processed {line_count} log lines",
            "lines_processed": line_count
        }), 200
    
    except Exception as e:
        with stats_lock:
            system_status["current_processing"] = False
        return jsonify({"error": str(e)}), 500

@app.route('/api/spark_ui')
def spark_ui_link():
    """Get Spark UI link"""
    return jsonify({
        "spark_ui_url": f"http://localhost:{SPARK_UI_PORT}",
        "port": SPARK_UI_PORT
    })

@app.route('/api/logs')
def get_logs():
    """Get list of all generated log files with metadata"""
    try:
        logs_dir = 'logs'
        logs_list = []
        
        # Check both uploads and logs directories
        for directory in [app.config['UPLOAD_FOLDER'], logs_dir]:
            if os.path.exists(directory):
                for filename in os.listdir(directory):
                    filepath = os.path.join(directory, filename)
                    
                    # Only include files
                    if os.path.isfile(filepath):
                        try:
                            # Get file stats
                            file_stat = os.stat(filepath)
                            size_bytes = file_stat.st_size
                            created_timestamp = file_stat.st_mtime
                            
                            # Format size
                            if size_bytes < 1024:
                                size_str = f"{size_bytes} B"
                            elif size_bytes < 1024 * 1024:
                                size_str = f"{size_bytes / 1024:.2f} KB"
                            else:
                                size_str = f"{size_bytes / (1024 * 1024):.2f} MB"
                            
                            # Format creation date
                            created_date = datetime.fromtimestamp(created_timestamp).strftime("%Y-%m-%d %H:%M:%S")
                            
                            logs_list.append({
                                "filename": filename,
                                "size": size_str,
                                "created": created_date,
                                "download_url": f"/api/logs/download/{filename}",
                                "view_url": f"/api/logs/view/{filename}",
                                "path": filepath
                            })
                        except Exception as e:
                            print(f"Error processing file {filename}: {e}")
                            continue
        
        # Sort by creation date (newest first)
        logs_list.sort(key=lambda x: x['created'], reverse=True)
        
        return jsonify(logs_list)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/logs/download/<filename>')
def download_log(filename):
    """Download a log file"""
    try:
        # Prevent directory traversal attacks
        if '..' in filename or '/' in filename or '\\' in filename:
            return jsonify({"error": "Invalid filename"}), 400
        
        # Check both directories
        for directory in [app.config['UPLOAD_FOLDER'], 'logs']:
            filepath = os.path.join(directory, filename)
            if os.path.exists(filepath) and os.path.isfile(filepath):
                return send_file(filepath, as_attachment=True, download_name=filename)
        
        return jsonify({"error": "File not found"}), 404
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/logs/view/<filename>')
def view_log(filename):
    """View a log file content"""
    try:
        # Prevent directory traversal attacks
        if '..' in filename or '/' in filename or '\\' in filename:
            return jsonify({"error": "Invalid filename"}), 400
        
        # Check both directories
        for directory in [app.config['UPLOAD_FOLDER'], 'logs']:
            filepath = os.path.join(directory, filename)
            if os.path.exists(filepath) and os.path.isfile(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    return jsonify({
                        "filename": filename,
                        "content": content,
                        "lines": len(content.split('\n'))
                    })
                except Exception as e:
                    return jsonify({"error": f"Could not read file: {str(e)}"}), 500
        
        return jsonify({"error": "File not found"}), 404
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ========== UPLOADED DATASET ANALYSIS API ENDPOINTS ==========

@app.route('/api/uploaded/analyze', methods=['POST'])
def analyze_uploaded_dataset():
    """Analyze uploaded dataset for anomalies and generate alerts"""
    data = request.get_json()
    filepath = data.get('filepath')
    options = data.get('options', {})
    
    if not filepath or not os.path.exists(filepath):
        return jsonify({"error": "Invalid file path"}), 400
    
    try:
        # Generate file ID
        file_id = UploadedDatasetAnalyzer.generate_file_id(filepath)
        
        # Mark analysis as active
        active_analyses[file_id] = {
            "status": "processing",
            "start_time": time.time(),
            "filepath": filepath,
            "filename": os.path.basename(filepath)
        }
        
        # Start analysis in background thread
        def analyze_in_background():
            try:
                socketio.emit('dataset_analysis_started', {
                    "file_id": file_id,
                    "filename": os.path.basename(filepath),
                    "status": "processing"
                })
                
                # Use Spark-based analysis with user options
                success, stats, anomalies, alerts = UploadedDatasetAnalyzer.analyze_uploaded_logs_with_spark(
                    filepath, file_id, options
                )
                
                if success:
                    active_analyses[file_id]["status"] = "completed"
                    socketio.emit('dataset_analysis_completed', {
                        "file_id": file_id,
                        "stats": stats,
                        "anomalies_count": len(anomalies),
                        "alerts_count": len(alerts),
                        "status": "completed",
                        "method": stats.get('analysis_method', 'Spark MLlib')
                    })
                else:
                    active_analyses[file_id]["status"] = "failed"
                    active_analyses[file_id]["error"] = stats
                    socketio.emit('dataset_analysis_failed', {
                        "file_id": file_id,
                        "error": stats,
                        "status": "failed"
                    })
                    
            except Exception as e:
                active_analyses[file_id]["status"] = "failed"
                active_analyses[file_id]["error"] = str(e)
                socketio.emit('dataset_analysis_failed', {
                    "file_id": file_id,
                    "error": str(e),
                    "status": "failed"
                })
        
        analysis_thread = threading.Thread(target=analyze_in_background, daemon=True)
        analysis_thread.start()
        
        return jsonify({
            "status": "success",
            "message": "Analysis started with Spark",
            "file_id": file_id,
            "filename": os.path.basename(filepath),
            "spark_ui_url": f"http://localhost:{SPARK_UI_PORT}"
        }), 202
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/uploaded/results/<file_id>')
def get_uploaded_analysis_results(file_id):
    """Get analysis results for uploaded dataset"""
    if file_id not in uploaded_dataset_results:
        return jsonify({"error": "Analysis not found"}), 404
    
    results = uploaded_dataset_results[file_id]
    stats = uploaded_statistics.get(file_id, {})
    
    return jsonify({
        "file_id": file_id,
        "results": results,
        "statistics": stats,
        "anomalies_count": len(uploaded_anomalies.get(file_id, [])),
        "alerts_count": len(uploaded_alerts.get(file_id, []))
    })

@app.route('/api/uploaded/anomalies/<file_id>')
def get_uploaded_anomalies(file_id):
    """Get anomalies found in uploaded dataset"""
    if file_id not in uploaded_anomalies:
        return jsonify({"error": "Analysis not found"}), 404
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    severity_filter = request.args.get('severity', None)
    service_filter = request.args.get('service', None)
    
    anomalies = uploaded_anomalies[file_id]
    
    # Apply filters
    filtered_anomalies = anomalies
    if severity_filter:
        filtered_anomalies = [a for a in filtered_anomalies if a.get('anomaly_score', 0) >= float(severity_filter)]
    if service_filter:
        filtered_anomalies = [a for a in filtered_anomalies if a.get('service') == service_filter]
    
    # Pagination
    total = len(filtered_anomalies)
    start = (page - 1) * per_page
    end = start + per_page
    paginated_anomalies = filtered_anomalies[start:end]
    
    return jsonify({
        "file_id": file_id,
        "anomalies": paginated_anomalies,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page
        }
    })

@app.route('/api/uploaded/alerts/<file_id>')
def get_uploaded_alerts(file_id):
    """Get alerts generated from uploaded dataset analysis"""
    if file_id not in uploaded_alerts:
        return jsonify({"error": "Analysis not found"}), 404
    
    severity_filter = request.args.get('severity', None)
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    
    alerts = uploaded_alerts[file_id]
    
    # Apply severity filter
    if severity_filter:
        alerts = [a for a in alerts if a.get('severity') == severity_filter.upper()]
    
    # Sort by priority and timestamp
    alerts.sort(key=lambda x: (x.get('priority', 999), x.get('timestamp', '')), reverse=True)
    
    # Pagination
    total = len(alerts)
    start = (page - 1) * per_page
    end = start + per_page
    paginated_alerts = alerts[start:end]
    
    return jsonify({
        "file_id": file_id,
        "alerts": paginated_alerts,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page
        }
    })

@app.route('/api/uploaded/statistics/<file_id>')
def get_uploaded_statistics(file_id):
    """Get detailed statistics for uploaded dataset analysis"""
    if file_id not in uploaded_statistics:
        return jsonify({"error": "Analysis not found"}), 404
    
    stats = uploaded_statistics[file_id]
    
    return jsonify({
        "file_id": file_id,
        "statistics": stats,
        "summary": {
            "anomaly_rate": round(stats.get('anomalies_found', 0) / max(stats.get('total_logs', 1), 1) * 100, 2),
            "critical_rate": round(stats.get('critical_alerts', 0) / max(stats.get('anomalies_found', 1), 1) * 100, 2),
            "services_count": len(stats.get('services_analyzed', [])),
            "suspicious_ips_count": len(stats.get('suspicious_ips_found', [])),
            "error_patterns_count": len(stats.get('error_patterns', {}))
        }
    })

@app.route('/api/uploaded/timeline/<file_id>')
def get_uploaded_timeline(file_id):
    """Get timeline data for uploaded dataset anomalies"""
    if file_id not in uploaded_anomalies:
        return jsonify({"error": "Analysis not found"}), 404
    
    anomalies = uploaded_anomalies[file_id]
    
    # Group anomalies by hour
    timeline = {}
    for anomaly in anomalies:
        timestamp = anomaly.get('timestamp', '')
        if timestamp:
            hour_key = timestamp[:13]  # YYYY-MM-DD HH
            if hour_key not in timeline:
                timeline[hour_key] = {"count": 0, "critical": 0, "high": 0, "medium": 0, "low": 0}
            
            timeline[hour_key]["count"] += 1
            
            # Categorize by severity
            score = anomaly.get('anomaly_score', 0)
            if score >= 0.9:
                timeline[hour_key]["critical"] += 1
            elif score >= 0.7:
                timeline[hour_key]["high"] += 1
            elif score >= 0.5:
                timeline[hour_key]["medium"] += 1
            else:
                timeline[hour_key]["low"] += 1
    
    # Sort by timestamp
    sorted_timeline = dict(sorted(timeline.items()))
    
    return jsonify({
        "file_id": file_id,
        "timeline": sorted_timeline,
        "timestamps": list(sorted_timeline.keys()),
        "total_periods": len(sorted_timeline)
    })

@app.route('/api/uploaded/distribution/<file_id>')
def get_uploaded_distribution(file_id):
    """Get service and severity distribution for uploaded dataset"""
    if file_id not in uploaded_anomalies:
        return jsonify({"error": "Analysis not found"}), 404
    
    anomalies = uploaded_anomalies[file_id]
    
    # Service distribution
    service_dist = {}
    severity_dist = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    ip_dist = {}
    
    for anomaly in anomalies:
        # Service distribution
        service = anomaly.get('service', 'Unknown')
        service_dist[service] = service_dist.get(service, 0) + 1
        
        # Severity distribution
        score = anomaly.get('anomaly_score', 0)
        if score >= 0.9:
            severity_dist["critical"] += 1
        elif score >= 0.7:
            severity_dist["high"] += 1
        elif score >= 0.5:
            severity_dist["medium"] += 1
        else:
            severity_dist["low"] += 1
        
        # IP distribution (top suspicious IPs)
        ip = anomaly.get('ip_address', 'Unknown')
        ip_dist[ip] = ip_dist.get(ip, 0) + 1
    
    # Get top 10 IPs
    top_ips = dict(sorted(ip_dist.items(), key=lambda x: x[1], reverse=True)[:10])
    
    return jsonify({
        "file_id": file_id,
        "service_distribution": service_dist,
        "severity_distribution": severity_dist,
        "ip_distribution": top_ips
    })

@app.route('/api/uploaded/logs/<file_id>')
def get_uploaded_logs_view(file_id):
    """Get paginated log view for uploaded dataset with anomaly highlighting"""
    if file_id not in uploaded_dataset_results:
        return jsonify({"error": "Analysis not found"}), 404
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
    show_anomalies_only = request.args.get('anomalies_only', 'false').lower() == 'true'
    
    filepath = uploaded_dataset_results[file_id]['filepath']
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            all_lines = f.readlines()
        
        # Get anomaly line numbers for highlighting
        anomalies = uploaded_anomalies.get(file_id, [])
        anomaly_lines = {a.get('line_number'): a for a in anomalies}
        
        # Prepare log entries
        log_entries = []
        for i, line in enumerate(all_lines, 1):
            line = line.strip()
            if not line:
                continue
            
            is_anomaly = i in anomaly_lines
            
            if show_anomalies_only and not is_anomaly:
                continue
            
            entry = {
                "line_number": i,
                "content": line,
                "is_anomaly": is_anomaly,
                "anomaly_score": anomaly_lines[i].get('anomaly_score', 0) if is_anomaly else 0,
                "severity": "critical" if is_anomaly and anomaly_lines[i].get('anomaly_score', 0) >= 0.9 else
                          "high" if is_anomaly and anomaly_lines[i].get('anomaly_score', 0) >= 0.7 else
                          "medium" if is_anomaly and anomaly_lines[i].get('anomaly_score', 0) >= 0.5 else
                          "low" if is_anomaly else "normal"
            }
            
            log_entries.append(entry)
        
        # Pagination
        total = len(log_entries)
        start = (page - 1) * per_page
        end = start + per_page
        paginated_entries = log_entries[start:end]
        
        return jsonify({
            "file_id": file_id,
            "logs": paginated_entries,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page
            },
            "filters": {
                "anomalies_only": show_anomalies_only
            }
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/uploaded/list')
def list_uploaded_analyses():
    """List all uploaded dataset analyses"""
    analyses = []
    
    for file_id, result in uploaded_dataset_results.items():
        stats = uploaded_statistics.get(file_id, {})
        analyses.append({
            "file_id": file_id,
            "filename": result.get('filename', 'Unknown'),
            "analysis_completed": result.get('analysis_completed'),
            "status": result.get('status', 'unknown'),
            "total_logs": stats.get('total_logs', 0),
            "anomalies_found": stats.get('anomalies_found', 0),
            "critical_alerts": stats.get('critical_alerts', 0),
            "processing_time": stats.get('processing_time', 0)
        })
    
    # Sort by completion time (newest first)
    analyses.sort(key=lambda x: x.get('analysis_completed', ''), reverse=True)
    
    return jsonify({
        "analyses": analyses,
        "total_count": len(analyses)
    })

@app.route('/api/uploaded/delete/<file_id>', methods=['DELETE'])
def delete_uploaded_analysis(file_id):
    """Delete uploaded dataset analysis results"""
    try:
        # Remove from all tracking dictionaries
        deleted_items = []
        
        if file_id in uploaded_dataset_results:
            del uploaded_dataset_results[file_id]
            deleted_items.append('results')
        
        if file_id in uploaded_anomalies:
            del uploaded_anomalies[file_id]
            deleted_items.append('anomalies')
        
        if file_id in uploaded_alerts:
            del uploaded_alerts[file_id]
            deleted_items.append('alerts')
        
        if file_id in uploaded_statistics:
            del uploaded_statistics[file_id]
            deleted_items.append('statistics')
        
        if file_id in active_analyses:
            del active_analyses[file_id]
            deleted_items.append('active_analysis')
        
        if not deleted_items:
            return jsonify({"error": "Analysis not found"}), 404
        
        socketio.emit('dataset_analysis_deleted', {
            "file_id": file_id,
            "deleted_items": deleted_items
        })
        
        return jsonify({
            "status": "success",
            "message": f"Analysis {file_id} deleted successfully",
            "deleted_items": deleted_items
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/uploaded/download/<file_id>')
def download_uploaded_analysis(file_id):
    """Download comprehensive analysis report as text file"""
    if file_id not in uploaded_dataset_results:
        return jsonify({"error": "Analysis not found"}), 404
    
    try:
        # Get report data
        metadata = uploaded_dataset_results[file_id]
        statistics = uploaded_statistics.get(file_id, {})
        anomalies = uploaded_anomalies.get(file_id, [])
        alerts = uploaded_alerts.get(file_id, [])
        
        # Build text report
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("LOG ANOMALY DETECTION ANALYSIS REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # Report Metadata
        report_lines.append(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"File ID: {file_id}")
        report_lines.append(f"Source File: {metadata.get('filename', 'Unknown')}")
        report_lines.append(f"Upload Time: {metadata.get('upload_time', 'Unknown')}")
        report_lines.append("")
        
        # Executive Summary
        report_lines.append("-" * 80)
        report_lines.append("EXECUTIVE SUMMARY")
        report_lines.append("-" * 80)
        report_lines.append(f"Total Logs Processed: {statistics.get('total_logs', 0):,}")
        report_lines.append(f"Anomalies Detected: {len(anomalies):,}")
        report_lines.append(f"Alerts Generated: {len(alerts):,}")
        report_lines.append(f"Processing Time: {statistics.get('processing_time', 0):.2f} seconds")
        report_lines.append(f"Analysis Method: {statistics.get('analysis_method', 'Ensemble (KMeans + Isolation Forest)')}")
        report_lines.append("")
        
        # Alert Breakdown
        report_lines.append("-" * 80)
        report_lines.append("ALERT SEVERITY BREAKDOWN")
        report_lines.append("-" * 80)
        report_lines.append(f"Critical Alerts: {statistics.get('critical_alerts', 0)}")
        report_lines.append(f"High Risk Alerts: {statistics.get('high_risk_alerts', 0)}")
        report_lines.append(f"Medium Risk Alerts: {statistics.get('medium_risk_alerts', 0)}")
        report_lines.append(f"Low Risk Alerts: {statistics.get('low_risk_alerts', 0)}")
        report_lines.append("")
        
        # Statistics
        report_lines.append("-" * 80)
        report_lines.append("DETAILED STATISTICS")
        report_lines.append("-" * 80)
        
        # Services Affected
        if statistics.get('services'):
            report_lines.append("Services Affected:")
            for service in statistics['services']:
                report_lines.append(f"  • {service}")
            report_lines.append("")
        
        # Error Rate
        if 'error_rate' in statistics:
            report_lines.append(f"Error Rate: {statistics['error_rate']:.2%}")
            report_lines.append("")
        
        # Top Suspicious IPs
        if statistics.get('suspicious_ips'):
            report_lines.append("Top 10 Suspicious IP Addresses:")
            for idx, ip in enumerate(statistics['suspicious_ips'][:10], 1):
                report_lines.append(f"  {idx}. {ip}")
            report_lines.append("")
        
        # Top Anomalous Services
        if statistics.get('services_distribution'):
            report_lines.append("Services with Most Anomalies:")
            for service, count in statistics['services_distribution'].items():
                report_lines.append(f"  • {service}: {count} anomalies")
            report_lines.append("")
        
        # Top Anomalies
        if anomalies:
            report_lines.append("-" * 80)
            report_lines.append(f"TOP ANOMALIES (showing first 50 of {len(anomalies)})")
            report_lines.append("-" * 80)
            for idx, anomaly in enumerate(anomalies[:50], 1):
                report_lines.append(f"\n[Anomaly {idx}]")
                report_lines.append(f"  Timestamp: {anomaly.get('timestamp', 'N/A')}")
                report_lines.append(f"  Service: {anomaly.get('service', 'N/A')}")
                report_lines.append(f"  Log Level: {anomaly.get('log_level', 'N/A')}")
                report_lines.append(f"  Message: {anomaly.get('message', 'N/A')[:100]}")
                report_lines.append(f"  Source IP: {anomaly.get('ip_address', 'N/A')}")
                report_lines.append(f"  Anomaly Score: {anomaly.get('anomaly_score', 0):.4f}")
                report_lines.append(f"  Severity: {anomaly.get('severity', 'N/A')}")
        
        # Top Alerts
        if alerts:
            report_lines.append("\n" + "-" * 80)
            report_lines.append(f"TOP CRITICAL ALERTS (showing first 30 of {len(alerts)})")
            report_lines.append("-" * 80)
            for idx, alert in enumerate(alerts[:30], 1):
                report_lines.append(f"\n[Alert {idx}]")
                report_lines.append(f"  Timestamp: {alert.get('timestamp', 'N/A')}")
                report_lines.append(f"  Service: {alert.get('service', 'N/A')}")
                report_lines.append(f"  Severity: {alert.get('severity', 'N/A')}")
                report_lines.append(f"  Description: {alert.get('description', 'N/A')}")
        
        # Recommendations
        report_lines.append("\n" + "-" * 80)
        report_lines.append("RECOMMENDATIONS")
        report_lines.append("-" * 80)
        
        if statistics.get('critical_alerts', 0) > 0:
            report_lines.append("• Immediate investigation required for CRITICAL severity alerts")
        if statistics.get('high_risk_alerts', 0) > 10:
            report_lines.append("• Review configuration and firewall rules for suspicious IPs")
        if len(anomalies) > statistics.get('total_logs', 1) * 0.1:
            report_lines.append("• Anomaly rate is high (>10%) - consider reviewing log sources")
        if statistics.get('error_rate', 0) > 0.05:
            report_lines.append("• Error rate exceeds 5% - investigate service health")
        report_lines.append("• Archive this report for compliance and audit purposes")
        report_lines.append("• Schedule regular analysis runs for continuous monitoring")
        
        report_lines.append("\n" + "=" * 80)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 80)
        
        # Create temporary text file
        import tempfile
        report_text = "\n".join(report_lines)
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt', prefix=f'report_{file_id}_', encoding='utf-8') as f:
            f.write(report_text)
            temp_path = f.name
        
        filename = metadata.get('filename', 'unknown')
        download_name = f"anomaly_analysis_report_{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        # Send file and clean up
        response = send_file(
            temp_path,
            as_attachment=True,
            download_name=download_name,
            mimetype='text/plain'
        )
        
        # Schedule cleanup of temp file after response
        @response.call_on_close
        def cleanup():
            try:
                os.unlink(temp_path)
            except:
                pass
        
        return response
        
    except Exception as e:
        print(f"Error generating report: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

def add_anomaly(anomaly_data):
    """Add a new anomaly to the system"""
    with stats_lock:
        recent_anomalies.append(anomaly_data)
        statistics["total_anomalies"] += 1
        statistics["total_logs"] += 1
        system_status["last_anomaly"] = anomaly_data
        
        if statistics["total_logs"] > 0:
            statistics["anomaly_rate"] = statistics["total_anomalies"] / statistics["total_logs"]
        
        if anomaly_data.get("anomaly_score", 0) > 0.8:
            statistics["critical_alerts"] += 1
        
        if "service" in anomaly_data:
            statistics["services_affected"].add(anomaly_data["service"])
        
        if "ip_address" in anomaly_data:
            statistics["suspicious_ips"].add(anomaly_data["ip_address"])
    
    # Emit to connected clients
    socketio.emit('new_anomaly', anomaly_data)

def update_statistics(log_count):
    """Update general statistics"""
    with stats_lock:
        statistics["total_logs"] += log_count
        if statistics["total_logs"] > 0:
            statistics["anomaly_rate"] = statistics["total_anomalies"] / statistics["total_logs"]
    
    socketio.emit('stats_update', {
        "total_logs": statistics["total_logs"],
        "total_anomalies": statistics["total_anomalies"],
        "anomaly_rate": statistics["anomaly_rate"]
    })

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('Client connected')
    emit('connection_response', {'status': 'connected'})
    # Send current status on connect
    with stats_lock:
        emit('system_status_update', system_status)

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

if __name__ == '__main__':
    # Create necessary directories
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    print(f"Starting Flask server on {FLASK_HOST}:{FLASK_PORT}")
    print(f"Dashboard available at http://localhost:{FLASK_PORT}")
    print(f"Spark UI available at http://localhost:{SPARK_UI_PORT}")
    
    socketio.run(app, host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG, allow_unsafe_werkzeug=True)

"""
End of web_app.py
"""
