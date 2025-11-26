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

app = Flask(__name__)
app.config['SECRET_KEY'] = 'anomaly-detection-secret-key'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = 'uploads'

CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

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
