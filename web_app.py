"""
Flask Web Application for Real-time Log Anomaly Detection Dashboard
"""
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import threading
import time
import json
import os
from datetime import datetime
from collections import deque
from config import FLASK_HOST, FLASK_PORT, FLASK_DEBUG

app = Flask(__name__)
app.config['SECRET_KEY'] = 'anomaly-detection-secret-key'
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

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

# Lock for thread-safe operations
stats_lock = threading.Lock()

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')

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

def add_anomaly(anomaly_data):
    """Add a new anomaly to the system"""
    with stats_lock:
        recent_anomalies.append(anomaly_data)
        statistics["total_anomalies"] += 1
        statistics["total_logs"] += 1
        
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

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

if __name__ == '__main__':
    # Create necessary directories
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    print(f"Starting Flask server on {FLASK_HOST}:{FLASK_PORT}")
    print(f"Dashboard available at http://localhost:{FLASK_PORT}")
    
    socketio.run(app, host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG, allow_unsafe_werkzeug=True)
