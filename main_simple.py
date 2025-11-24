"""
Simplified Main Application - Works without PySpark
Uses pandas for data processing instead
"""
import threading
import time
import os
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

from log_generator import LogGenerator
from web_app import add_anomaly, update_statistics, app, socketio
from ngrok_tunnel import NgrokTunnel
from config import LOG_FILE_PATH

class SimpleAnomalyDetectionSystem:
    def __init__(self):
        self.log_generator = None
        self.ngrok_tunnel = None
        self.running = False
        self.model = None
        self.scaler = None
        self.isolation_forest = None
        
    def initialize(self):
        """Initialize all components"""
        print("Initializing Simple Anomaly Detection System...")
        
        # Initialize components
        self.log_generator = LogGenerator()
        self.ngrok_tunnel = NgrokTunnel()
        
        print("System initialized successfully!")
    
    def parse_log_line(self, line):
        """Parse a single log line"""
        try:
            parts = line.split(' | ')
            if len(parts) < 4:
                return None
            
            timestamp_level_service_msg = parts[0].split('] ')
            if len(timestamp_level_service_msg) < 2:
                return None
            
            timestamp_level = timestamp_level_service_msg[0].split(' [')
            timestamp = timestamp_level[0] if len(timestamp_level) > 0 else ''
            level = timestamp_level[1] if len(timestamp_level) > 1 else ''
            
            service_msg = timestamp_level_service_msg[1].split(' - ')
            service = service_msg[0] if len(service_msg) > 0 else ''
            message = service_msg[1] if len(service_msg) > 1 else ''
            
            ip = parts[1].replace('IP: ', '').strip() if len(parts) > 1 else ''
            user = parts[2].replace('User: ', '').strip() if len(parts) > 2 else ''
            response_time = parts[3].replace('ResponseTime: ', '').replace('ms', '').strip() if len(parts) > 3 else '0'
            
            return {
                'timestamp': timestamp,
                'level': level,
                'service': service,
                'message': message,
                'ip_address': ip,
                'user': user,
                'response_time': int(response_time) if response_time.isdigit() else 0
            }
        except Exception as e:
            return None
    
    def extract_features(self, log_entry):
        """Extract numerical features from log entry"""
        if not log_entry:
            return None
        
        features = {
            'response_time': log_entry['response_time'],
            'level_error': 1 if log_entry['level'] == 'ERROR' else 0,
            'level_warn': 1 if log_entry['level'] == 'WARN' else 0,
            'level_info': 1 if log_entry['level'] == 'INFO' else 0,
            'message_length': len(log_entry['message']),
            'has_error_keyword': 1 if any(word in log_entry['message'].lower() 
                for word in ['timeout', 'error', 'exception', 'failed', 'critical']) else 0,
            'suspicious_ip': 1 if any(num in log_entry['ip_address'] 
                for num in ['999', '666']) else 0,
            'high_response_time': 1 if log_entry['response_time'] > 1000 else 0,
        }
        
        return features
    
    def train_model(self):
        """Train the ML model"""
        print("\n" + "="*60)
        print("TRAINING ANOMALY DETECTION MODEL")
        print("="*60)
        
        # Generate training data
        print("Generating training data...")
        self.log_generator.start()
        time.sleep(30)
        self.log_generator.stop()
        
        # Read and process logs
        print("Reading and processing logs...")
        if not os.path.exists(LOG_FILE_PATH):
            print("No log data found!")
            return False
        
        logs = []
        with open(LOG_FILE_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                log_entry = self.parse_log_line(line.strip())
                if log_entry:
                    features = self.extract_features(log_entry)
                    if features:
                        logs.append(features)
        
        if len(logs) == 0:
            print("No valid logs to process!")
            return False
        
        # Create DataFrame
        df = pd.DataFrame(logs)
        print(f"Processed {len(df)} log entries")
        
        # Scale features
        print("Training models...")
        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(df)
        
        # Train KMeans
        self.model = KMeans(n_clusters=5, random_state=42)
        self.model.fit(X_scaled)
        
        # Train Isolation Forest
        self.isolation_forest = IsolationForest(contamination=0.1, random_state=42)
        self.isolation_forest.fit(X_scaled)
        
        print("Model training completed successfully!")
        print("="*60 + "\n")
        
        return True
    
    def detect_anomaly(self, log_entry):
        """Detect if a log entry is anomalous"""
        if not self.model or not log_entry:
            return 0.5
        
        features = self.extract_features(log_entry)
        if not features:
            return 0.5
        
        # Create feature vector
        feature_vector = [[
            features['response_time'],
            features['level_error'],
            features['level_warn'],
            features['level_info'],
            features['message_length'],
            features['has_error_keyword'],
            features['suspicious_ip'],
            features['high_response_time']
        ]]
        
        # Scale
        X_scaled = self.scaler.transform(feature_vector)
        
        # Get KMeans distance
        cluster = self.model.predict(X_scaled)[0]
        distance = np.linalg.norm(X_scaled[0] - self.model.cluster_centers_[cluster])
        kmeans_score = min(distance / 3.0, 1.0)  # Normalize to 0-1
        
        # Get Isolation Forest score
        iso_score = self.isolation_forest.score_samples(X_scaled)[0]
        iso_normalized = (iso_score + 0.5) / 1.0  # Normalize
        iso_normalized = max(0, min(1, 1 - iso_normalized))  # Invert and clip
        
        # Combine scores
        final_score = 0.6 * kmeans_score + 0.4 * iso_normalized
        
        return final_score
    
    def process_logs(self):
        """Process logs and detect anomalies"""
        print("Starting log processing loop...")
        
        last_position = 0
        
        while self.running:
            try:
                if not os.path.exists(LOG_FILE_PATH):
                    time.sleep(5)
                    continue
                
                # Read new logs
                with open(LOG_FILE_PATH, 'r', encoding='utf-8') as f:
                    f.seek(last_position)
                    new_lines = f.readlines()
                    last_position = f.tell()
                
                for line in new_lines:
                    log_entry = self.parse_log_line(line.strip())
                    if log_entry:
                        # Detect anomaly
                        anomaly_score = self.detect_anomaly(log_entry)
                        
                        # Update statistics
                        update_statistics(1)
                        
                        # If anomalous, send to web interface
                        if anomaly_score > 0.6:
                            anomaly_data = {
                                'timestamp': log_entry['timestamp'],
                                'level': log_entry['level'],
                                'service': log_entry['service'],
                                'message': log_entry['message'],
                                'ip_address': log_entry['ip_address'],
                                'user': log_entry['user'],
                                'response_time': log_entry['response_time'],
                                'anomaly_score': float(anomaly_score)
                            }
                            add_anomaly(anomaly_data)
                            print(f"Anomaly detected: {log_entry['service']} - Score: {anomaly_score:.3f}")
                
                time.sleep(2)
                
            except Exception as e:
                print(f"Error in log processing: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(5)
    
    def start(self):
        """Start the entire system"""
        print("\n" + "="*60)
        print("STARTING LOG ANOMALY DETECTION SYSTEM")
        print("="*60 + "\n")
        
        # Initialize
        self.initialize()
        
        # Train model
        if not self.train_model():
            print("Failed to train model. Exiting...")
            return
        
        # Start ngrok tunnel
        print("Starting ngrok tunnel...")
        public_url = self.ngrok_tunnel.start_tunnel()
        
        # Start log generator
        print("Starting log generator...")
        self.log_generator.start()
        
        # Start log processing in background
        self.running = True
        processing_thread = threading.Thread(target=self.process_logs, daemon=True)
        processing_thread.start()
        
        print("\n" + "="*60)
        print("SYSTEM READY!")
        print("="*60)
        if public_url:
            print(f"Dashboard: {public_url}")
        print(f"Local Dashboard: http://localhost:5000")
        print("="*60 + "\n")
        
        # Start Flask app
        try:
            socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Stop the system"""
        print("\nStopping Anomaly Detection System...")
        self.running = False
        
        if self.log_generator:
            self.log_generator.stop()
        
        if self.ngrok_tunnel:
            self.ngrok_tunnel.stop_tunnel()
        
        print("System stopped successfully!")

if __name__ == "__main__":
    system = SimpleAnomalyDetectionSystem()
    try:
        system.start()
    except KeyboardInterrupt:
        system.stop()
