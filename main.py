"""
Main application orchestrator
Integrates all components: log generation, processing, ML detection, and web UI
"""
import threading
import time
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from log_generator import LogGenerator
from log_parser import LogParser, create_spark_session
from anomaly_detector import AnomalyDetector
from graph_analyzer import GraphAnalyzer
from web_app import add_anomaly, update_statistics, app, socketio
from ngrok_tunnel import NgrokTunnel
from config import LOG_FILE_PATH, MODEL_PATH

class AnomalyDetectionSystem:
    def __init__(self):
        self.spark = None
        self.log_generator = None
        self.parser = None
        self.detector = None
        self.graph_analyzer = None
        self.ngrok_tunnel = None
        self.running = False
        
    def initialize(self):
        """Initialize all components"""
        print("Initializing Anomaly Detection System...")
        
        # Create Spark session
        print("Creating Spark session...")
        self.spark = create_spark_session()
        
        # Initialize components
        self.log_generator = LogGenerator()
        self.parser = LogParser(self.spark)
        self.detector = AnomalyDetector(self.spark)
        self.graph_analyzer = GraphAnalyzer(self.spark)
        self.ngrok_tunnel = NgrokTunnel()
        
        print("System initialized successfully!")
    
    def train_model(self):
        """Train the ML model on initial data"""
        print("\n" + "="*60)
        print("TRAINING ANOMALY DETECTION MODEL")
        print("="*60)
        
        # Generate initial training data
        print("Generating training data...")
        self.log_generator.start()
        time.sleep(30)  # Generate 30 seconds of data
        self.log_generator.stop()
        
        # Check if log file exists and has data
        if not os.path.exists(LOG_FILE_PATH):
            print("No log data found. Please run log generator first.")
            return False
        
        # Read and process logs
        print("Reading and processing logs...")
        log_df = self.spark.read.text(LOG_FILE_PATH)
        
        if log_df.count() == 0:
            print("No logs to process!")
            return False
        
        parsed_df = self.parser.parse_log_line(log_df)
        feature_df = self.parser.extract_features(parsed_df)
        
        # Create feature vectors
        print("Creating feature vectors...")
        scaled_df, scaler_model = self.parser.create_feature_vector(feature_df)
        
        # Train model
        print("Training KMeans model...")
        self.detector.train_kmeans(scaled_df)
        
        # Save model
        print("Saving model...")
        self.detector.save_model()
        
        print("Model training completed successfully!")
        print("="*60 + "\n")
        
        return True
    
    def load_or_train_model(self):
        """Load existing model or train a new one"""
        try:
            if os.path.exists(MODEL_PATH):
                print("Loading existing model...")
                self.detector.load_model()
                return True
            else:
                print("No existing model found. Training new model...")
                return self.train_model()
        except Exception as e:
            print(f"Error loading model: {e}")
            print("Training new model...")
            return self.train_model()
    
    def process_logs(self):
        """Process logs and detect anomalies"""
        print("Starting log processing loop...")
        
        last_position = 0
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while self.running:
            try:
                # Check if log file exists
                if not os.path.exists(LOG_FILE_PATH):
                    print(f"Waiting for log file: {LOG_FILE_PATH}")
                    time.sleep(5)
                    continue
                
                # Get file size
                current_size = os.path.getsize(LOG_FILE_PATH)
                
                # If file grew, process new logs
                if current_size > last_position:
                    # Read logs with better error handling
                    try:
                        log_df = self.spark.read.text(LOG_FILE_PATH)
                        log_count = log_df.count()
                        
                        if log_count == 0:
                            time.sleep(5)
                            continue
                        
                        print(f"Processing {log_count} total logs...")
                        
                    except Exception as e:
                        print(f"Error reading log file: {e}")
                        time.sleep(5)
                        continue
                    
                    # Parse logs with validation
                    try:
                        parsed_df = self.parser.parse_log_line(log_df)
                        feature_df = self.parser.extract_features(parsed_df)
                        
                        # Filter out empty rows with better validation
                        valid_df = feature_df.filter(
                            col("timestamp").isNotNull() & 
                            col("level").isNotNull() &
                            col("service").isNotNull() &
                            col("response_time").isNotNull()
                        )
                        
                        valid_count = valid_df.count()
                        if valid_count == 0:
                            print("No valid logs found after parsing")
                            time.sleep(5)
                            continue
                            
                        print(f"Found {valid_count} valid logs after parsing")
                        
                    except Exception as e:
                        print(f"Error parsing logs: {e}")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            print("Too many consecutive errors. Stopping...")
                            break
                        time.sleep(10)
                        continue
                    
                    # Create feature vectors with error handling
                    try:
                        scaled_df, _ = self.parser.create_feature_vector(valid_df)
                        
                        # Check if we have enough data for ML
                        if scaled_df.count() < 5:
                            print("Not enough data for anomaly detection")
                            last_position = current_size
                            time.sleep(5)
                            continue
                            
                    except Exception as e:
                        print(f"Error creating feature vectors: {e}")
                        time.sleep(5)
                        continue
                    
                    # Detect anomalies with error handling
                    try:
                        anomaly_df = self.detector.ensemble_anomaly_detection(scaled_df)
                        
                        # Filter only anomalies
                        anomalies = anomaly_df.filter(col("is_anomaly_final") == 1)
                        anomaly_count = anomalies.count()
                        
                        # Send to web interface
                        if anomaly_count > 0:
                            anomaly_list = anomalies.select(
                                "timestamp", "level", "service", "message", 
                                "ip_address", "user", "response_time",
                                "final_anomaly_score"
                            ).collect()
                            
                            for anomaly in anomaly_list:
                                try:
                                    anomaly_data = {
                                        "timestamp": str(anomaly.timestamp) if anomaly.timestamp else datetime.now().isoformat(),
                                        "level": str(anomaly.level) if anomaly.level else "UNKNOWN",
                                        "service": str(anomaly.service) if anomaly.service else "unknown",
                                        "message": str(anomaly.message) if anomaly.message else "",
                                        "ip_address": str(anomaly.ip_address) if anomaly.ip_address else "N/A",
                                        "user": str(anomaly.user) if anomaly.user else "N/A",
                                        "response_time": int(anomaly.response_time) if anomaly.response_time else 0,
                                        "anomaly_score": float(anomaly.final_anomaly_score) if anomaly.final_anomaly_score else 0.0
                                    }
                                    add_anomaly(anomaly_data)
                                except Exception as e:
                                    print(f"Error processing anomaly data: {e}")
                                    continue
                        
                        # Update statistics
                        new_logs = log_count - (last_position // 100)  # Rough estimate
                        update_statistics(max(new_logs, 0))
                        
                        # Update graph every 100 new logs
                        if log_count % 100 == 0:
                            try:
                                print("Updating graph analysis...")
                                vertices, edges = self.graph_analyzer.create_service_graph(valid_df)
                                node_stats, suspicious_edges = self.graph_analyzer.detect_graph_anomalies(
                                    vertices, edges
                                )
                                self.graph_analyzer.export_graph_for_visualization(
                                    vertices, edges, "static/graph_data.json"
                                )
                            except Exception as e:
                                print(f"Error updating graph: {e}")
                        
                        last_position = current_size
                        consecutive_errors = 0  # Reset error counter on success
                        print(f"Successfully processed logs. Found {anomaly_count} anomalies.")
                        
                    except Exception as e:
                        print(f"Error in anomaly detection: {e}")
                        consecutive_errors += 1
                        time.sleep(5)
                        continue
                
                time.sleep(5)
                
            except Exception as e:
                print(f"Unexpected error in log processing loop: {e}")
                import traceback
                traceback.print_exc()
                consecutive_errors += 1
                
                if consecutive_errors >= max_consecutive_errors:
                    print("Too many consecutive errors. Stopping log processing.")
                    break
                    
                time.sleep(10)
        
        print("Log processing loop ended")
    
    def start(self):
        """Start the entire system"""
        print("\n" + "="*60)
        print("STARTING LOG ANOMALY DETECTION SYSTEM")
        print("="*60 + "\n")
        
        # Initialize
        self.initialize()
        
        # Load or train model
        if not self.load_or_train_model():
            print("Failed to initialize model. Exiting...")
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
        print(f"Spark UI: http://localhost:4040")
        print("="*60 + "\n")
        
        # Start Flask app (this blocks)
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
        
        if self.spark:
            self.spark.stop()
        
        print("System stopped successfully!")

if __name__ == "__main__":
    system = AnomalyDetectionSystem()
    try:
        system.start()
    except KeyboardInterrupt:
        system.stop()
