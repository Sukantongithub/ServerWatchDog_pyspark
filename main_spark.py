"""
PySpark-based Log Anomaly Detection with proper Spark connectivity
"""
import threading
import time
import os
from datetime import datetime

from log_generator import LogGenerator
from web_app import add_anomaly, update_statistics, app, socketio
from ngrok_tunnel import NgrokTunnel
from config import LOG_FILE_PATH

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when, length
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
import numpy as np

class SparkAnomalyDetectionSystem:
    def __init__(self):
        self.spark = None
        self.log_generator = None
        self.ngrok_tunnel = None
        self.running = False
        self.model = None
        self.scaler = None
        
    def initialize_spark(self):
        """Initialize Spark Session"""
        print("Initializing Apache Spark...")
        
        self.spark = SparkSession.builder \
            .appName("LogAnomalyDetection") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"✅ Spark initialized successfully!")
        print(f"   Spark UI: http://localhost:4040")
        print(f"   Spark Version: {self.spark.version}")
        
    def initialize(self):
        """Initialize all components"""
        print("Initializing Spark-based Anomaly Detection System...")
        
        # Initialize Spark
        self.initialize_spark()
        
        # Initialize other components
        self.log_generator = LogGenerator()
        self.ngrok_tunnel = NgrokTunnel()
        
        print("System initialized successfully!")
    
    def parse_logs_spark(self, log_file):
        """Parse logs using PySpark"""
        # Read logs
        log_df = self.spark.read.text(log_file)
        
        # Extract components using regex
        parsed_df = log_df.withColumn(
            "timestamp",
            regexp_extract(col("value"), r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})", 1)
        ).withColumn(
            "level",
            regexp_extract(col("value"), r"\[(INFO|WARN|ERROR|DEBUG)\]", 1)
        ).withColumn(
            "service",
            regexp_extract(col("value"), r"\] ([a-z\-]+) -", 1)
        ).withColumn(
            "message",
            regexp_extract(col("value"), r"- (.*?) \|", 1)
        ).withColumn(
            "ip_address",
            regexp_extract(col("value"), r"IP: ([0-9\.]+)", 1)
        ).withColumn(
            "user",
            regexp_extract(col("value"), r"User: ([a-z0-9_]+)", 1)
        ).withColumn(
            "response_time",
            regexp_extract(col("value"), r"ResponseTime: (\d+)ms", 1).cast(IntegerType())
        )
        
        # Filter valid rows
        parsed_df = parsed_df.filter(
            (col("level") != "") & (col("service") != "")
        )
        
        return parsed_df
    
    def extract_features_spark(self, parsed_df):
        """Extract features using PySpark"""
        feature_df = parsed_df.withColumn(
            "level_error",
            when(col("level") == "ERROR", 1).otherwise(0)
        ).withColumn(
            "level_warn",
            when(col("level") == "WARN", 1).otherwise(0)
        ).withColumn(
            "level_info",
            when(col("level") == "INFO", 1).otherwise(0)
        ).withColumn(
            "message_length",
            length(col("message"))
        ).withColumn(
            "has_error_keyword",
            when(
                col("message").rlike("(?i)(timeout|error|exception|failed|critical|denied|exhausted|deadlock)"),
                1
            ).otherwise(0)
        ).withColumn(
            "suspicious_ip",
            when(col("ip_address").rlike("(999|666|0\\.0\\.0\\.0)"), 1).otherwise(0)
        ).withColumn(
            "high_response_time",
            when(col("response_time") > 1000, 1).otherwise(0)
        )
        
        return feature_df
    
    def create_feature_vectors(self, feature_df):
        """Create feature vectors for ML"""
        feature_columns = [
            "response_time", "level_error", "level_warn", "level_info",
            "message_length", "has_error_keyword", "suspicious_ip", "high_response_time"
        ]
        
        assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol="features_raw"
        )
        
        assembled_df = assembler.transform(feature_df)
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        self.scaler = scaler.fit(assembled_df)
        scaled_df = self.scaler.transform(assembled_df)
        
        return scaled_df
    
    def train_model(self):
        """Train KMeans model using Spark MLlib"""
        print("\n" + "="*60)
        print("TRAINING SPARK MLLIB ANOMALY DETECTION MODEL")
        print("="*60)
        
        # Generate training data
        print("Generating training data...")
        self.log_generator.start()
        time.sleep(30)
        self.log_generator.stop()
        
        if not os.path.exists(LOG_FILE_PATH):
            print("No log data found!")
            return False
        
        # Process logs with Spark
        print("Processing logs with Apache Spark...")
        parsed_df = self.parse_logs_spark(LOG_FILE_PATH)
        
        log_count = parsed_df.count()
        print(f"Processed {log_count} log entries with Spark")
        
        if log_count == 0:
            print("No valid logs to process!")
            return False
        
        # Extract features
        feature_df = self.extract_features_spark(parsed_df)
        
        # Create feature vectors
        scaled_df = self.create_feature_vectors(feature_df)
        
        # Train KMeans
        print("Training KMeans model with Spark MLlib...")
        kmeans = KMeans(k=5, seed=42, featuresCol="features", predictionCol="cluster")
        self.model = kmeans.fit(scaled_df)
        
        print(f"✅ Model trained successfully!")
        print(f"   Cluster Centers: {len(self.model.clusterCenters())}")
        print("="*60 + "\n")
        
        return True
    
    def detect_anomaly_spark(self, log_entry_row):
        """Detect anomaly using Spark model"""
        if not self.model:
            return 0.5
        
        try:
            # Get cluster prediction
            cluster = int(log_entry_row.cluster)
            features = log_entry_row.features.toArray()
            
            # Calculate distance to cluster center
            center = self.model.clusterCenters()[cluster]
            distance = np.sqrt(np.sum((features - center) ** 2))
            
            # Normalize score (0-1, higher = more anomalous)
            anomaly_score = min(distance / 3.0, 1.0)
            
            return float(anomaly_score)
        except Exception as e:
            return 0.5
    
    def process_logs(self):
        """Process logs with Spark in real-time"""
        print("Starting Spark log processing loop...")
        
        last_count = 0
        
        while self.running:
            try:
                if not os.path.exists(LOG_FILE_PATH):
                    time.sleep(5)
                    continue
                
                # Read logs with Spark
                parsed_df = self.parse_logs_spark(LOG_FILE_PATH)
                current_count = parsed_df.count()
                
                if current_count <= last_count:
                    time.sleep(5)
                    continue
                
                # Process new logs
                feature_df = self.extract_features_spark(parsed_df)
                scaled_df = self.create_feature_vectors(feature_df)
                
                # Predict clusters
                predictions = self.model.transform(scaled_df)
                
                # Collect predictions
                results = predictions.select(
                    "timestamp", "level", "service", "message",
                    "ip_address", "user", "response_time",
                    "cluster", "features"
                ).collect()
                
                # Check for anomalies
                for row in results[last_count:]:
                    anomaly_score = self.detect_anomaly_spark(row)
                    
                    if anomaly_score > 0.6:
                        anomaly_data = {
                            'timestamp': str(row.timestamp),
                            'level': row.level,
                            'service': row.service,
                            'message': row.message,
                            'ip_address': row.ip_address,
                            'user': row.user,
                            'response_time': int(row.response_time) if row.response_time else 0,
                            'anomaly_score': anomaly_score
                        }
                        add_anomaly(anomaly_data)
                        print(f"Anomaly detected: {row.service} - Score: {anomaly_score:.3f}")
                
                # Update statistics
                update_statistics(current_count - last_count)
                last_count = current_count
                
                time.sleep(5)
                
            except Exception as e:
                print(f"Error in Spark processing: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(5)
    
    def start(self):
        """Start the Spark-based system"""
        print("\n" + "="*60)
        print("STARTING SPARK-BASED LOG ANOMALY DETECTION SYSTEM")
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
        
        # Start Spark processing in background
        self.running = True
        processing_thread = threading.Thread(target=self.process_logs, daemon=True)
        processing_thread.start()
        
        print("\n" + "="*60)
        print("SYSTEM READY!")
        print("="*60)
        if public_url:
            print(f"Dashboard: {public_url}")
        print(f"Local Dashboard: http://localhost:5000")
        print(f"Spark Web UI: http://localhost:4040")
        print("="*60 + "\n")
        
        # Start Flask app
        try:
            socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Stop the system"""
        print("\nStopping Spark Anomaly Detection System...")
        self.running = False
        
        if self.log_generator:
            self.log_generator.stop()
        
        if self.ngrok_tunnel:
            self.ngrok_tunnel.stop_tunnel()
        
        if self.spark:
            self.spark.stop()
        
        print("System stopped successfully!")

if __name__ == "__main__":
    system = SparkAnomalyDetectionSystem()
    try:
        system.start()
    except KeyboardInterrupt:
        system.stop()
