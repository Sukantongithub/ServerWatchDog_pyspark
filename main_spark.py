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
from graph_analyzer import GraphAnalyzer

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
        self.graph_analyzer = None
        
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
        
        # Initialize graph analyzer
        self.graph_analyzer = GraphAnalyzer(self.spark)
        
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
        graph_update_counter = 0
        
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
                
                # Update graph every 30 seconds (6 iterations at 5 second intervals)
                graph_update_counter += 1
                if graph_update_counter >= 6:
                    try:
                        print("📊 Updating network graph visualization...")
                        self.graph_analyzer.update_graph_periodically(
                            feature_df, 
                            "static/graph_data.json"
                        )
                        socketio.emit('graph_data_updated', {"status": "success"})
                        graph_update_counter = 0
                    except Exception as e:
                        print(f"⚠️ Graph update warning: {e}")
                
                time.sleep(5)
                
            except Exception as e:
                print(f"Error in Spark processing: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(5)
    
    def analyze_historical_logs(self, log_file):
        """
        Analyze historical logs for trends and patterns
        """
        print("Analyzing historical logs...")
        if not os.path.exists(log_file):
            print("No historical log file found!")
            return

        # Parse logs
        parsed_df = self.parse_logs_spark(log_file)
        log_count = parsed_df.count()
        print(f"Analyzed {log_count} historical log entries.")

        if log_count == 0:
            print("No valid logs to analyze!")
            return

        # Extract features
        feature_df = self.extract_features_spark(parsed_df)

        # Aggregate statistics
        stats_df = feature_df.groupBy("service").agg(
            col("service"),
            col("level_error").sum().alias("error_count"),
            col("level_warn").sum().alias("warn_count"),
            col("response_time").avg().alias("avg_response_time")
        )

        stats_df.show(truncate=False)
        print("Historical log analysis completed.")
    
    def analyze_uploaded_dataset_with_spark(self, filepath):
        """
        Analyze uploaded dataset using Spark MLlib for comprehensive anomaly detection
        """
        print(f"\n{'='*60}")
        print(f"SPARK ANALYSIS OF UPLOADED DATASET: {os.path.basename(filepath)}")
        print(f"{'='*60}")
        
        try:
            if not os.path.exists(filepath):
                print("File not found!")
                return False, "File not found"
            
            # Import the analyzer from web_app
            from web_app import UploadedDatasetAnalyzer
            
            # Generate file ID
            file_id = UploadedDatasetAnalyzer.generate_file_id(filepath)
            
            # Parse logs with Spark
            print("🔍 Parsing logs with Apache Spark...")
            parsed_df = self.parse_logs_spark(filepath)
            log_count = parsed_df.count()
            
            if log_count == 0:
                return False, "No valid logs found in file"
            
            print(f"📊 Processing {log_count} log entries with Spark MLlib...")
            
            # Extract features
            feature_df = self.extract_features_spark(parsed_df)
            
            # Create feature vectors
            scaled_df = self.create_feature_vectors(feature_df)
            
            # Use trained model for predictions if available
            if self.model:
                print("🤖 Applying trained ML model for anomaly detection...")
                predictions = self.model.transform(scaled_df)
                
                # Collect results for analysis
                results = predictions.select(
                    "timestamp", "level", "service", "message", "ip_address", 
                    "user", "response_time", "cluster", "features"
                ).collect()
                
                # Advanced anomaly analysis
                anomalies_found = []
                critical_anomalies = 0
                high_anomalies = 0
                medium_anomalies = 0
                
                for row in results:
                    anomaly_score = self.detect_anomaly_spark(row)
                    
                    # Enhanced scoring with Spark features
                    enhanced_score = self.calculate_enhanced_anomaly_score(row, anomaly_score)
                    
                    if enhanced_score > 0.4:  # Lower threshold for uploaded analysis
                        anomaly_data = {
                            'timestamp': str(row.timestamp),
                            'level': row.level,
                            'service': row.service,
                            'message': row.message,
                            'ip_address': row.ip_address,
                            'user': row.user,
                            'response_time': int(row.response_time) if row.response_time else 0,
                            'anomaly_score': enhanced_score,
                            'cluster': int(row.cluster),
                            'analysis_type': 'spark_mllib',
                            'file_id': file_id
                        }
                        
                        anomalies_found.append(anomaly_data)
                        
                        # Categorize severity
                        if enhanced_score >= 0.9:
                            critical_anomalies += 1
                        elif enhanced_score >= 0.7:
                            high_anomalies += 1
                        else:
                            medium_anomalies += 1
                
                # Generate comprehensive statistics
                spark_stats = self.generate_spark_statistics(feature_df, anomalies_found)
                
                print(f"✅ Spark MLlib Analysis Complete!")
                print(f"   Total Logs: {log_count}")
                print(f"   Anomalies Found: {len(anomalies_found)}")
                print(f"   Critical: {critical_anomalies}, High: {high_anomalies}, Medium: {medium_anomalies}")
                print(f"   Anomaly Rate: {len(anomalies_found)/log_count*100:.2f}%")
                
                # Store results in web_app format for dashboard integration
                from web_app import uploaded_dataset_results, uploaded_anomalies, uploaded_statistics
                
                uploaded_dataset_results[file_id] = {
                    "filepath": filepath,
                    "filename": os.path.basename(filepath),
                    "analysis_completed": datetime.now().isoformat(),
                    "stats": spark_stats,
                    "status": "completed",
                    "analysis_method": "spark_mllib"
                }
                
                uploaded_anomalies[file_id] = anomalies_found
                uploaded_statistics[file_id] = spark_stats
                
                # Emit real-time updates
                from web_app import socketio
                socketio.emit('spark_analysis_completed', {
                    "file_id": file_id,
                    "filename": os.path.basename(filepath),
                    "stats": spark_stats,
                    "anomalies_count": len(anomalies_found),
                    "method": "spark_mllib"
                })
                
                return True, {
                    "file_id": file_id,
                    "stats": spark_stats,
                    "anomalies": len(anomalies_found)
                }
            
            else:
                print("⚠️ No trained model available. Using basic pattern analysis...")
                # Fall back to basic analysis
                basic_analysis = UploadedDatasetAnalyzer.analyze_uploaded_logs(filepath, file_id)
                return basic_analysis
                
        except Exception as e:
            print(f"❌ Error in Spark analysis: {e}")
            import traceback
            traceback.print_exc()
            return False, str(e)
    
    def calculate_enhanced_anomaly_score(self, spark_row, base_score):
        """
        Calculate enhanced anomaly score using Spark ML features
        """
        enhanced_score = base_score
        
        # Add cluster-based anomaly detection
        cluster = int(spark_row.cluster)
        
        # Penalize rare clusters (clusters with fewer members are more anomalous)
        if cluster >= 3:  # Assuming clusters 3-4 are rare patterns
            enhanced_score += 0.2
        
        # Response time analysis
        response_time = spark_row.response_time if spark_row.response_time else 0
        if response_time > 5000:
            enhanced_score += 0.3
        elif response_time > 2000:
            enhanced_score += 0.1
        
        # Service-specific scoring
        service = spark_row.service
        level = spark_row.level
        
        if service in ['auth-service', 'security-service']:
            if level == 'ERROR':
                enhanced_score += 0.2
            elif level == 'WARN':
                enhanced_score += 0.1
        
        # IP address pattern analysis
        ip = spark_row.ip_address
        if ip and ('999' in ip or '666' in ip or ip.startswith('0.')):
            enhanced_score += 0.25
        
        return min(enhanced_score, 1.0)
    
    def generate_spark_statistics(self, feature_df, anomalies):
        """
        Generate comprehensive statistics using Spark DataFrame operations
        """
        try:
            # Collect basic stats using Spark aggregations
            total_logs = feature_df.count()
            
            # Service distribution
            service_dist = feature_df.groupBy("service").count().collect()
            services_analyzed = [row.service for row in service_dist]
            
            # Error analysis
            error_stats = feature_df.filter(col("level") == "ERROR").count()
            warn_stats = feature_df.filter(col("level") == "WARN").count()
            
            # Response time analysis
            avg_response_time = feature_df.agg({"response_time": "avg"}).collect()[0][0]
            max_response_time = feature_df.agg({"response_time": "max"}).collect()[0][0]
            
            # IP analysis
            ip_dist = feature_df.groupBy("ip_address").count().orderBy(col("count").desc()).limit(10).collect()
            top_ips = [row.ip_address for row in ip_dist]
            
            # Time range analysis
            time_range = feature_df.agg(
                {"timestamp": "min", "timestamp": "max"}
            ).collect()[0]
            
            # Generate statistics compatible with web app format
            stats = {
                "total_logs": total_logs,
                "anomalies_found": len(anomalies),
                "critical_alerts": len([a for a in anomalies if a.get('anomaly_score', 0) >= 0.9]),
                "high_risk_alerts": len([a for a in anomalies if 0.7 <= a.get('anomaly_score', 0) < 0.9]),
                "medium_risk_alerts": len([a for a in anomalies if 0.5 <= a.get('anomaly_score', 0) < 0.7]),
                "low_risk_alerts": len([a for a in anomalies if 0.4 <= a.get('anomaly_score', 0) < 0.5]),
                "services_analyzed": services_analyzed,
                "suspicious_ips_found": top_ips,
                "error_patterns": {
                    "ERROR_logs": error_stats,
                    "WARN_logs": warn_stats,
                    "avg_response_time": float(avg_response_time) if avg_response_time else 0,
                    "max_response_time": int(max_response_time) if max_response_time else 0
                },
                "time_range": {
                    "start": str(time_range[0]) if time_range[0] else None,
                    "end": str(time_range[1]) if time_range[1] else None
                },
                "processing_time": 0,  # Will be set by caller
                "analysis_method": "spark_mllib",
                "spark_clusters": 5,  # Number of KMeans clusters
                "feature_count": 8   # Number of features used
            }
            
            return stats
            
        except Exception as e:
            print(f"Error generating Spark statistics: {e}")
            return {
                "total_logs": len(anomalies),
                "anomalies_found": len(anomalies),
                "error": str(e)
            }
    
    def batch_analyze_uploaded_files(self, upload_directory="uploads"):
        """
        Batch analyze all files in the upload directory
        """
        print(f"\n{'='*60}")
        print(f"BATCH ANALYSIS OF UPLOADED FILES")
        print(f"{'='*60}")
        
        if not os.path.exists(upload_directory):
            print(f"Upload directory {upload_directory} not found!")
            return
        
        files = [f for f in os.listdir(upload_directory) if f.endswith(('.log', '.txt'))]
        
        if not files:
            print("No log files found in upload directory!")
            return
        
        print(f"Found {len(files)} files to analyze...")
        
        results = {}
        for filename in files:
            filepath = os.path.join(upload_directory, filename)
            print(f"\n🔍 Analyzing: {filename}")
            
            success, result = self.analyze_uploaded_dataset_with_spark(filepath)
            results[filename] = {
                "success": success,
                "result": result,
                "file_size": os.path.getsize(filepath)
            }
            
            if success:
                print(f"✅ {filename} - Analysis completed successfully")
            else:
                print(f"❌ {filename} - Analysis failed: {result}")
        
        # Summary report
        print(f"\n{'='*60}")
        print(f"BATCH ANALYSIS SUMMARY")
        print(f"{'='*60}")
        
        successful = len([r for r in results.values() if r['success']])
        failed = len(results) - successful
        
        print(f"Total files: {len(results)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        
        if successful > 0:
            total_anomalies = sum([
                r['result'].get('anomalies', 0) for r in results.values() 
                if r['success'] and isinstance(r['result'], dict)
            ])
            print(f"Total anomalies found across all files: {total_anomalies}")
        
        return results

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
