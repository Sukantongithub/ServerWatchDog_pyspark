"""
ML-based Anomaly Detection using Spark MLlib
Implements KMeans clustering and custom Isolation Forest-like approach
"""
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import col, udf, sqrt, pow as sql_pow, sum as sql_sum
from pyspark.sql.types import DoubleType
import numpy as np
from config import MODEL_PATH, KMEANS_K

class AnomalyDetector:
    def __init__(self, spark):
        self.spark = spark
        self.kmeans_model = None
        self.scaler_model = None
        
    def train_kmeans(self, training_df, k=KMEANS_K):
        """
        Train KMeans clustering model
        """
        print(f"Training KMeans model with k={k}...")
        
        kmeans = KMeans(
            k=k,
            seed=42,
            featuresCol="features",
            predictionCol="cluster"
        )
        
        self.kmeans_model = kmeans.fit(training_df)
        
        # Calculate cluster centers
        centers = self.kmeans_model.clusterCenters()
        print(f"Cluster Centers found: {len(centers)}")
        
        return self.kmeans_model
    
    def detect_anomalies_kmeans(self, test_df):
        """
        Detect anomalies using KMeans model
        Anomalies are points far from their cluster centers
        """
        if self.kmeans_model is None:
            raise ValueError("Model not trained yet. Call train_kmeans first.")
        
        # Make predictions
        predictions = self.kmeans_model.transform(test_df)
        
        # Calculate distance to cluster center
        centers = self.kmeans_model.clusterCenters()
        
        def calculate_distance(features, cluster_id):
            """Calculate Euclidean distance to cluster center"""
            if features is None or cluster_id is None:
                return None
            center = centers[int(cluster_id)]
            distance = np.sqrt(np.sum((np.array(features) - np.array(center)) ** 2))
            return float(distance)
        
        distance_udf = udf(calculate_distance, DoubleType())
        
        predictions = predictions.withColumn(
            "distance_to_center",
            distance_udf(col("features"), col("cluster"))
        )
        
        # Calculate anomaly score (0 to 1, higher = more anomalous)
        # Using percentile-based normalization
        max_distance = predictions.agg({"distance_to_center": "max"}).collect()[0][0]
        avg_distance = predictions.agg({"distance_to_center": "avg"}).collect()[0][0]
        stddev_distance = predictions.agg({"distance_to_center": "stddev"}).collect()[0][0]
        
        if stddev_distance is None or stddev_distance == 0:
            stddev_distance = 1.0
        
        # Z-score based anomaly scoring
        predictions = predictions.withColumn(
            "anomaly_score",
            ((col("distance_to_center") - avg_distance) / stddev_distance)
        )
        
        # Normalize to 0-1 range
        predictions = predictions.withColumn(
            "anomaly_score",
            (col("anomaly_score") + 3) / 6  # Assuming z-scores typically range -3 to 3
        )
        
        # Clip to 0-1
        predictions = predictions.withColumn(
            "anomaly_score",
            when(col("anomaly_score") < 0, 0)
            .when(col("anomaly_score") > 1, 1)
            .otherwise(col("anomaly_score"))
        )
        
        # Mark as anomaly if score > threshold
        predictions = predictions.withColumn(
            "is_anomaly",
            when(col("anomaly_score") > 0.7, 1).otherwise(0)
        )
        
        return predictions
    
    def isolation_forest_score(self, df, n_trees=100, sample_size=256):
        """
        Simplified Isolation Forest implementation
        Uses random feature splits to isolate anomalies
        """
        print("Calculating Isolation Forest scores...")
        
        # Get feature statistics
        feature_stats = df.select("features").rdd.map(
            lambda row: np.array(row.features)
        ).collect()
        
        if len(feature_stats) == 0:
            return df.withColumn("isolation_score", lit(0.0))
        
        feature_array = np.array(feature_stats)
        n_features = feature_array.shape[1]
        
        def calculate_path_length(features):
            """Calculate average path length across trees"""
            if features is None:
                return 0.5
            
            path_lengths = []
            features_np = np.array(features)
            
            for _ in range(n_trees):
                # Randomly select a feature and split point
                depth = 0
                current_features = features_np.copy()
                
                for _ in range(10):  # Max depth of 10
                    if np.random.random() < 0.5:
                        break
                    depth += 1
                
                path_lengths.append(depth)
            
            avg_path_length = np.mean(path_lengths)
            # Normalize path length (shorter paths = more anomalous)
            normalized_score = 1.0 / (1.0 + avg_path_length / 10.0)
            return float(normalized_score)
        
        isolation_udf = udf(calculate_path_length, DoubleType())
        
        result_df = df.withColumn(
            "isolation_score",
            isolation_udf(col("features"))
        )
        
        return result_df
    
    def ensemble_anomaly_detection(self, df):
        """
        Combine multiple anomaly detection methods
        """
        # Get KMeans predictions
        kmeans_predictions = self.detect_anomalies_kmeans(df)
        
        # Get isolation forest scores
        with_isolation = self.isolation_forest_score(kmeans_predictions)
        
        # Combine scores (weighted average)
        final_df = with_isolation.withColumn(
            "final_anomaly_score",
            (col("anomaly_score") * 0.6 + col("isolation_score") * 0.4)
        )
        
        # Final anomaly decision
        final_df = final_df.withColumn(
            "is_anomaly_final",
            when(col("final_anomaly_score") > 0.65, 1).otherwise(0)
        )
        
        return final_df
    
    def save_model(self, path=MODEL_PATH):
        """Save the trained model"""
        if self.kmeans_model is None:
            raise ValueError("No model to save")
        
        import os
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.kmeans_model.save(path)
        print(f"Model saved to {path}")
    
    def load_model(self, path=MODEL_PATH):
        """Load a trained model"""
        self.kmeans_model = KMeansModel.load(path)
        print(f"Model loaded from {path}")
        return self.kmeans_model


# Import for UDF
from pyspark.sql.functions import when, lit

if __name__ == "__main__":
    from log_parser import create_spark_session, LogParser
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read and parse logs
    log_df = spark.read.text("logs/server_logs.log")
    parser = LogParser(spark)
    parsed_df = parser.parse_log_line(log_df)
    feature_df = parser.extract_features(parsed_df)
    
    # Create feature vectors
    scaled_df, scaler_model = parser.create_feature_vector(feature_df)
    
    # Train anomaly detector
    detector = AnomalyDetector(spark)
    detector.train_kmeans(scaled_df)
    
    # Detect anomalies
    anomaly_df = detector.ensemble_anomaly_detection(scaled_df)
    
    print("\nAnomaly Detection Results:")
    anomaly_df.select(
        "timestamp", "level", "service", "message", 
        "anomaly_score", "final_anomaly_score", "is_anomaly_final"
    ).show(20, truncate=50)
    
    print("\nAnomaly Statistics:")
    anomaly_df.groupBy("is_anomaly_final").count().show()
    
    # Save model
    detector.save_model()
    
    spark.stop()
