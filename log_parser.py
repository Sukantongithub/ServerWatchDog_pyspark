"""
Log Parser and Feature Extraction using PySpark
Parses log files and extracts features for anomaly detection
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, when, length, hour, minute, 
    count, avg, stddev, unix_timestamp, window, lit
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.ml.feature import VectorAssembler, StandardScaler
import re

class LogParser:
    def __init__(self, spark):
        self.spark = spark
        
    def parse_log_line(self, log_df):
        """
        Parse log line and extract components
        Expected format: TIMESTAMP [LEVEL] SERVICE - MESSAGE | IP: x.x.x.x | User: user_xxxx | ResponseTime: xxxms
        """
        # Extract timestamp
        parsed_df = log_df.withColumn(
            "timestamp",
            regexp_extract(col("value"), r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})", 1)
        )
        
        # Extract log level
        parsed_df = parsed_df.withColumn(
            "level",
            regexp_extract(col("value"), r"\[(INFO|WARN|ERROR|DEBUG)\]", 1)
        )
        
        # Extract service name
        parsed_df = parsed_df.withColumn(
            "service",
            regexp_extract(col("value"), r"\] ([a-z\-]+) -", 1)
        )
        
        # Extract message
        parsed_df = parsed_df.withColumn(
            "message",
            regexp_extract(col("value"), r"- (.*?) \|", 1)
        )
        
        # Extract IP address
        parsed_df = parsed_df.withColumn(
            "ip_address",
            regexp_extract(col("value"), r"IP: ([0-9\.]+)", 1)
        )
        
        # Extract user
        parsed_df = parsed_df.withColumn(
            "user",
            regexp_extract(col("value"), r"User: ([a-z0-9_]+)", 1)
        )
        
        # Extract response time
        parsed_df = parsed_df.withColumn(
            "response_time",
            regexp_extract(col("value"), r"ResponseTime: (\d+)ms", 1).cast(IntegerType())
        )
        
        # Convert timestamp to proper timestamp type
        parsed_df = parsed_df.withColumn(
            "timestamp",
            unix_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS").cast(TimestampType())
        )
        
        return parsed_df
    
    def extract_features(self, parsed_df):
        """
        Extract features for anomaly detection
        """
        # Create numeric features from log level
        feature_df = parsed_df.withColumn(
            "level_error",
            when(col("level") == "ERROR", 1).otherwise(0)
        ).withColumn(
            "level_warn",
            when(col("level") == "WARN", 1).otherwise(0)
        ).withColumn(
            "level_info",
            when(col("level") == "INFO", 1).otherwise(0)
        )
        
        # Extract hour and minute from timestamp
        feature_df = feature_df.withColumn("hour", hour(col("timestamp")))
        feature_df = feature_df.withColumn("minute", minute(col("timestamp")))
        
        # Message length
        feature_df = feature_df.withColumn("message_length", length(col("message")))
        
        # Check for suspicious patterns in message
        feature_df = feature_df.withColumn(
            "has_error_keyword",
            when(
                col("message").rlike("(?i)(timeout|error|exception|failed|critical|denied|exhausted|deadlock)"),
                1
            ).otherwise(0)
        )
        
        # Check for suspicious IP patterns
        feature_df = feature_df.withColumn(
            "suspicious_ip",
            when(
                col("ip_address").rlike("(999|666|0\\.0\\.0\\.0)"),
                1
            ).otherwise(0)
        )
        
        # Response time categories
        feature_df = feature_df.withColumn(
            "high_response_time",
            when(col("response_time") > 1000, 1).otherwise(0)
        )
        
        feature_df = feature_df.withColumn(
            "very_high_response_time",
            when(col("response_time") > 5000, 1).otherwise(0)
        )
        
        return feature_df
    
    def create_feature_vector(self, feature_df):
        """
        Create feature vector for ML models
        """
        feature_columns = [
            "response_time",
            "level_error",
            "level_warn",
            "level_info",
            "hour",
            "minute",
            "message_length",
            "has_error_keyword",
            "suspicious_ip",
            "high_response_time",
            "very_high_response_time"
        ]
        
        # Assemble features
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
        
        scaler_model = scaler.fit(assembled_df)
        scaled_df = scaler_model.transform(assembled_df)
        
        return scaled_df, scaler_model
    
    def aggregate_features(self, feature_df, window_duration="1 minute"):
        """
        Aggregate features over time windows
        """
        aggregated_df = feature_df.groupBy(
            window(col("timestamp"), window_duration),
            col("service")
        ).agg(
            count("*").alias("log_count"),
            avg("response_time").alias("avg_response_time"),
            stddev("response_time").alias("stddev_response_time"),
            count(when(col("level") == "ERROR", 1)).alias("error_count"),
            count(when(col("level") == "WARN", 1)).alias("warn_count"),
            count(when(col("suspicious_ip") == 1, 1)).alias("suspicious_ip_count")
        )
        
        return aggregated_df

def create_spark_session():
    """
    Create and configure Spark session
    """
    spark = SparkSession.builder \
        .appName("LogAnomalyDetection") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.warehouse.dir", "./spark-warehouse") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

if __name__ == "__main__":
    # Test the parser
    spark = create_spark_session()
    
    # Read sample logs
    log_df = spark.read.text("logs/server_logs.log")
    
    parser = LogParser(spark)
    parsed_df = parser.parse_log_line(log_df)
    feature_df = parser.extract_features(parsed_df)
    
    print("Parsed logs sample:")
    feature_df.show(10, truncate=False)
    
    print("\nFeature statistics:")
    feature_df.describe().show()
    
    spark.stop()
