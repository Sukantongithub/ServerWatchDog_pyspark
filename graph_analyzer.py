"""
Graph Analysis using GraphX for Log Anomaly Detection
Analyzes relationships between services, IPs, and users
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, collect_list, struct, explode, when, lit, sum as sql_sum
import networkx as nx
import json
import os

class GraphAnalyzer:
    def __init__(self, spark):
        self.spark = spark
        self.graph = None
        
    def create_service_graph(self, log_df):
        """
        Create a graph representing service interactions
        Nodes: services, IPs, users
        Edges: interactions with weights (frequency)
        """
        # Service nodes
        service_nodes = log_df.select(
            col("service").alias("id"),
            lit("service").alias("type")
        ).distinct()
        
        # IP nodes
        ip_nodes = log_df.select(
            col("ip_address").alias("id"),
            lit("ip").alias("type")
        ).distinct()
        
        # User nodes
        user_nodes = log_df.select(
            col("user").alias("id"),
            lit("user").alias("type")
        ).distinct()
        
        # Combine all vertices
        vertices = service_nodes.union(ip_nodes).union(user_nodes)
        
        # Create edges
        
        # IP -> Service edges
        ip_service_edges = log_df.groupBy("ip_address", "service").agg(
            count("*").alias("weight"),
            avg("response_time").alias("avg_response_time"),
            count(when(col("level") == "ERROR", 1)).alias("error_count")
        ).select(
            col("ip_address").alias("src"),
            col("service").alias("dst"),
            col("weight"),
            col("avg_response_time"),
            col("error_count")
        )
        
        # User -> Service edges
        user_service_edges = log_df.groupBy("user", "service").agg(
            count("*").alias("weight"),
            avg("response_time").alias("avg_response_time"),
            count(when(col("level") == "ERROR", 1)).alias("error_count")
        ).select(
            col("user").alias("src"),
            col("service").alias("dst"),
            col("weight"),
            col("avg_response_time"),
            col("error_count")
        )
        
        # Combine all edges
        edges = ip_service_edges.union(user_service_edges)
        
        return vertices, edges
    
    def detect_graph_anomalies(self, vertices, edges):
        """
        Detect anomalies in the graph structure
        """
        # Calculate node degrees
        out_degree = edges.groupBy("src").agg(
            count("*").alias("out_degree"),
            sql_sum("weight").alias("total_interactions")
        ).withColumnRenamed("src", "node_id")
        
        in_degree = edges.groupBy("dst").agg(
            count("*").alias("in_degree"),
            sql_sum("weight").alias("total_incoming")
        ).withColumnRenamed("dst", "node_id")
        
        # Merge vertices with degree information
        node_stats = vertices.join(
            out_degree, vertices.id == out_degree.node_id, "left"
        ).drop("node_id")
        
        node_stats = node_stats.join(
            in_degree, vertices.id == in_degree.node_id, "left"
        ).drop("node_id")
        
        # Fill null values
        node_stats = node_stats.fillna(0)
        
        # Calculate anomaly scores based on graph properties
        
        # IPs with unusually high connections
        ip_stats = node_stats.filter(col("type") == "ip")
        if (ip_stats.count() > 0):
            avg_out_degree = ip_stats.agg(avg("out_degree")).collect()[0][0] or 1
            
            node_stats = node_stats.withColumn(
                "degree_anomaly_score",
                when(
                    col("type") == "ip",
                    (col("out_degree") / avg_out_degree)
                ).otherwise(1.0)
            )
        else:
            node_stats = node_stats.withColumn("degree_anomaly_score", lit(1.0))
        
        # Normalize anomaly score
        node_stats = node_stats.withColumn(
            "graph_anomaly_score",
            when(col("degree_anomaly_score") > 3, 1.0)
            .when(col("degree_anomaly_score") > 2, 0.8)
            .when(col("degree_anomaly_score") > 1.5, 0.5)
            .otherwise(0.1)
        )
        
        # Identify suspicious edges (high error rates, high response times)
        suspicious_edges = edges.withColumn(
            "edge_anomaly_score",
            when(col("error_count") > 5, 0.9)
            .when(col("avg_response_time") > 2000, 0.8)
            .when(col("error_count") > 2, 0.6)
            .otherwise(0.2)
        )
        
        return node_stats, suspicious_edges
    
    def find_anomalous_patterns(self, node_stats, edges):
        """
        Find common patterns in anomalous behavior
        """
        # Find IPs with high anomaly scores
        suspicious_ips = node_stats.filter(
            (col("type") == "ip") & (col("graph_anomaly_score") > 0.6)
        ).select("id", "graph_anomaly_score", "out_degree")
        
        # Find services frequently targeted by suspicious IPs
        suspicious_connections = edges.join(
            suspicious_ips,
            edges.src == suspicious_ips.id,
            "inner"
        ).select(
            col("src").alias("suspicious_ip"),
            col("dst").alias("target_service"),
            col("weight"),
            col("error_count"),
            col("graph_anomaly_score")
        )
        
        # Aggregate by service to find most targeted services
        targeted_services = suspicious_connections.groupBy("target_service").agg(
            count("*").alias("attack_count"),
            sql_sum("error_count").alias("total_errors"),
            avg("graph_anomaly_score").alias("avg_anomaly_score")
        ).orderBy(col("attack_count").desc())
        
        return suspicious_ips, targeted_services
    
    def export_graph_for_visualization(self, vertices, edges, output_path="static/graph_data.json"):
        """
        Export graph data for web visualization with improved format
        """
        try:
            # Convert to lists
            vertices_list = vertices.collect()
            edges_list = edges.collect()
            
            # Create nodes with improved structure
            nodes = []
            node_ids = set()
            
            for row in vertices_list:
                node_id = str(row.id) if row.id else "unknown"
                node_type = str(row.type) if row.type else "unknown"
                
                nodes.append({
                    "id": node_id,
                    "type": node_type,
                    "label": node_id
                })
                node_ids.add(node_id)
            
            # Create links with improved structure
            links = []
            for row in edges_list:
                src = str(row.src) if row.src else None
                dst = str(row.dst) if row.dst else None
                weight = int(row.weight) if row.weight else 1
                
                # Only add links where both nodes exist
                if src and dst and src in node_ids and dst in node_ids:
                    links.append({
                        "source": src,
                        "target": dst,
                        "value": max(weight, 1)
                    })
            
            graph_data = {
                "nodes": nodes[:150],  # Increased limit for visualization
                "links": links[:300]
            }
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
            
            # Write with better formatting
            with open(output_path, 'w') as f:
                json.dump(graph_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Graph data exported to {output_path}")
            print(f"   Nodes: {len(graph_data['nodes'])}")
            print(f"   Links: {len(graph_data['links'])}")
            return graph_data
        
        except Exception as e:
            print(f"❌ Error exporting graph data: {e}")
            import traceback
            traceback.print_exc()
            # Create empty graph structure on error
            empty_graph = {"nodes": [], "links": []}
            os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(empty_graph, f)
            return empty_graph
    
    def update_graph_periodically(self, log_df, output_path="static/graph_data.json"):
        """
        Update graph data from log dataframe
        """
        try:
            vertices, edges = self.create_service_graph(log_df)
            return self.export_graph_for_visualization(vertices, edges, output_path)
        except Exception as e:
            print(f"❌ Error updating graph: {e}")
            import traceback
            traceback.print_exc()
            return None


if __name__ == "__main__":
    from log_parser import create_spark_session, LogParser
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read and parse logs
    log_df = spark.read.text("logs/server_logs.log")
    parser = LogParser(spark)
    parsed_df = parser.parse_log_line(log_df)
    feature_df = parser.extract_features(parsed_df)
    
    # Create graph analyzer
    analyzer = GraphAnalyzer(spark)
    
    # Create graph
    vertices, edges = analyzer.create_service_graph(feature_df)
    
    print("Graph Statistics:")
    print(f"Number of vertices: {vertices.count()}")
    print(f"Number of edges: {edges.count()}")
    
    print("\nVertex types:")
    vertices.groupBy("type").count().show()
    
    print("\nTop edges by weight:")
    edges.orderBy(col("weight").desc()).show(10)
    
    # Detect anomalies
    node_stats, suspicious_edges = analyzer.detect_graph_anomalies(vertices, edges)
    
    print("\nNodes with high anomaly scores:")
    node_stats.filter(col("graph_anomaly_score") > 0.6).show()
    
    # Find patterns
    suspicious_ips, targeted_services = analyzer.find_anomalous_patterns(node_stats, edges)
    
    print("\nSuspicious IPs:")
    suspicious_ips.show()
    
    print("\nMost targeted services:")
    targeted_services.show()
    
    # Export for visualization
    analyzer.export_graph_for_visualization(vertices, edges, "static/graph_data.json")
    
    spark.stop()
