"""
Graph Analysis using NetworkX for Log Anomaly Detection
Analyzes relationships between services, IPs, and users
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, collect_list, struct, explode
import networkx as nx
import json

class GraphAnalyzer:
    def __init__(self, spark):
        self.spark = spark

    def create_service_graph(self, log_df):
        """
        Create a graph using NetworkX to represent service interactions.
        Nodes: services, IPs, users
        Edges: interactions with weights (frequency)
        """
        G = nx.Graph()
        
        # Collect vertices
        vertices = log_df.select("service", "ip_address", "user").distinct().collect()
        for row in vertices:
            G.add_node(row.service, type="service")
            G.add_node(row.ip_address, type="ip")
            G.add_node(row.user, type="user")
        
        # Collect edges for IP-Service
        ip_edges = log_df.groupBy("ip_address", "service").agg(count("*").alias("weight")).collect()
        for row in ip_edges:
            G.add_edge(row.ip_address, row.service, weight=row.weight)
        
        # Collect edges for User-Service
        user_edges = log_df.groupBy("user", "service").agg(count("*").alias("weight")).collect()
        for row in user_edges:
            G.add_edge(row.user, row.service, weight=row.weight)
        
        return G

    def detect_graph_anomalies(self, G):
        """
        Detect anomalies in the graph structure using NetworkX.
        """
        # Calculate degree centrality
        degrees = dict(G.degree())
        
        # Identify nodes with unusually high degrees (anomalies)
        avg_degree = sum(degrees.values()) / len(degrees) if degrees else 0
        anomalies = {node: degree for node, degree in degrees.items() if degree > avg_degree * 2}
        
        return anomalies

    def export_graph_for_visualization(self, G, output_path="graph_data.json"):
        """
        Export graph data for web visualization.
        """
        nodes = [{"id": node, "type": data.get("type", "unknown")} for node, data in G.nodes(data=True)]
        links = [{"source": u, "target": v, "value": data.get("weight", 1)} for u, v, data in G.edges(data=True)]
        
        graph_data = {
            "nodes": nodes[:100],  # Limit for visualization
            "links": links[:200]
        }
        
        with open(output_path, 'w') as f:
            json.dump(graph_data, f, indent=2)
        
        print(f"Graph data exported to {output_path}")
        return graph_data


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
    graph = analyzer.create_service_graph(feature_df)

    print("Graph created using NetworkX.")

    # Detect anomalies
    anomalies = analyzer.detect_graph_anomalies(graph)

    print("\nAnomalous nodes:")
    for node, degree in anomalies.items():
        print(f"Node: {node}, Degree: {degree}")

    # Export for visualization
    analyzer.export_graph_for_visualization(graph, "static/graph_data.json")

    spark.stop()
