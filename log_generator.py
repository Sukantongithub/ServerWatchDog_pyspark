"""
Log Generator for GraphX-based Log Anomaly Detection
Generates synthetic logs with fields: timestamp, service, ip_address, user, response_time, log_level
"""

import random
import time
from datetime import datetime

class LogGenerator:
    def __init__(self, output_file="logs/server_logs.log", num_logs=1000, anomaly_rate=0.05):
        self.output_file = output_file
        self.num_logs = num_logs
        self.anomaly_rate = anomaly_rate

        # Define services, IPs, and users
        self.services = ["auth-service", "payment-service", "inventory-service", "user-service", "email-service"]
        self.ip_addresses = [f"192.168.1.{i}" for i in range(1, 51)]
        self.users = [f"user{i}" for i in range(1, 101)]
        self.log_levels = ["INFO", "WARN", "ERROR"]

    def generate_log_line(self):
        """
        Generate a single log line with normal or anomalous patterns.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_level = random.choice(self.log_levels)
        service = random.choice(self.services)
        ip_address = random.choice(self.ip_addresses)
        user = random.choice(self.users)
        response_time = random.randint(50, 500)  # Normal response time in ms
        message = "Operation completed successfully"

        # Introduce anomalies based on the anomaly rate
        if random.random() < self.anomaly_rate:
            # Anomalous log: Unusual response time or invalid service
            response_time = random.randint(1000, 5000)  # Very high response time
            if random.random() < 0.5:
                service = "unknown-service"  # Invalid service name
            log_level = "ERROR"
            message = "Critical failure occurred"

        # NOTE: parsers expect the level to be wrapped in brackets like: "TIMESTAMP [LEVEL] SERVICE - MESSAGE | IP: ..."
        log_line = (
            f"{timestamp} [{log_level}] {service} - {message} | "
            f"IP: {ip_address} | User: {user} | ResponseTime: {response_time}ms"
        )
        return log_line

    def generate_logs(self):
        """
        Generate logs and write them to the output file.
        """
        # Use append mode so downstream processors watching the file can tail new records
        with open(self.output_file, "a", buffering=1) as f:
            for _ in range(self.num_logs):
                log_line = self.generate_log_line()
                f.write(log_line + "\n")
                f.flush()
                # Small sleep to simulate live generation
                time.sleep(0.01)
        print(f"Generated {self.num_logs} logs in {self.output_file}")

    def start(self):
        """
        Start the log generation process.
        """
        self.generate_logs()

    def stop(self):
        """
        Stop the log generation process (placeholder).
        """
        print("Log generation process stopped.")


if __name__ == "__main__":
    # Generate logs
    generator = LogGenerator(num_logs=1000, anomaly_rate=0.1)
    generator.generate_logs()
