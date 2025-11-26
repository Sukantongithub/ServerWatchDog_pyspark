"""
Live Log Generator for Anomaly Detection Testing
Generates realistic server logs with normal and anomalous patterns
"""
import random
import time
from datetime import datetime
import threading
import os
from config import LOG_FILE_PATH, LOG_GENERATION_INTERVAL, ANOMALY_RATE

class LogGenerator:
    def __init__(self):
        self.running = False
        self.log_count = 0
        
        # Normal log patterns
        self.log_levels = ["INFO", "WARN", "ERROR", "DEBUG"]
        self.services = ["auth-service", "api-gateway", "user-service", "payment-service", "db-service"]
        self.normal_messages = [
            "Request processed successfully",
            "User authentication successful",
            "Database query executed",
            "Cache hit for user data",
            "API endpoint accessed",
            "Session created",
            "Transaction completed",
            "Health check passed",
            "Configuration loaded",
            "Connection established"
        ]
        
        # Anomalous patterns
        self.anomaly_patterns = [
            ("ERROR", "Connection timeout after 30000ms"),
            ("ERROR", "OutOfMemoryError: Java heap space"),
            ("ERROR", "Database connection pool exhausted"),
            ("ERROR", "SQL injection attempt detected"),
            ("ERROR", "Unauthorized access attempt from IP {}"),
            ("WARN", "Unusually high number of requests from {}"),
            ("ERROR", "Failed login attempts exceeded threshold"),
            ("ERROR", "Disk space critically low: {}% remaining"),
            ("ERROR", "Service unavailable - circuit breaker open"),
            ("ERROR", "Rate limit exceeded for user {}"),
            ("ERROR", "Deadlock detected in transaction processing"),
            ("ERROR", "Malformed request payload"),
            ("WARN", "Response time exceeded SLA: {}ms"),
            ("ERROR", "Null pointer exception in service"),
            ("ERROR", "Invalid authentication token")
        ]
        
        self.ip_addresses = [
            f"192.168.{random.randint(1,255)}.{random.randint(1,255)}"
            for _ in range(100)
        ]
        
        self.suspicious_ips = [
            "10.0.0.666", "192.168.999.999", "172.16.0.1", "203.0.113.0"
        ]
        
        self.user_ids = [f"user_{i:04d}" for i in range(1, 501)]
        
    def generate_normal_log(self):
        """Generate a normal log entry"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level = random.choices(
            self.log_levels, 
            weights=[70, 20, 5, 5]  # Most logs are INFO
        )[0]
        service = random.choice(self.services)
        message = random.choice(self.normal_messages)
        ip = random.choice(self.ip_addresses)
        user = random.choice(self.user_ids)
        response_time = random.randint(50, 500)
        
        log_entry = f"{timestamp} [{level}] {service} - {message} | IP: {ip} | User: {user} | ResponseTime: {response_time}ms"
        return log_entry
    
    def generate_anomalous_log(self):
        """Generate an anomalous log entry"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level, message_template = random.choice(self.anomaly_patterns)
        service = random.choice(self.services)
        
        # Fill in dynamic values
        if "{}" in message_template:
            if "IP" in message_template:
                ip = random.choice(self.suspicious_ips)
                message = message_template.format(ip)
            elif "%" in message_template:
                message = message_template.format(random.randint(1, 5))
            elif "ms" in message_template:
                message = message_template.format(random.randint(5000, 15000))
            elif "user" in message_template:
                message = message_template.format(random.choice(self.user_ids))
            else:
                message = message_template.format(random.choice(self.suspicious_ips))
        else:
            message = message_template
        
        ip = random.choice(self.suspicious_ips) if random.random() < 0.5 else random.choice(self.ip_addresses)
        user = random.choice(self.user_ids)
        response_time = random.randint(1000, 10000)  # Anomalous logs have higher response times
        
        log_entry = f"{timestamp} [{level}] {service} - {message} | IP: {ip} | User: {user} | ResponseTime: {response_time}ms"
        return log_entry
    
    def generate_log(self):
        """Generate a single log entry (normal or anomalous)"""
        if random.random() < ANOMALY_RATE:
            return self.generate_anomalous_log()
        else:
            return self.generate_normal_log()
    
    def write_log(self, log_entry):
        """Write log entry to file"""
        os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
        with open(LOG_FILE_PATH, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
        self.log_count += 1
    
    def start(self):
        """Start generating logs"""
        self.running = True
        print(f"Starting log generation... Writing to {LOG_FILE_PATH}")
        
        def generate_loop():
            while self.running:
                log_entry = self.generate_log()
                self.write_log(log_entry)
                
                if self.log_count % 100 == 0:
                    print(f"Generated {self.log_count} log entries...")
                
                time.sleep(LOG_GENERATION_INTERVAL)
        
        self.thread = threading.Thread(target=generate_loop, daemon=True)
        self.thread.start()
    
    def stop(self):
        """Stop generating logs"""
        self.running = False
        print(f"\nLog generation stopped. Total logs generated: {self.log_count}")

if __name__ == "__main__":
    generator = LogGenerator()
    generator.start()
    
    try:
        print("Press Ctrl+C to stop log generation...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        generator.stop()
