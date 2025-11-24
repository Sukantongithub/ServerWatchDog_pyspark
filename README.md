# 🔍 Real-time Log Anomaly Detection System

A comprehensive PySpark-based system for detecting anomalies in server logs using Machine Learning, Graph Analysis, and Real-time Visualization.

## 🌟 Features

- **Real-time Log Generation**: Simulates realistic server logs with normal and anomalous patterns
- **PySpark Processing**: Efficient distributed log parsing and feature extraction
- **ML-based Detection**:
  - KMeans clustering for pattern recognition
  - Isolation Forest algorithm for outlier detection
  - Ensemble approach combining multiple methods
- **Graph Analysis**: NetworkX-based relationship analysis between services, IPs, and users
- **Interactive Dashboard**:
  - Real-time anomaly alerts with WebSocket updates
  - Interactive visualizations (Plotly charts, D3.js network graphs)
  - Anomaly timeline and score distribution
- **External Access**: Ngrok integration for remote dashboard access
- **Spark Web UI**: Monitor Spark jobs and performance at `http://localhost:4040`
- **Model Persistence**: Train once, use for future predictions

## 📋 Prerequisites

- Python 3.8 or higher
- Java 8 or higher (for PySpark)
- 4GB+ RAM recommended

## 🚀 Installation

### 1. Clone or Download the Project

```powershell
cd m:\Spark3
```

### 2. Create Virtual Environment (Recommended)

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 3. Install Dependencies

```powershell
pip install -r requirements.txt
```

## ⚙️ Configuration

Edit `config.py` to customize:

- Spark configuration (memory, cores)
- Ngrok authentication token (already configured)
- Log generation parameters
- ML model parameters
- Alert thresholds

## 🎯 Usage

### Quick Start (All-in-One)

Run the entire system with one command:

```powershell
python main.py
```

This will:

1. Initialize PySpark
2. Generate training data (30 seconds)
3. Train the ML model
4. Start log generator
5. Start real-time anomaly detection
6. Launch web dashboard
7. Start ngrok tunnel for external access
8. Start Spark Web UI

### Access the Dashboards

Once running, you'll see:

- **Main Dashboard**: `http://localhost:5000` or the ngrok public URL
- **Spark Web UI**: `http://localhost:4040`

### Running Components Separately

#### 1. Generate Logs Only

```powershell
python log_generator.py
```

#### 2. Train Model

```powershell
python anomaly_detector.py
```

#### 3. Run Graph Analysis

```powershell
python graph_analyzer.py
```

#### 4. Start Web Dashboard

```powershell
python web_app.py
```

#### 5. Start Ngrok Tunnel

```powershell
python ngrok_tunnel.py
```

## 📊 Dashboard Features

### Real-time Statistics

- Total logs processed
- Anomalies detected
- Anomaly rate percentage
- Critical alerts count

### Visualizations

1. **Anomaly Timeline**: Shows anomaly trends over time
2. **Score Distribution**: Histogram of anomaly scores
3. **Network Graph**: Interactive visualization of service interactions
4. **Anomaly Table**: Recent anomalies with details

### Alerts

- Critical anomalies (score > 0.8) trigger visual alerts
- Color-coded severity levels (High/Medium/Low)
- Real-time WebSocket updates

## 🧠 Machine Learning Model

### Training

The system automatically trains on initial log data:

- **Algorithm**: KMeans clustering (k=5) + Isolation Forest
- **Features**:
  - Response time
  - Log level (ERROR, WARN, INFO)
  - Time of day (hour, minute)
  - Message characteristics
  - IP patterns
  - Error keywords

### Model Persistence

- Trained models are saved to `models/` directory
- Automatically loads existing models on restart
- Retrain by deleting the model directory

## 📈 Graph Analysis

Analyzes relationships between:

- **Services**: auth-service, api-gateway, user-service, etc.
- **IP Addresses**: Identifies suspicious IPs
- **Users**: Tracks user behavior patterns

Detects:

- IPs with unusually high connection rates
- Services with high error rates
- Anomalous user-service interactions

## 🔧 Project Structure

```
Spark3/
├── main.py                  # Main orchestrator
├── config.py                # Configuration settings
├── log_generator.py         # Live log data generator
├── log_parser.py            # PySpark log parsing
├── anomaly_detector.py      # ML-based detection
├── graph_analyzer.py        # GraphX analysis
├── web_app.py              # Flask web server
├── ngrok_tunnel.py         # Ngrok integration
├── requirements.txt         # Python dependencies
├── templates/
│   └── dashboard.html      # Web dashboard UI
├── static/
│   └── graph_data.json     # Graph visualization data
├── logs/
│   └── server_logs.log     # Generated logs
└── models/
    └── anomaly_detection_model/  # Trained ML model
```

## 🎨 Customization

### Adjust Anomaly Detection Sensitivity

In `config.py`:

```python
ANOMALY_THRESHOLD = 0.7  # Lower = more sensitive
CRITICAL_THRESHOLD = 0.9  # Lower = more critical alerts
```

### Modify Log Patterns

Edit `log_generator.py` to add custom:

- Normal log messages
- Anomalous patterns
- Services
- Error types

### Change ML Parameters

In `config.py`:

```python
KMEANS_K = 5  # Number of clusters
ISOLATION_FOREST_CONTAMINATION = 0.1  # Expected anomaly rate
```

## 📝 Log Format

Generated logs follow this format:

```
TIMESTAMP [LEVEL] SERVICE - MESSAGE | IP: x.x.x.x | User: user_xxxx | ResponseTime: xxxms
```

Example:

```
2025-11-19 10:30:45.123 [ERROR] auth-service - Connection timeout after 30000ms | IP: 10.0.0.666 | User: user_0042 | ResponseTime: 5432ms
```

## 🐛 Troubleshooting

### Issue: Spark fails to start

**Solution**: Ensure Java 8+ is installed and JAVA_HOME is set

### Issue: Ngrok connection fails

**Solution**: Verify the auth token in `config.py`

### Issue: Model training fails

**Solution**: Ensure logs are being generated first. Run `log_generator.py` for 30 seconds

### Issue: Dashboard shows no data

**Solution**: Wait a few seconds for initial data generation and processing

### Issue: Port already in use

**Solution**: Change FLASK_PORT in `config.py` or kill the process using port 5000

## 🔒 Security Notes

- The ngrok token is for development/testing only
- In production, use proper authentication
- Sanitize log data before processing
- Implement rate limiting for the web interface

## 📚 Technologies Used

- **PySpark 3.5**: Distributed data processing
- **Spark MLlib**: Machine learning algorithms
- **Flask**: Web framework
- **Socket.IO**: Real-time communication
- **Plotly**: Interactive charts
- **D3.js**: Network graph visualization
- **Ngrok**: Secure tunneling
- **NetworkX**: Graph analysis

## 🎓 Learning Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [Anomaly Detection Techniques](https://en.wikipedia.org/wiki/Anomaly_detection)

## 📄 License

This project is for educational and demonstration purposes.

## 👥 Contributing

Feel free to fork and enhance! Suggested improvements:

- Add more ML algorithms
- Implement deep learning models
- Add email/SMS alerts
- Create mobile dashboard
- Add log filtering and search

## 🚀 Performance Tips

1. **Increase Spark Memory**: Edit `log_parser.py` to increase driver/executor memory
2. **Batch Processing**: Adjust `BATCH_INTERVAL` in `config.py`
3. **Graph Limits**: Modify graph export to include more/fewer nodes
4. **Model Updates**: Periodically retrain model with new data patterns

## 📞 Support

For issues or questions:

1. Check the troubleshooting section
2. Review Spark logs in the console
3. Check Spark UI at `http://localhost:4040`

---

**Happy Anomaly Hunting! 🔍🎯**
