# Stream Data Processing Pipeline

A real-time data processing pipeline that collects random user data, processes it with Apache Spark, stores results in MongoDB, and visualizes insights with Grafana.

## Architecture

1. **Data Collection**: Python script fetches data from RandomUser.me API and sends it to Kafka
2. **Data Processing**: Spark Structured Streaming processes the data in real-time
3. **Data Storage**: Processed results are stored in MongoDB
4. **Visualization**: Grafana dashboards display real-time analytics on user demographics

## Tech Stack

- Apache Kafka: Stream processing platform
- Apache Spark Structured Streaming: Real-time data processing
- MongoDB: NoSQL database for storing analysis results
- Grafana: Real-time visualization dashboard

## Project Structure

```
stream-data-pipeline/
├── config/                    # Configuration files
│   ├── grafana_dashboard.json         # Grafana dashboard configuration
│   ├── grafana_dashboard_provider.yaml # Grafana dashboard provider
│   └── grafana_datasource.yaml        # Grafana data source configuration
├── docker-compose.yaml        # Docker services configuration
├── requirements.txt           # Python dependencies
└── src/
    ├── data_producer/         # Kafka producer fetching data from RandomUser.me
    │   ├── data_producer.py   # Main producer script
    │   └── Dockerfile         # Docker configuration for producer
    ├── spark_processor/       # Spark streaming jobs
    │   ├── spark_processor.py # Main Spark processing script
    │   └── Dockerfile         # Docker configuration for Spark processor
    └── mongodb_connector/     # MongoDB connection utilities
        └── mongodb_connector.py # MongoDB connector utility
```

## Getting Started

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/stream-data-pipeline.git
   cd stream-data-pipeline
   ```
2. Install packages:
   ```
   pip install -r requirements.txt
   ```
3. Start the services:
   ```bash
   docker-compose up -d
   ```

4. Be patient and wait for services to start :)

5. Access the Grafana dashboard:
   - URL: http://localhost:3000
   - Username: admin
   - Password: admin

6. Access MongoDB Express to view the data:
   - URL: http://localhost:8082
   - Username: admin
   - Password: admin

7. Access Spark UI to monitor Spark jobs:
   - URL: http://localhost:8080

## Pipeline workflow

1. **Data Producer**:
   - Fetches random user data from the RandomUser.me API
   - Sends data to Kafka topic "user_data"

2. **Spark Processor**:
   - Reads data from Kafka
   - Processes the data to extract insights
   - Creates several analytics: gender distribution, age distribution, country distribution, user inflow, and average age by country

3. **MongoDB Storage**:
   - Stores processed analytics in different collections

4. **Grafana Visualization**:
   - Displays real-time dashboards with user demographics