FROM bitnami/spark:3.5.0

USER root

# Install Python dependencies and wget
RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/spark_processor/ .
COPY src/mongodb_connector/ ./mongodb_connector/

# Set environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC=user_data
ENV MONGODB_HOST=mongodb
ENV MONGODB_PORT=27017
ENV MONGODB_USERNAME=admin
ENV MONGODB_PASSWORD=admin
ENV MONGODB_DATABASE=user_analytics

# Run the Spark processor
CMD ["python3", "spark_processor.py"] 