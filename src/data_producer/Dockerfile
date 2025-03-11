FROM python:3.9-slim

WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/data_producer/ .

# Set environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC=user_data
ENV BATCH_SIZE=10
ENV INTERVAL_SECONDS=5

# Run the data producer
CMD ["python", "data_producer.py"] 