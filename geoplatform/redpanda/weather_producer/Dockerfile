FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    confluent-kafka[avro]==2.4.0 \
    requests==2.31.0

# Copy schema and producer code
COPY weather_event.avsc .
COPY weather_producer.py .

# Health check — verify producer process is running
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD pgrep -f weather_producer.py || exit 1

CMD ["python", "-u", "weather_producer.py"]
