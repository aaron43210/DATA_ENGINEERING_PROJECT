FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka[avro]==2.4.0 requests==2.31.0 paho-mqtt==1.6.1

COPY sensor_event.avsc .
COPY sensor_producer.py .

CMD ["python", "-u", "sensor_producer.py"]
