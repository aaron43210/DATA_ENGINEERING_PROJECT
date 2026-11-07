FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY sensor_event.avsc ./sensor_event.avsc
COPY sensor_producer.py ./sensor_producer.py
RUN useradd --system --uid 10001 --no-create-home producer
USER producer
CMD ["python", "-u", "sensor_producer.py"]
