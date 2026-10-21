FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY weather_event.avsc ./weather_event.avsc
COPY weather_producer.py ./weather_producer.py
CMD ["python", "-u", "weather_producer.py"]
