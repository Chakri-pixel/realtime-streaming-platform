FROM python:3.11-slim

WORKDIR /opt/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY event_generator.py .

ENV KAFKA_BOOTSTRAP=redpanda:29092 \
    TOPIC=clickstream-events \
    EVENT_RATE=40

CMD ["python", "event_generator.py"]
