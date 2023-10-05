FROM python:3.9-slim

WORKDIR /app

COPY csv_to_kafka.py .

COPY *.csv .

RUN pip install confluent-kafka

CMD ["python", "csv_to_kafka.py"]
