FROM python:3.11-slim

WORKDIR /app

COPY csv_to_kafka.py .

COPY *.csv .

RUN pip install confluent-kafka

RUN pip show confluent-kafka

CMD ["python", "csv_to_kafka.py"]
