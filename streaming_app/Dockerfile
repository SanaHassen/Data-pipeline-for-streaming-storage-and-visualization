FROM python:3.6-slim

WORKDIR /app

COPY requirements.txt /app/

RUN pip install -r requirements.txt

COPY . /app/
# Start your Spark Streaming application (replace with your actual command)
CMD ["python", "app.py"]