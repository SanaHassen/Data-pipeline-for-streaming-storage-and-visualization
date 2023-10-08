FROM python:3.6-slim

WORKDIR /app
# RUN  add-apt-repository ppa:openjdk-r/ppa
RUN apt-get update
RUN apt-get install openjdk-11-jdk -y

COPY requirements.txt /app/
RUN pip install -r requirements.txt


COPY . /app/

CMD ["python", "app.py"]
