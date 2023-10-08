from confluent_kafka import Producer
import csv
import threading
import time


print("csv_to_kafka started")

# # Configure Kafka Producer
producer = Producer({'bootstrap.servers': 'kafka:9092'})


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))



def send_to_kafka(file_name):
    topic = file_name[:-4]  # Remove the .csv extension to use as the topic name
    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header
        for row in reader:
            producer.produce(topic, key="key", value=','.join(row), callback=delivery_report)


# # Define CSV files
csv_files = ['orders.csv', 'bikes.csv', 'bikeshops.csv']

# # Create a thread for each file
threads = []
for file_name in csv_files:
    thread = threading.Thread(target=send_to_kafka, args=(file_name,))
    threads.append(thread)
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

# # Close producer
producer.flush()

print("csv_to_kafka ended")