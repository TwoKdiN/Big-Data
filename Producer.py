import csv
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'test2'

# Load data from CSV file
def load_vehicle_data(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        return list(reader)

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Send vehicle data to Kafka
def send_vehicle_data(data, start_time, interval):
    for record in data:
        vehicle_time = start_time + timedelta(seconds=int(record['t']))
        record['timestamp'] = vehicle_time.strftime('%Y-%m-%d %H:%M:%S')

        # Check if vehicle is in motion
        if float(record['x']) != -1.0 and float(record['v']) != -1.0:
            producer.send(TOPIC, record)
            print(f"Sent data: {record}")

        time.sleep(interval)

if __name__ == '__main__':
    # File path to the CSV file
    file_path = 'D:/ΣΧΟΛΗ/Συστήματα Διαχείρισης Μεγάλων Δεδομένων/Project/Code/Big-Data/out/data_vehicles.csv'

    # Load vehicle data
    vehicle_data = load_vehicle_data(file_path)

    # Start time of the producer
    start_time = datetime.now()

    # Interval in seconds
    interval = 1  # Update this value as needed

    # Send data to Kafka periodically
    send_vehicle_data(vehicle_data, start_time, interval)
