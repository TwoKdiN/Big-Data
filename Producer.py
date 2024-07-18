import csv
import json
import time
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'test'

# Load data from CSV file
def load_vehicle_data(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        return list(reader)

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

JSON_DIR = '/Users/twokdin/Documents/GitHub/Big-Data/json'

# Save JSON file
def save_json_file(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file)
        
        
# Send vehicle data to Kafka
def send_vehicle_data(data, start_time, interval):
    for record in data:
        vehicle_time = start_time + timedelta(seconds=int(record['t']))
        record['timestamp'] = vehicle_time.strftime('%Y-%m-%d %H:%M:%S')

        # Check if vehicle is in motion
        if float(record['x']) != -1.0 and float(record['v']) != -1.0:
            producer.send(TOPIC, record)
            print(f"Sent data: {record}")
            
            # Save full record as JSON file
            # filename_full = os.path.join(JSON_DIR, f"{record['name']}_{record['t']}_full.json")
            # save_json_file(record, filename_full)
            
            # vehicle_position = {
            #     'time': record['timestamp'],
            #     'x': record['x'],
            # }
            # producer.send(TOPIC, vehicle_position)
            # print(f"Sent data2: {vehicle_position}")
            
            # # Save simplified vehicle position as JSON file
            # filename_position = os.path.join(JSON_DIR, f"{record['name']}_{record['t']}.json")
            # save_json_file(vehicle_position, filename_position)

        time.sleep(interval)

if __name__ == '__main__':
    # File path to the CSV file
    file_path = '/Users/twokdin/Documents/GitHub/Big-Data/out/data_vehicles.csv'

    # Load vehicle data
    vehicle_data = load_vehicle_data(file_path)

    # Start time of the producer
    start_time = datetime.now()

    # Interval in seconds
    interval = 0.1  # Update this value as needed

    # Send data to Kafka periodically
    send_vehicle_data(vehicle_data, start_time, interval)
