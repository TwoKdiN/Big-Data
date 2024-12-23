from kafka import KafkaConsumer
import json

# Kafka 
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'vehicle_positions'

# Δημιουργία Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  
)

# Κατανάλωση μηνυμάτων από Kafka
print(f"Listening to Kafka topic: {TOPIC}")
for message in consumer:
    print(f"Json File: {json.dumps(message.value, indent=2)}")  
