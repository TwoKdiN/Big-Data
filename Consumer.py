from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'test'

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
)

# Consume messages from Kafka
print(f"Listening to Kafka topic: {TOPIC}")
for message in consumer:
    print(message.value.decode("utf-8"))