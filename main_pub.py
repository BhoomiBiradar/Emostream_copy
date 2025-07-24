from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'processed_emoji_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='emoji_processing_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consuming messages from 'processed_emoji_data'...")
for message in consumer:
    print(f"Received message: {message.value}")
  
