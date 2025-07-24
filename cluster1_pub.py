from kafka import KafkaConsumer, KafkaProducer
import json

# Specify the input and output topics for the single cluster
input_topic = "processed_emoji_data"  # The main publisher topic
output_topic = "cluster1_topic"  # The unique topic for this cluster's subscribers

def create_cluster_publisher(input_topic, output_topic):
    # Initialize Kafka consumer to read from the main publisher topic
    consumer = KafkaConsumer(
        input_topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='emoji_processing_single_cluster',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Initialize Kafka producer to forward messages to the single cluster topic
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"Cluster 1 Publisher is listening to '{input_topic}' and publishing to '{output_topic}'...")
    
    for message in consumer:
        print(f"Cluster 1 received message: {message.value}")
        
        # Forward message to the single cluster-specific topic
        producer.send(output_topic, value=message.value)
        print(f"Cluster 1 forwarded message to '{output_topic}'")

# Start the single cluster publisher
create_cluster_publisher(input_topic, output_topic)

