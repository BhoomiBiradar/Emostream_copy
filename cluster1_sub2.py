from kafka import KafkaConsumer
import json

# Specify the topic to subscribe to
topic = "cluster1_topic"  # The topic this cluster's subscribers will consume from

def start_cluster_subscriber(topic):
    # Initialize Kafka consumer to listen to the cluster-specific topic
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='cluster1_subscriber_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Cluster 1 Subscriber is listening to '{topic}'...")
    
    for message in consumer:
        print(f"Subscriber received message: {message.value}")
        # Here, you could process the message further if needed

# Start the subscriber
start_cluster_subscriber(topic)

