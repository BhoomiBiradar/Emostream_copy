from flask import Flask, request, jsonify
from confluent_kafka import Producer
import json
import random
import threading
import time
from datetime import datetime
import requests

app = Flask(__name__)

# Kafka producer configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9092', # Change as per your Kafka server
}
producer = Producer(kafka_conf)
topic_name = 'emoji_data'

# Predefined list of valid emoji types
valid_emoji_types = ['happy', 'crying', 'laughing', 'surprised', 'sad', 'angry', 'scared']

def delivery_report(err, msg):
    """Delivery callback for Kafka messages"""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@app.route('/emoji', methods=['POST'])
def handle_emoji():
    data = request.json

    # Check if required fields (user_id, emoji_type, and timestamp) are present
    if not all(k in data for k in ('user_id', 'emoji_type', 'timestamp')):
        return jsonify({'error': 'Missing data fields'}), 400

    # Validate the emoji_type
    emoji_type = data.get('emoji_type')
    if emoji_type not in valid_emoji_types:
        return jsonify({'error': f"Invalid emoji_type. Valid options are {', '.join(valid_emoji_types)}."}), 400

    # Prepare the message as JSON
    message = json.dumps(data)

    # Send to Kafka asynchronously
    producer.produce(topic_name, value=message, callback=delivery_report)
    # Non-blocking flush in a separate thread
    threading.Thread(target=producer.flush).start()

    return jsonify({'status': 'Data received and sent to Kafka', 'emoji_type': emoji_type}), 200

def send_data(client_id):
    """Function to generate and send emoji data to the Flask endpoint"""
    url = 'http://localhost:5000/emoji'
    while True:
        user_id = random.randint(1, 1000)
        emoji_type = random.choice(valid_emoji_types)
        timestamp = datetime.now().isoformat()

        data = {
            "user_id": user_id,
            "emoji_type": emoji_type,
            "timestamp": timestamp
        }

        try:
            response = requests.post(url, json=data)
            if response.status_code == 200:
                print(f"Client {client_id}: Data sent successfully: {data}")
            else:
                print(f"Client {client_id}: Failed to send data. Status code: {response.status_code}, Response: {response.text}")
        except Exception as e:
            print(f"Client {client_id}: Error occurred: {e}")

        # Adjust sleep time to control the data generation rate
        time.sleep(random.uniform(0.01, 0.02)) # 10ms to 20ms delay

def start_client_threads(num_threads=10):
    """Function to spawn multiple threads for concurrent data generation"""
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=send_data, args=(i,))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    # Keep the main thread alive to allow background threads to run
    while True:
        time.sleep(1)

# Run send_data in multiple background threads
if __name__ == '__main__':
    threading.Thread(target=start_client_threads, args=(20,), daemon=True).start() # 20 threads for 200+ requests/sec
    app.run(port=5000)
