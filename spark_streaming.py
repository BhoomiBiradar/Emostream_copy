from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, floor
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from confluent_kafka import Producer
import json

# Kafka producer configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9092'  # Adjust as per your Kafka server configuration
}
producer = Producer(kafka_conf)
output_topic = 'processed_emoji_data'

def delivery_report(err, msg):
    """Callback function to confirm message delivery"""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_to_kafka(df, epoch_id):
    """Function to send data to Kafka and print to console"""
    records = df.collect()  # Collect records from the micro-batch
    for record in records:
        message = {
            "emoji_type": record["emoji_type"],
            "scaled_emoji_count": record["scaled_emoji_count"]
        }
        json_message = json.dumps(message)
       
        # Send message to Kafka
        producer.produce(output_topic, value=json_message, callback=delivery_report)
        print(f"Sent to Kafka: {json_message}")  # Print to console

    # Flush messages to ensure delivery
    producer.flush()

# Define the schema for incoming data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Create a Spark session
spark = SparkSession.builder \
    .appName("EmojiStreamProcessor") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Set Spark log level to ERROR to minimize output noise
spark.sparkContext.setLogLevel("ERROR")

# Read the stream from Kafka
emoji_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emoji_data") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the value from Kafka (assumed to be JSON)
emoji_data = emoji_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Apply windowing and count emoji types over fixed time windows (e.g., 10 seconds)
emoji_windowed_counts = emoji_data \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("emoji_type")
    ) \
    .agg(count("*").alias("emoji_count"))

# Apply the scaling logic to count (scale down to 1 if count >= 10)
scaled_counts = emoji_windowed_counts.select(
    col("emoji_type"),
    floor(col("emoji_count") / 10).alias("scaled_emoji_count")  # Scale the count down if >= 10
)

# Filter out emoji types with zero counts
scaled_counts = scaled_counts.filter(col("scaled_emoji_count") > 0)

# Output the processed data to Kafka using foreachBatch
query = scaled_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(send_to_kafka) \
    .start()

# Wait for the termination of the query
query.awaitTermination()
