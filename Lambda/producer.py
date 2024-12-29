from confluent_kafka import Producer

# Configure the producer
conf = {
    'bootstrap.servers': 'pkc-n3603.us-central1.gcp.confluent.cloud:9092',  # Example: 'pkc-xxxxxx.us-west1.gcp.confluent.cloud:9092'
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'CLWBGYF4SIM553SP',
    'sasl.password': 'P2LUtgMSQSfy1R7w74eu8cq1EkVVFZggY+JLqFIKIqi31KCzfS9SObQOoGF96qeQ'
}

# Create the producer
producer = Producer(conf)

# Định nghĩa hàm callback
def delivery_report(err, msg):
    """Callback để xử lý phản hồi sau khi gửi tin nhắn."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic: {msg.topic()} | partition: {msg.partition()} | offset: {msg.offset()}")

def send_message(message, topic):
    try:
        producer.produce(
            topic=topic, 
            value=str(message).encode('utf-8'), 
            callback=delivery_report
        )
        producer.flush()
        print(f"Produced: {message} to Kafka topic: {topic}")
    except Exception as error:
        print(f"Error: {error}")

