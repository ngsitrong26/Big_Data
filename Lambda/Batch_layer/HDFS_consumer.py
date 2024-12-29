import ast
from confluent_kafka import Consumer, KafkaException, KafkaError
from put_data_hdfs import store_data_in_hdfs  # Assuming this is where your store_data_in_hdfs function is defined

def consume_hdfs():
    # Kafka broker configuration
    topic = 'historical_data'  # Change the topic name to match the topic you're using for website content

    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'pkc-n3603.us-central1.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'CLWBGYF4SIM553SP',
        'sasl.password': 'P2LUtgMSQSfy1R7w74eu8cq1EkVVFZggY+JLqFIKIqi31KCzfS9SObQOoGF96qeQ',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False  # We handle committing offsets manually
    }

    # Create a Kafka consumer
    consumer = Consumer(consumer_config)

    # Subscribe to the topic
    consumer.subscribe([topic])

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(1.0)  # Wait up to 1 second for a message

            if msg is None:
                # No message available within the timeout
                continue
            if msg.error():
                # Handle error
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition (no more messages)
                    continue
                else:
                    raise KafkaException(msg.error())

            # Process the message
            try:
                # Decode the message
                data = msg.value().decode('utf-8')

                # Assuming the message data is a dictionary-like structure with text and metadata
                data = ast.literal_eval(data)

                # Extract values from the data
                url = data['url']
                text = data['text']
                metadata = data['metadata']  # Make sure the metadata is included in the message

                # Store the data in HDFS
                store_data_in_hdfs(url, text, metadata)

                print(f"Processed and stored data for URL: {url}")
                consumer.commit()  # Commit offset after successful processing
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
    except KeyboardInterrupt:
        # Gracefully handle the shutdown (e.g., when Ctrl+C is pressed)
        print("Shutting down consumer...")
    finally:
        # Close the consumer when done
        consumer.close()
