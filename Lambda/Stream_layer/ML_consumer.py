import json
import logging
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from Stream_layer.insert_data_hbase import insert_data_hbase

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transformation(data):
    """
    Perform transformation on the incoming data using pandas.
    Example transformation: convert JSON data to a DataFrame and apply processing.
    """
    try:
        # Assuming the incoming data is JSON that can be converted to a pandas DataFrame
        df = pd.read_json(data)

        # Example transformation: Convert column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        
        # You can add other transformations here as needed (e.g., data cleaning, filtering)
        transformed_data = df.to_dict(orient='records')  # Convert back to list of dicts (suitable for HBase insertion)
        
        return transformed_data
    
    except Exception as e:
        logger.error(f"Error in transformation: {e}")
        return None

def consume():
    # Kafka broker configuration
    topic = 'content-data'

    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'pkc-n3603.us-central1.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'CLWBGYF4SIM553SP',
        'sasl.password': 'P2LUtgMSQSfy1R7w74eu8cq1EkVVFZggY+JLqFIKIqi31KCzfS9SObQOoGF96qeQ',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False  # Manually commit offsets
    }

    # Create a Kafka consumer
    consumer = Consumer(consumer_config)
    
    # Subscribe to the topic
    consumer.subscribe([topic])

    try:
        while True:
            # Poll for new messages
            msg = consumer.poll(timeout=1.0)  # Timeout in seconds
            
            if msg is None:
                # No message available, continue polling
                continue
            elif msg.error():
                # Handle error in message
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('End of partition reached: %s', msg)
                else:
                    logger.error('Error: %s', msg.error())
            else:
                # Process valid message
                try:
                    data = msg.value().decode('utf-8')  # Decode message value
                    logger.info('Received message: %s', data)
                    
                    # Apply transformation (using pandas now)
                    logger.info('Before transformation')
                    transformed_data = transformation(data)
                    
                    if transformed_data:
                        logger.info('Transformed result: %s', transformed_data)
                        
                        # Insert data into HBase
                        insert_data_hbase(transformed_data)
                        logger.info("Data inserted into HBase successfully.")
                    
                    # Commit the offset after processing
                    consumer.commit(message=msg)
                    logger.info("Offset committed successfully.")
                    
                    logger.info("-------------------")

                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
    
    except KeyboardInterrupt:
        logger.info("Shutting down consumer gracefully...")
    
    finally:
        # Close the Kafka consumer
        consumer.close()
        logger.info("Kafka consumer closed.")
