# Lambda Architecture Data Processing System

This project implements a **Lambda Architecture** with three layers: **Speed Layer**, **Batch Layer**, and **Serving Layer**. It processes real-time and batch data to provide actionable insights efficiently.

## System Overview

1. **Data Collection**:
   - Data is collected from various sources (e.g., price changes, news).
   - **Kafka Topics**:
     - `Topic 1`: Real-time processing.
     - `Topic 2`: Batch processing.

2. **Speed Layer**:
   - **Real-time processing** using tools like Apache Flink or Spark Streaming.
   - Provides instant responses (e.g., REST API or WebSocket).

3. **Batch Layer**:
   - Processes data periodically using Apache Hadoop or Spark.
   - Generates analytics like trends, forecasts, and reports.
   - Stores results in distributed databases (HBase, Cassandra).

4. **Serving Layer**:
   - Combines real-time and batch data for accurate and timely insights.
   - Provides results via API or user interface.

## Technologies Used

- **Stream Processing**: Apache Flink, Apache Spark Streaming
- **Batch Processing**: Apache Hadoop, Apache Spark
- **Messaging**: Apache Kafka
- **Databases**: HBase, Cassandra, PostgreSQL
- **APIs**: REST API, WebSocket

## Usage

1. **Setup Kafka Topics**:
   ```bash
   kafka-topics --create --topic topic1 --bootstrap-server localhost:9092
   kafka-topics --create --topic topic2 --bootstrap-server localhost:9092
