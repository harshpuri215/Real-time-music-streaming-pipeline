# Real-Time Music Streaming Data Pipeline with Snowflake Analytics

## Project Overview
This project builds a scalable, fault-tolerant real-time data pipeline simulating music streaming events. It integrates Apache Kafka for streaming ingestion, Apache Spark Structured Streaming for processing, Google Cloud Platform services for orchestration, and Snowflake as the cloud data warehouse for analytics.

## Architecture
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/beb4add2-838e-4120-916a-090dbc1fb659" />


## Technology Stack
- Apache Kafka  
- Apache Spark Structured Streaming  
- Google Cloud Pub/Sub  
- Google Cloud Functions  
- Google Cloud Storage  
- Snowflake Data Warehouse  
- Docker (optional)

## Features
- Real-time ingestion of streaming music event data  
- Processing and transformation with Spark Structured Streaming  
- Event-driven orchestration via Pub/Sub and Cloud Functions  
- Seamless ingestion of processed data into Snowflake  
- Fault tolerance with checkpointing and schema evolution handling  
- Monitoring and alerting for pipeline health

## Setup Instructions

### Prerequisites
- Kafka cluster setup (local or cloud)  
- Spark with Structured Streaming environment  
- Google Cloud SDK with Pub/Sub, Functions, and Storage enabled  
- Snowflake account with warehouse and database access  
- Docker (optional for containerization)

### Deployment Steps
1. Run the Kafka producer to simulate real-time music events:  
   `python kafka/producer.py`  
2. Submit the Spark streaming job to process events:  
   `spark-submit spark/spark_streaming_job.py`  
3. Deploy GCP Cloud Functions to handle Pub/Sub event orchestration.  
4. Create Snowflake tables and schemas using `snowflake/snowflake_schema.sql`.  
5. Run the Snowflake ingestion script to load processed data into Snowflake.  
6. Setup monitoring and alerting as per the monitoring folder configuration.

## Usage
- Sample Spark queries to validate streaming transformations  
- Example SQL queries to analyze music streaming data in Snowflake  

## Contribution Guidelines
- Fork the repo and create feature branches  
- Write clean, documented code  
- Submit pull requests for any enhancements or fixes  

]
