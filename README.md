# Traffic-Analytics-Platform-Project
Architecture: [Kafka] → [Spark Structured Streaming] → [Spark batch processing] → [Hive] → [Airflow]
Designed an end-to-end real-time traffic analytics platform to ingest, process, clean, and analyze high-volume vehicle movement data. This system supports both streaming and batch processing to generate actionable insights using python, Kafka, Spark (Structured Streaming and Batch), HDFS, Hive, Airflow)
•	Built Kafka-based ingestion pipelines to stream real-time traffic events using Python producers.
•	Processed streaming data using Spark Structured Streaming and stored raw data in HDFS (Parquet). 
•	Developed PySpark batch pipelines for data validation, deduplication, enrichment, and aggregations. Derived city-wise congestion levels, road-level metrics, peak-hour trends, and traffic violations using window functions. 
•	Created Hive EXTERNAL tables on processed Parquet datasets for SQL-based reporting and automated the workflow using Apache Airflow DAGs, ensuring reliable orchestration and scheduling.
