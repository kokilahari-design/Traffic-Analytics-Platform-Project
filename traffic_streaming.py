from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# Create Spark Session
spark = SparkSession.builder.appName("TrafficKafkaToHDFS").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Schema
my_traffic_schema = StructType([
        StructField("Event_time", StringType()),
        StructField("Vehicle_id", StringType()),
        StructField("vehicle_type", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("Speed_kmh", IntegerType()),
        StructField("Road_Name", StringType()),
        StructField("signal_status", StringType()),
        StructField("City", StringType()),
        StructField("congestion_level", StringType())
])

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mytopic_traffic_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka JSON value
parsed_df = kafka_df.select(from_json(col("value").cast("string"), my_traffic_schema).alias("data")).select("data.*")

# Write streaming data to HDFS (Raw Zone)
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "/traffic/raw") \
    .option("checkpointLocation", "/traffic/checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
