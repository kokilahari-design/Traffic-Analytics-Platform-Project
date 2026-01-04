from pyspark.sql import SparkSession
from pyspark.sql.functions import(date_format, col, to_timestamp, current_timestamp)

# Create Spark Session
spark = SparkSession.builder.appName("TrafficBatchCleaning").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read raw traffic data from HDFS

raw_df = spark.read.parquet("/traffic/raw")

# Data Cleaning & Validation

clean_df = (raw_df.filter(
                    (col("Speed_kmh").isNotNull()) &
                    (col("Speed_kmh").between(0,180)) &
                    (col("latitude").between(-90,90)) &
                    (col("longitude").between(-180,180)) &
                    (col("vehicle_count") > 0)) \
                 .withColumn("Event_time", to_timestamp(col("Event_time"))) \
                 .filter(col("Event_time").isNotNull()) \
                 .withColumn("Processed_time", date_format(current_timestamp(), "dd-MM-yyyy HH:mm:ss")) \
                 .dropDuplicates(["Event_time", "Vehicle_id", "Road_Name"])
            )

# Write Cleaned Data

clean_df.write.mode("overwrite").parquet("/traffic/cleaned_output")

spark.stop()