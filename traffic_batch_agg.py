from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, concat, when, col, hour, lit
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Traffic Batch Analytics").getOrCreate()

# Read cleaned traffic data from HDFS
df = spark.read.parquet("/traffic/cleaned_output")

# Aggregations

# 1. City-Wise Congestion Distribution

city_congestion_df = df.groupBy("City", "congestion_level").agg(count("*").alias("Vehicle_Count")) \
                       .withColumn("City_Traffic_Percentage", col("Vehicle_Count")*100 / sum("Vehicle_Count").over(Window.partitionBy("City")))

# 2. Road-wise Traffic Analytics (Traffic volume, Speed analytics, Congestion indicators)

road_analytics_df = df.groupBy("City","Road_Name").agg(avg("Speed_kmh").alias("Avg_speed_kmh"),
                        count("*").alias("Total_Vehicles"),
                        sum(when(col("congestion_level") == "Congested Road", 1).otherwise(0)).alias("High_Traffic_Events"),
                        sum(when(col("congestion_level") == "Moderate Traffic", 1).otherwise(0)).alias("Moderate_Traffic_Events"),
                        sum(when(col("congestion_level") == "Free Flow Traffic", 1).otherwise(0)).alias("FreeFlow_Events"))

# 3. Overspeeding Detection (Road-based)

speed_viol_df = df.filter(col("Speed_kmh") > 60) \
                  .withColumn("speed_violation",
                               when(((col("Road_Name") == "Mount Road") & (col("Speed_kmh") > 60)) |
                                    ((col("Road_Name") == "OMR") & (col("Speed_kmh") > 70)) |
                                    ((col("Road_Name") == "ECR") & (col("Speed_kmh") > 80)) |
                                    ((col("Road_Name") == "Mysore Road") & (col("Speed_kmh") > 70)) |
                                    ((col("Road_Name") == "Hosur Road") & (col("Speed_kmh") > 60)),"Overspeed")
                               .otherwise("OK")) \
                  .select("Event_time","Vehicle_id","vehicle_type","City","Road_Name","Speed_kmh","speed_violation")

# 4. Peak-Hour Congestion Analysis

peak_hour_metrics = df.withColumn("hour", hour("Event_time")) \
                      .groupBy("hour", "Road_Name") \
                      .agg(avg("Speed_kmh").alias("Avg_speed")) \
                      .withColumn("hour_diff", when(col("hour") == 0, "12 AM")
                                              .when(col("hour") < 12, concat(col("hour"), lit(" AM")))
                                              .when(col("hour") == 12, "12 PM")
                                              .otherwise(concat(col("hour") - 12, lit(" PM")))) \
                      .withColumn("traffic_period",
                                   when(col("hour").between(8,10) | col("hour").between(17,19), "Peak Hour")
                                   .otherwise("Off Peak"))

# 5. Signal violation detection

signal_viol_df = df.withColumn("signal_violation",
                           when((col("signal_status") == "RED") & (col("Speed_kmh") > 5), "Red Light Violation")
                           .otherwise("NO")) \
                   .select("Event_time","Vehicle_id","vehicle_type","City","Road_Name","Speed_kmh","signal_status","signal_violation")


#  Write to HDFS
road_analytics_df.write.mode("overwrite").parquet("/traffic/road_traffic_analytics")
city_congestion_df.write.mode("overwrite").parquet("/traffic/city_wise_congestion")
speed_viol_df.write.mode("overwrite").parquet("/traffic/overspeed_detection")
peak_hour_metrics.write.mode("overwrite").parquet("/traffic/peak_hour_congestion")
signal_viol_df.write.mode("overwrite").parquet("/traffic/signal_violation_detect")