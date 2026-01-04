CREATE EXTERNAL TABLE road_traffic_analytics (
  City STRING,
  Road_Name STRING,
  Avg_speed_kmh DOUBLE,
  Total_Vehicles INT,
  High_Traffic_Events INT,
  Moderate_Traffic_Events INT,
  FreeFlow_Events INT
)
STORED AS PARQUET
LOCATION '/database/road_level_traffic_metrics';