CREATE EXTERNAL TABLE road_traffic_analytics (
  City STRING,
  Road_Name STRING,
  Avg_speed_kmh DOUBLE,
  Total_Vehicles BIGINT,
  High_Traffic_Events BIGINT,
  Moderate_Traffic_Events BIGINT,
  FreeFlow_Events BIGINT
)
STORED AS PARQUET

LOCATION '/database/road_level_traffic_metrics';
