import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

VEHICLE_TYPES = ["CAR", "BUS", "TRUCK", "BIKE"]
VEHICLE_SPEED_FACTOR = {"CAR": 1.0, "BUS": 0.7, "TRUCK": 0.6, "BIKE": 1.1}
signal = ["GREEN", "YELLOW", "RED"]
Roads = {"NH45": "Mount Road", "NH44": "Hosur Road", "SH49": "ECR", "SH49A": "OMR", "NH275": "Mysore Road"}
ROAD_GEO_MAP = {
    "Mount Road": {"city": "Chennai", "lat": (13.04, 13.07), "lon": (80.24, 80.27), "speed_limit": 60},
    "ECR": {"city": "Chennai", "lat": (12.90, 12.98), "lon": (80.25, 80.28), "speed_limit": 80},
    "OMR": {"city": "Chennai", "lat": (12.88, 12.97), "lon": (80.21, 80.24), "speed_limit": 70},
    "Hosur Road": {"city": "Bangalore", "lat": (12.88, 12.95), "lon": (77.59, 77.65), "speed_limit": 60},
    "Mysore Road": {"city": "Bangalore", "lat": (12.93, 12.99), "lon": (77.46, 77.52), "speed_limit": 70}}

producer = KafkaProducer(bootstrap_servers = 'localhost:9092', value_serializer = lambda v: json.dumps(v).encode('utf-8'))

while True:
    road_id = random.choice(list(Roads.keys()))
    road_name = Roads[road_id]

    CITY_GEO = ROAD_GEO_MAP[road_name]
    city_name = CITY_GEO["city"]
    lat_range = CITY_GEO["lat"]
    lon_range = CITY_GEO["lon"]

    vehicle_type = random.choice(VEHICLE_TYPES)
    hour = datetime.now().hour
    peak_factor = 0.6 if (8 <= hour <= 10 or 17 <= hour <= 19) else 1.0
    base_speed = CITY_GEO["speed_limit"] * VEHICLE_SPEED_FACTOR[vehicle_type] * peak_factor

    if signal == "RED":
        speed = random.randint(0, 5)
    elif signal == "YELLOW":
        speed = random.randint(5, int(base_speed * 0.5))
    else:
        speed = random.randint(int(base_speed * 0.6), int(base_speed))
    event = {
        "Event_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "Vehicle_id": f"TN{ random.randint(10,99)}{''.join(random.sample(['A','B','C','D'], k=2))}{random.randint(1000,9999)}",
        "vehicle_type": vehicle_type,
        "latitude": round(random.uniform(*lat_range),6),
        "longitude": round(random.uniform(*lon_range),6),
        "Speed_kmh": speed,
        "Road_Name": road_name,
        "signal_status": random.choice(signal),
        "City": city_name,
        "congestion_level": "Congested Road" if speed < 30 else"Moderate Traffic" if speed < 60 else "Free Flow Traffic"
    }
    producer.send("mytopic_traffic_events", event)
    print(f"Sent event: {event}")
    time.sleep(1)  # Pause for a second before sending the next event