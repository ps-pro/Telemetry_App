# debug_payload_format.py
import requests
import json
from datetime import datetime

# Test the exact same payload format as DataStreamHandler
test_payload = {
    "timestamp": datetime.now().isoformat() + "Z",
    "batch_size": 2,
    "telemetry_data": [
        {
            "vehicle_id": "DEBUG-V001",
            "timestamp": datetime.now().isoformat() + "Z",
            "latitude": 22.5700,
            "longitude": 88.3600,
            "speed_kph": 45.0,
            "fuel_percentage": 75.5
        },
        {
            "vehicle_id": "DEBUG-V002", 
            "timestamp": datetime.now().isoformat() + "Z",
            "latitude": 22.5710,
            "longitude": 88.3610,
            "speed_kph": 0.0,
            "fuel_percentage": 68.2
        }
    ]
}

print("Sending payload:")
print(json.dumps(test_payload, indent=2))

response = requests.post(
    "http://localhost:8000/api/v1/ingest",
    json=test_payload,
    headers={"Content-Type": "application/json"}
)

print(f"\nResponse status: {response.status_code}")
print(f"Response: {response.json()}")