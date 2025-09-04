"""
Super simple test to verify API is working.
Run this AFTER starting the API with: python main.py
"""
import requests
import json

def test_api():
    """Test the basic API endpoints."""
    base_url = "http://localhost:8000"
    
    print("🧪 Testing Telemetry Platform API")
    print("=" * 40)
    
    # Test 1: Root endpoint
    try:
        print("1. Testing root endpoint...")
        response = requests.get(base_url)
        if response.status_code == 200:
            print("   ✅ ROOT: API is responding")
            print(f"   📄 Response: {response.json()}")
        else:
            print(f"   ❌ ROOT failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Cannot connect to API: {e}")
        print("   💡 Make sure to run: python main.py")
        return False
    
    # Test 2: Health check
    try:
        print("\n2. Testing health endpoint...")
        response = requests.get(f"{base_url}/health")
        if response.status_code == 200:
            print("   ✅ HEALTH: API is healthy")
            print(f"   📄 Response: {response.json()}")
        else:
            print(f"   ❌ HEALTH failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Health check failed: {e}")
    
    # Test 3: Simple data ingestion
    try:
        print("\n3. Testing data ingestion...")
        test_data = {
            "readings": [
                {
                    "vehicle_id": "TEST-V1",
                    "timestamp": "2025-09-04T12:00:00Z",
                    "latitude": 22.57,
                    "longitude": 88.36,
                    "speed_kph": 42.0,
                    "fuel_percentage": 73.1
                }
            ]
        }
        
        response = requests.post(
            f"{base_url}/api/v1/ingest",
            json=test_data,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            print("   ✅ INGEST: Data ingestion working")
            print(f"   📄 Response: {response.json()}")
        else:
            print(f"   ❌ INGEST failed: {response.status_code}")
            print(f"   📄 Error: {response.text}")
    except Exception as e:
        print(f"   ❌ Ingestion test failed: {e}")
    
    # Test 4: Check stats
    try:
        print("\n4. Testing stats endpoint...")
        response = requests.get(f"{base_url}/api/v1/ingest/stats")
        if response.status_code == 200:
            print("   ✅ STATS: Statistics working")
            print(f"   📄 Response: {response.json()}")
        else:
            print(f"   ❌ STATS failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Stats test failed: {e}")
    
    print("\n🎉 API testing complete!")
    print("💡 Next: Open http://localhost:8000/docs in your browser")
    
    return True

if __name__ == "__main__":
    test_api()