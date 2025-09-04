"""
Streaming verification script to test SimulationEngine integration with the API.
"""
import requests
import time
import logging
from datetime import datetime, timedelta

from ..world import GridWorld
from ..models import BehavioralProfile
from ..core.simulation_engine import SimulationEngine
from ..core.stream_handler import DataStreamHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_api_endpoints(api_base_url: str = "http://localhost:8000"):
    """Test all relevant API endpoints before running simulation."""
    
    print("Testing API Endpoints")
    print("=" * 50)
    
    endpoints_to_test = [
        ("Health Check", f"{api_base_url}/health"),
        ("API Docs", f"{api_base_url}/docs"),
        ("Ingestion Stats", f"{api_base_url}/api/v1/ingest/stats"),
        ("Ingestion Health", f"{api_base_url}/api/v1/ingest/health"),
    ]
    
    for name, url in endpoints_to_test:
        try:
            if name == "API Docs":
                # Just check if docs endpoint returns something
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    print(f"✓ {name}: OK")
                else:
                    print(f"~ {name}: Available but returned {response.status_code}")
                continue
                
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"✓ {name}: OK")
                if name == "Ingestion Stats":
                    stats = response.json()
                    print(f"  Current readings: {stats.get('total_readings', 0)}")
                elif name == "Ingestion Health":
                    health = response.json()
                    print(f"  Status: {health.get('status', 'unknown')}")
            else:
                print(f"✗ {name}: Failed ({response.status_code})")
                if name == "Ingestion Health":
                    print(f"  Note: Make sure you updated src/api/ingestion.py with the new version")
        except Exception as e:
            print(f"✗ {name}: Error - {e}")
            if name == "Ingestion Health":
                print(f"  Note: This endpoint may not exist in the old version")


def test_data_stream_handler():
    """Test DataStreamHandler independently."""
    
    print("\nTesting DataStreamHandler")
    print("=" * 50)
    
    api_endpoint = "http://localhost:8000/api/v1"
    
    with DataStreamHandler(api_endpoint) as handler:
        
        # Test connection
        if handler.test_connection():
            print("✓ Connection test passed")
        else:
            print("✗ Connection test failed")
            return False
        
        # Create sample telemetry data
        from ..models import TelemetryReading
        
        sample_readings = [
            TelemetryReading(
                vehicle_id="TEST-STREAM-001",
                timestamp=datetime.now().isoformat() + "Z",
                latitude=22.5700,
                longitude=88.3600,
                speed_kph=45.0,
                fuel_percentage=75.5
            ),
            TelemetryReading(
                vehicle_id="TEST-STREAM-002",
                timestamp=(datetime.now() + timedelta(seconds=30)).isoformat() + "Z",
                latitude=22.5710,
                longitude=88.3610,
                speed_kph=0.0,
                fuel_percentage=68.2
            )
        ]
        
        # Test streaming
        result = handler.stream_telemetry_batch(sample_readings)
        
        if result.get("status") == "success":
            print(f"✓ Streaming test passed: {result.get('processed_count', 0)} readings processed")
        else:
            print(f"✗ Streaming test failed: {result}")
            return False
        
        # Show statistics
        stats = handler.get_statistics()
        print(f"  Statistics: {stats['total_readings_sent']} readings sent, "
              f"{stats['success_rate_percent']}% success rate")
        
        return True


def test_simulation_engine_quick():
    """Run a quick simulation to verify end-to-end functionality."""
    
    print("\nTesting SimulationEngine (Quick Run)")
    print("=" * 50)
    
    try:
        # Create world and engine
        world = GridWorld(width=51, height=51, num_refueling_stations=25, verbose=False)
        engine = SimulationEngine(world)
        
        # Create small fleet
        profiles = {
            'test_standard': BehavioralProfile(p_stop_at_node=0.05, p_theft_given_stop=0.10),
            'test_active': BehavioralProfile(p_stop_at_node=0.15, p_theft_given_stop=0.30)
        }
        
        engine.create_vehicle_fleet(num_vehicles=3, profiles=profiles)
        print(f"✓ Created fleet: {len(engine.vehicles)} vehicles")
        
        # Run short simulation
        api_endpoint = "http://localhost:8000/api/v1"
        
        print("Starting 2-minute simulation...")
        telemetry_df, anomaly_df = engine.run_simulation(
            duration_minutes=2,
            api_endpoint=api_endpoint,
            time_step_seconds=30,
            time_scale_factor=10,  # 10x speed: 1 real second = 10 simulated seconds
            batch_size=10
        )
        
        print(f"✓ Simulation completed")
        print(f"  Telemetry records: {len(telemetry_df)}")
        print(f"  Anomaly events: {len(anomaly_df)}")
        
        if len(anomaly_df) > 0:
            print(f"  Anomaly types: {anomaly_df['event_type'].value_counts().to_dict()}")
        
        return True
        
    except Exception as e:
        print(f"✗ Simulation test failed: {e}")
        return False


def verify_api_received_data():
    """Verify the API actually received and stored the data."""
    
    print("\nVerifying API Data Reception")
    print("=" * 50)
    
    try:
        response = requests.get("http://localhost:8000/api/v1/ingest/stats")
        
        if response.status_code == 200:
            stats = response.json()
            
            print(f"Total readings in API: {stats.get('total_readings', 0)}")
            print(f"Unique vehicles: {stats.get('unique_vehicles', 0)}")
            print(f"Format breakdown: {stats.get('format_breakdown', {})}")
            
            if stats.get('vehicle_activity'):
                print("Vehicle activity:")
                for vid, activity in stats['vehicle_activity'].items():
                    if 'TEST-' in vid or 'V-TEST-' in vid:
                        print(f"  {vid}: {activity['count']} readings")
            
            return True
            
        else:
            print(f"✗ Failed to get API stats: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"✗ Error checking API stats: {e}")
        return False


def main():
    """Run complete verification suite."""
    
    print("SimulationEngine + API Integration Verification")
    print("=" * 70)
    print(f"Start time: {datetime.now().isoformat()}")
    print()
    
    # Step 1: Test API endpoints
    test_api_endpoints()
    
    # Step 2: Test DataStreamHandler
    if not test_data_stream_handler():
        print("\nDataStreamHandler test failed. Check API connection.")
        return
    
    # Step 3: Test SimulationEngine
    if not test_simulation_engine_quick():
        print("\nSimulationEngine test failed.")
        return
    
    # Step 4: Verify data was received
    time.sleep(2)  # Give background processing time
    verify_api_received_data()
    
    print(f"\nVerification completed at: {datetime.now().isoformat()}")
    print("\nNext steps:")
    print("1. Check API documentation: http://localhost:8000/docs")
    print("2. View detailed stats: http://localhost:8000/api/v1/ingest/stats")
    print("3. Monitor health: http://localhost:8000/api/v1/ingest/health")


if __name__ == "__main__":
    main()