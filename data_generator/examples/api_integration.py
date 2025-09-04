"""
Integration module for sending data generator output to the telemetry API.
"""
import requests
import json
import time
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from ..world import GridWorld
from ..vehicle import VehicleAgent
from ..models import BehavioralProfile, TelemetryReading, AnomalyEvent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class APIClient:
    """Client for interacting with the telemetry API."""
    
    def __init__(self, base_url: str = "http://localhost:8000", timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def check_health(self) -> bool:
        """Check if the API is running and healthy."""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            if response.status_code == 200:
                logger.info("API health check passed")
                return True
            else:
                logger.warning(f"API health check failed with status: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Cannot connect to API: {e}")
            return False
    
    def send_telemetry_batch(self, readings: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Send a batch of telemetry readings to the API."""
        payload = {"readings": readings}
        
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/ingest",
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Successfully sent {result.get('processed_count', 0)} readings")
                return result
            else:
                logger.error(f"API request failed with status {response.status_code}: {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending telemetry batch: {e}")
            return None
    
    def get_ingestion_stats(self) -> Optional[Dict[str, Any]]:
        """Get current ingestion statistics from the API."""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/ingest/stats", timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Failed to get stats: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting ingestion stats: {e}")
            return None


class SimulationRunner:
    """Runs vehicle simulations and sends data to the API."""
    
    def __init__(self, api_client: APIClient, batch_size: int = 100):
        self.api_client = api_client
        self.batch_size = batch_size
        self.telemetry_buffer = []
        self.anomaly_buffer = []
    
    def convert_telemetry_reading(self, reading: TelemetryReading) -> Dict[str, Any]:
        """Convert TelemetryReading dataclass to dictionary."""
        return {
            "vehicle_id": reading.vehicle_id,
            "timestamp": reading.timestamp,
            "latitude": reading.latitude,
            "longitude": reading.longitude,
            "speed_kph": reading.speed_kph,
            "fuel_percentage": reading.fuel_percentage
        }
    
    def add_telemetry(self, reading: TelemetryReading):
        """Add telemetry reading to buffer."""
        self.telemetry_buffer.append(self.convert_telemetry_reading(reading))
        
        if len(self.telemetry_buffer) >= self.batch_size:
            self.flush_telemetry_buffer()
    
    def add_anomaly(self, anomaly: AnomalyEvent):
        """Add anomaly event to buffer."""
        self.anomaly_buffer.append({
            "vehicle_id": anomaly.vehicle_id,
            "timestamp": anomaly.timestamp,
            "event_type": anomaly.event_type,
            "details": anomaly.details
        })
        logger.warning(f"Anomaly detected: {anomaly.event_type} for vehicle {anomaly.vehicle_id}")
    
    def flush_telemetry_buffer(self):
        """Send buffered telemetry data to API."""
        if not self.telemetry_buffer:
            return
        
        result = self.api_client.send_telemetry_batch(self.telemetry_buffer)
        if result:
            self.telemetry_buffer.clear()
        else:
            logger.error("Failed to send telemetry batch, keeping in buffer for retry")
    
    def flush_all_buffers(self):
        """Send all remaining buffered data."""
        if self.telemetry_buffer:
            self.flush_telemetry_buffer()


def run_single_vehicle_simulation(
    api_client: APIClient,
    simulation_duration_minutes: int = 30,
    time_step_seconds: int = 30
) -> None:
    """Run a single vehicle simulation and send data to API."""
    
    logger.info(f"Starting single vehicle simulation for {simulation_duration_minutes} minutes")
    
    # Create world and vehicle
    world = GridWorld(width=101, height=101, num_refueling_stations=50, verbose=False)
    
    profile = BehavioralProfile(
        p_stop_at_node=0.15,
        p_theft_given_stop=0.20,
        theft_pct_min=2.0,
        theft_pct_max=10.0
    )
    
    vehicle = VehicleAgent(
        vehicle_id="SIM-V001",
        world=world,
        behavioral_profile=profile,
        mileage_kmpl=4.5
    )
    
    # Generate route
    start_node = (0, 0)
    end_node = (80, 80)
    import networkx as nx
    route = nx.shortest_path(world.graph, source=start_node, target=end_node)
    vehicle.assign_new_trip(route, speed_kph=55.0)
    
    # Setup simulation
    runner = SimulationRunner(api_client, batch_size=50)
    sim_time = datetime.now()
    max_steps = (simulation_duration_minutes * 60) // time_step_seconds
    
    logger.info(f"Simulation will run for {max_steps} steps with {time_step_seconds}s intervals")
    
    # Run simulation loop
    for step in range(max_steps):
        reading, anomaly = vehicle.tick(sim_time, time_step_seconds)
        
        # Add data to buffers
        runner.add_telemetry(reading)
        if anomaly:
            runner.add_anomaly(anomaly)
        
        # Check if vehicle completed trip
        if vehicle.state.name == "PARKED" and step > 10:
            logger.info(f"Vehicle completed trip at step {step}")
            break
        
        # Advance time
        sim_time += timedelta(seconds=time_step_seconds)
        
        # Log progress periodically
        if step % 20 == 0:
            logger.info(f"Simulation step {step}/{max_steps}, vehicle state: {vehicle.state.name}")
    
    # Send any remaining data
    runner.flush_all_buffers()
    
    # Final statistics
    stats = api_client.get_ingestion_stats()
    if stats:
        logger.info(f"Final stats: {stats}")


def run_multi_vehicle_simulation(
    api_client: APIClient,
    num_vehicles: int = 3,
    simulation_duration_minutes: int = 60,
    time_step_seconds: int = 30
) -> None:
    """Run simulation with multiple vehicles."""
    
    logger.info(f"Starting multi-vehicle simulation: {num_vehicles} vehicles, {simulation_duration_minutes} minutes")
    
    # Create world
    world = GridWorld(width=101, height=101, num_refueling_stations=100, verbose=False)
    
    # Create vehicles with different profiles
    vehicles = []
    profiles = [
        BehavioralProfile(p_stop_at_node=0.05, p_theft_given_stop=0.10),  # Conservative
        BehavioralProfile(p_stop_at_node=0.15, p_theft_given_stop=0.30),  # Moderate risk
        BehavioralProfile(p_stop_at_node=0.25, p_theft_given_stop=0.50),  # High risk
    ]
    
    for i in range(num_vehicles):
        profile = profiles[i % len(profiles)]
        vehicle = VehicleAgent(
            vehicle_id=f"SIM-V{i+1:03d}",
            world=world,
            behavioral_profile=profile,
            mileage_kmpl=4.0 + (i * 0.5)  # Different fuel efficiencies
        )
        
        # Assign different routes
        start_node = (i * 10, i * 10)
        end_node = (90 - i * 10, 90 - i * 10)
        import networkx as nx
        route = nx.shortest_path(world.graph, source=start_node, target=end_node)
        vehicle.assign_new_trip(route, speed_kph=50.0 + (i * 5))
        
        vehicles.append(vehicle)
    
    # Setup simulation
    runner = SimulationRunner(api_client, batch_size=100)
    sim_time = datetime.now()
    max_steps = (simulation_duration_minutes * 60) // time_step_seconds
    
    # Run simulation loop
    for step in range(max_steps):
        active_vehicles = 0
        
        for vehicle in vehicles:
            if vehicle.state.name != "PARKED" or step < 10:
                reading, anomaly = vehicle.tick(sim_time, time_step_seconds)
                
                runner.add_telemetry(reading)
                if anomaly:
                    runner.add_anomaly(anomaly)
                
                if vehicle.state.name != "PARKED":
                    active_vehicles += 1
        
        # Stop if all vehicles are parked
        if active_vehicles == 0 and step > 50:
            logger.info(f"All vehicles completed trips at step {step}")
            break
        
        # Advance time
        sim_time += timedelta(seconds=time_step_seconds)
        
        # Log progress
        if step % 30 == 0:
            logger.info(f"Step {step}/{max_steps}, active vehicles: {active_vehicles}")
    
    # Send remaining data
    runner.flush_all_buffers()
    
    # Final statistics
    stats = api_client.get_ingestion_stats()
    if stats:
        logger.info(f"Simulation complete. Final stats: {stats}")


def main():
    """Main function to run simulations."""
    
    # Initialize API client
    api_client = APIClient()
    
    # Check API health
    if not api_client.check_health():
        logger.error("API is not available. Please start the API server first.")
        logger.info("Run: python src/main.py")
        return
    
    # Get initial stats
    initial_stats = api_client.get_ingestion_stats()
    if initial_stats:
        logger.info(f"Initial API stats: {initial_stats}")
    
    print("Choose simulation type:")
    print("1. Single vehicle simulation (30 minutes)")
    print("2. Multi-vehicle simulation (60 minutes)")
    print("3. Quick test (5 minutes, single vehicle)")
    
    choice = input("Enter choice (1-3): ").strip()
    
    if choice == "1":
        run_single_vehicle_simulation(api_client, simulation_duration_minutes=30)
    elif choice == "2":
        run_multi_vehicle_simulation(api_client, num_vehicles=3, simulation_duration_minutes=60)
    elif choice == "3":
        run_single_vehicle_simulation(api_client, simulation_duration_minutes=5)
    else:
        logger.error("Invalid choice")
        return
    
    # Final stats
    final_stats = api_client.get_ingestion_stats()
    if final_stats:
        logger.info(f"Final API stats: {final_stats}")


if __name__ == "__main__":
    main()