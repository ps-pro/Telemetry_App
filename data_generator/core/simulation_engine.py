"""
SimulationEngine - High-level orchestrator for vehicle telemetry simulation.
Extracted from your complete data generator code.
"""
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import asdict
import pandas as pd
import random

from ..world import GridWorld
from ..vehicle import VehicleAgent, VehicleState
from ..models import BehavioralProfile, TelemetryReading, AnomalyEvent
from .stream_handler import DataStreamHandler


class SimulationEngine:
    """
    High-level orchestrator for real-time vehicle telemetry simulation.

    This class manages a fleet of vehicles in a simulated world, coordinates
    their movements and behaviors, and streams generated telemetry data to
    external systems in real-time.
    """

    def __init__(self, world: GridWorld):
        """
        Initialize the simulation engine with a world instance.

        Args:
            world: GridWorld instance representing the simulation environment
        """
        self.world = world
        self.vehicles: List[VehicleAgent] = []
        self.telemetry_log: List[TelemetryReading] = []
        self.anomaly_log: List[AnomalyEvent] = []

        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def create_vehicle_fleet(self,
                           num_vehicles: int,
                           profiles: Dict[str, BehavioralProfile]) -> None:
        """
        Create and initialize a fleet of vehicles with distributed behavioral profiles.

        Args:
            num_vehicles: Total number of vehicles to create
            profiles: Dictionary mapping profile names to BehavioralProfile instances
        """
        if num_vehicles <= 0:
            raise ValueError("Number of vehicles must be positive")

        if not profiles:
            raise ValueError("At least one behavioral profile must be provided")

        self.vehicles.clear()

        profile_names = list(profiles.keys())
        profile_instances = list(profiles.values())

        # Distribute vehicles evenly across available profiles
        for i in range(num_vehicles):
            profile_index = i % len(profiles)
            profile_name = profile_names[profile_index]
            profile = profile_instances[profile_index]

            vehicle_id = f"V-{profile_name.upper()}-{i+1:03d}"

            vehicle = VehicleAgent(
                vehicle_id=vehicle_id,
                world=self.world,
                behavioral_profile=profile
            )

            self.vehicles.append(vehicle)

        self.logger.info(
            f"Created fleet of {num_vehicles} vehicles across {len(profiles)} profiles"
        )

    def _assign_refueling_trip(self, vehicle: VehicleAgent) -> bool:
        """Assign a refueling trip to a vehicle that needs fuel."""
        try:
            # Get current position (approximate from route or assume random start)
            if vehicle.route:
                current_pos = vehicle.route[-1]
            else:
                # If no route history, pick a random starting position
                nodes = list(self.world.graph.nodes())
                current_pos = random.choice(nodes)

            # Find nearest refueling station
            station_node, route_to_station = self.world.find_nearest_refueling_station(current_pos)

            if len(route_to_station) >= 2:
                vehicle.assign_new_trip(route_to_station, speed_kph=60.0)
                self.logger.debug(
                    f"Assigned refueling trip to {vehicle.vehicle_id}: "
                    f"{len(route_to_station)-1} km to station {station_node}"
                )
                return True

        except Exception as e:
            self.logger.warning(
                f"Failed to assign refueling trip to {vehicle.vehicle_id}: {e}"
            )

        return False

    def _assign_random_trip(self, vehicle: VehicleAgent) -> bool:
        """Assign a random trip to a vehicle within fuel range."""
        # Calculate maximum safe travel distance (with 90% fuel buffer)
        max_fuel_range_km = (vehicle.fuel_liters * vehicle.mileage_kmpl) * 0.9

        # Try multiple times to find a suitable route
        max_attempts = 20

        for attempt in range(max_attempts):
            try:
                # Generate random route with reasonable minimum distance
                min_distance = min(5, int(max_fuel_range_km * 0.1))
                random_route = self.world.get_random_route(min_distance=min_distance)

                if not random_route:
                    continue

                route_distance_km = len(random_route) - 1

                # Check if route is within fuel range
                if route_distance_km <= max_fuel_range_km:
                    vehicle.assign_new_trip(random_route, speed_kph=60.0)
                    self.logger.debug(
                        f"Assigned random trip to {vehicle.vehicle_id}: "
                        f"{route_distance_km} km (fuel range: {max_fuel_range_km:.1f} km)"
                    )
                    return True

            except Exception as e:
                self.logger.warning(
                    f"Attempt {attempt+1}: Failed to assign random trip to "
                    f"{vehicle.vehicle_id}: {e}"
                )

        self.logger.warning(
            f"Could not find suitable random trip for {vehicle.vehicle_id} "
            f"within {max_attempts} attempts"
        )
        return False

    def _manage_vehicle_lifecycle(self, vehicle: VehicleAgent) -> None:
        """Manage the lifecycle of a parked vehicle by assigning new trips."""
        if vehicle.state != VehicleState.PARKED:
            return

        # Priority 1: Refueling if needed
        if vehicle.needs_refueling():
            if not self._assign_refueling_trip(vehicle):
                self.logger.warning(
                    f"Failed to assign refueling trip to {vehicle.vehicle_id}"
                )
            return

        # Priority 2: New random work assignment
        if not self._assign_random_trip(vehicle):
            self.logger.warning(
                f"Failed to assign random trip to {vehicle.vehicle_id}"
            )

    def run_simulation(self,
                      duration_minutes: int,
                      api_endpoint: str,
                      time_step_seconds: int = 60,
                      time_scale_factor: int = 60,
                      batch_size: int = 50) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Execute the main simulation loop with real-time data streaming.

        Args:
            duration_minutes: Total simulation duration in simulated minutes
            api_endpoint: URL endpoint for telemetry data streaming
            time_step_seconds: Resolution of each simulation tick in simulated seconds
            time_scale_factor: Simulated seconds per real-world second
            batch_size: Number of readings to batch before sending

        Returns:
            Tuple containing telemetry and anomaly DataFrames
        """
        # Validate parameters
        if duration_minutes <= 0:
            raise ValueError("Duration must be positive")
        if time_step_seconds <= 0:
            raise ValueError("Time step must be positive")
        if time_scale_factor <= 0:
            raise ValueError("Time scale factor must be positive")
        if not self.vehicles:
            raise ValueError("No vehicles in fleet - call create_vehicle_fleet() first")

        # Initialize data streaming
        with DataStreamHandler(api_endpoint) as stream_handler:
            
            # Test connection first
            if not stream_handler.test_connection():
                raise ConnectionError(f"Cannot connect to API at {api_endpoint}")

            # Clear previous simulation data
            self.telemetry_log.clear()
            self.anomaly_log.clear()

            # Calculate simulation parameters
            total_simulation_ticks = (duration_minutes * 60) // time_step_seconds
            real_time_per_tick = time_step_seconds / time_scale_factor

            self.logger.info(
                f"Starting simulation: {duration_minutes} min duration, "
                f"{total_simulation_ticks} ticks, "
                f"{real_time_per_tick:.2f}s real-time per tick"
            )

            # Initialize simulation time
            sim_start_time = datetime.now()
            current_sim_time = sim_start_time
            telemetry_buffer = []

            try:
                # Main simulation loop
                for tick in range(total_simulation_ticks):
                    tick_start_real_time = time.time()

                    # Process each vehicle
                    for vehicle in self.vehicles:
                        # Manage vehicle lifecycle for parked vehicles
                        self._manage_vehicle_lifecycle(vehicle)

                        # Execute vehicle simulation tick
                        try:
                            telemetry, anomaly = vehicle.tick(current_sim_time, time_step_seconds)

                            # Add to buffer
                            telemetry_buffer.append(telemetry)
                            self.telemetry_log.append(telemetry)

                            if anomaly:
                                self.anomaly_log.append(anomaly)
                                stream_handler.stream_anomaly_event(anomaly)

                        except Exception as e:
                            self.logger.error(
                                f"Error processing vehicle {vehicle.vehicle_id}: {e}"
                            )

                    # Send batch when buffer is full
                    if len(telemetry_buffer) >= batch_size:
                        result = stream_handler.stream_telemetry_batch(telemetry_buffer)
                        if result.get("status") == "success":
                            telemetry_buffer.clear()

                    # Progress logging
                    if tick % 50 == 0 or tick == total_simulation_ticks - 1:
                        progress_pct = (tick + 1) / total_simulation_ticks * 100
                        stats = stream_handler.get_statistics()
                        self.logger.info(
                            f"Progress: {progress_pct:.1f}% (Tick {tick+1}/{total_simulation_ticks}) - "
                            f"Sent: {stats['total_readings_sent']} readings, "
                            f"Success rate: {stats['success_rate_percent']}%"
                        )

                    # Advance simulation time
                    current_sim_time += timedelta(seconds=time_step_seconds)

                    # Real-time pacing
                    tick_computation_time = time.time() - tick_start_real_time
                    sleep_time = max(0, real_time_per_tick - tick_computation_time)

                    if sleep_time > 0:
                        time.sleep(sleep_time)

                # Send any remaining telemetry
                if telemetry_buffer:
                    stream_handler.stream_telemetry_batch(telemetry_buffer)

                # Final statistics
                final_stats = stream_handler.get_statistics()
                self.logger.info(f"Simulation completed: {final_stats}")

            except Exception as e:
                self.logger.error(f"Simulation failed: {e}")
                raise

        # Convert logs to DataFrames
        telemetry_df = self._create_telemetry_dataframe()
        anomaly_df = self._create_anomaly_dataframe()

        return telemetry_df, anomaly_df

    def _create_telemetry_dataframe(self) -> pd.DataFrame:
        """Convert telemetry log to a structured DataFrame."""
        if not self.telemetry_log:
            return pd.DataFrame()

        telemetry_dicts = [asdict(reading) for reading in self.telemetry_log]
        df = pd.DataFrame(telemetry_dicts)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df

    def _create_anomaly_dataframe(self) -> pd.DataFrame:
        """Convert anomaly log to a structured DataFrame."""
        if not self.anomaly_log:
            return pd.DataFrame()

        anomaly_dicts = [event._asdict() for event in self.anomaly_log]

        # Flatten the 'details' dictionary into separate columns
        flattened_data = []
        for record in anomaly_dicts:
            flat_record = {
                'vehicle_id': record['vehicle_id'],
                'timestamp': record['timestamp'],
                'event_type': record['event_type']
            }
            if 'details' in record and isinstance(record['details'], dict):
                flat_record.update(record['details'])

            flattened_data.append(flat_record)

        df = pd.DataFrame(flattened_data)
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    def get_fleet_status(self) -> Dict[str, Any]:
        """Get current status summary of the vehicle fleet."""
        if not self.vehicles:
            return {"fleet_size": 0, "message": "No vehicles in fleet"}

        # Aggregate fleet statistics
        state_counts = {}
        total_fuel = 0
        low_fuel_count = 0

        for vehicle in self.vehicles:
            # Count vehicle states
            state_name = vehicle.state.name
            state_counts[state_name] = state_counts.get(state_name, 0) + 1

            # Fuel statistics
            total_fuel += vehicle.fuel_percentage
            if vehicle.needs_refueling():
                low_fuel_count += 1

        avg_fuel = total_fuel / len(self.vehicles)

        return {
            "fleet_size": len(self.vehicles),
            "vehicle_states": state_counts,
            "fuel_statistics": {
                "average_fuel_percentage": round(avg_fuel, 2),
                "vehicles_needing_refuel": low_fuel_count
            },
            "data_statistics": {
                "total_telemetry_records": len(self.telemetry_log),
                "total_anomaly_events": len(self.anomaly_log)
            }
        }