# Imports needed for VehicleAgent
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
import random

# Import from your local models
from .models import VehicleState, BehavioralProfile, TelemetryReading, AnomalyEvent


class VehicleAgent:
    EDGE_LENGTH_KM: float = 1.0
    IDLE_CONSUMPTION_LPH: float = 0.8
    DEFAULT_SPEED_KPH: float = 60.0
    POSITION_TOLERANCE: float = 0.99999  # Threshold for considering arrival at node

    def __init__(self,
                 vehicle_id: str,
                 world, # GridWorld type hint omitted to avoid import issues
                 behavioral_profile: BehavioralProfile,
                 tank_capacity_liters: float = 500.0,
                 mileage_kmpl: float = 4.0,
                 refueling_threshold_pct: float = 20.0):

        self.vehicle_id = vehicle_id
        self.world = world
        self.behavioral_profile = behavioral_profile
        self.tank_capacity_liters = tank_capacity_liters
        self.mileage_kmpl = mileage_kmpl
        self.fuel_liters = self.tank_capacity_liters
        self.refueling_threshold = refueling_threshold_pct

        # State management
        self.state: VehicleState = VehicleState.PARKED
        self.speed_kph: float = 0.0
        self._state_timer_seconds: int = 0

        # Route and position tracking
        self.route: List[Tuple[float, float]] = []  # List of (lon, lat) coordinates
        self.current_edge_index: int = 0
        self.progress_on_edge: float = 0.0

        # Physics consistency tracking
        self._last_update_time: Optional[datetime] = None

    @property
    def fuel_percentage(self) -> float:
        """Calculate current fuel level as percentage of tank capacity."""
        return (self.fuel_liters / self.tank_capacity_liters) * 100.0

    def needs_refueling(self) -> bool:
        """Check if vehicle needs refueling based on threshold."""
        return self.fuel_percentage <= self.refueling_threshold

    def assign_new_trip(self, route: List[Tuple[float, float]], speed_kph: float = 60.0):
        """Assign a new route to the vehicle and start the trip."""
        if len(route) < 2:
            self.state = VehicleState.PARKED
            self.speed_kph = 0.0
            self.route = []
            return

        self.route = route.copy()
        self.current_edge_index = 0
        self.progress_on_edge = 0.0
        self.speed_kph = speed_kph
        self.state = VehicleState.DRIVING
        self._state_timer_seconds = 0

        print(f"Vehicle {self.vehicle_id} starting new trip of {len(route)-1} edges.")

    def refuel(self):
        """Refuel the vehicle to full capacity."""
        print(f"Vehicle {self.vehicle_id} is refueling.")
        self.fuel_liters = self.tank_capacity_liters
        self.state = VehicleState.PARKED
        self.speed_kph = 0.0

    def tick(self, timestamp: datetime, delta_time_seconds: int) -> Tuple[TelemetryReading, Optional[AnomalyEvent]]:
        """
        Main simulation tick - updates vehicle state and generates telemetry.

        CORRECTED PHYSICS ORDER:
        1. Handle state transitions from previous tick's effects
        2. Update timed states (idle countdown)
        3. Apply physics movement for current tick
        4. Check for arrivals and plan next state
        5. Generate telemetry from final state
        """
        anomaly_event: Optional[AnomalyEvent] = None

        # Validate inputs
        if delta_time_seconds <= 0:
            raise ValueError("delta_time_seconds must be positive")

        # Step 1: Update timed states (idle countdown)
        if self.state == VehicleState.IDLING and self._state_timer_seconds > 0:
            self._state_timer_seconds = max(0, self._state_timer_seconds - delta_time_seconds)
            if self._state_timer_seconds <= 0:
                self._resume_driving()

        # Step 2: Apply physics for current tick based on current state
        distance_traveled_km = 0.0
        if self.state == VehicleState.DRIVING:
            distance_traveled_km = self._apply_driving_physics(delta_time_seconds)
        elif self.state in [VehicleState.IDLING, VehicleState.PARKED]:
            self._apply_stationary_physics()

        # Step 3: Update fuel consumption based on movement and state
        self._update_fuel_consumption(distance_traveled_km, delta_time_seconds)

        # Step 4: Check for node arrivals and handle state transitions
        if self.state == VehicleState.DRIVING:
            anomaly_event = self._check_and_handle_arrivals(timestamp)

        # Step 5: Generate telemetry from final, consistent state
        telemetry = self._create_telemetry_reading(timestamp)
        self._last_update_time = timestamp

        return telemetry, anomaly_event

    def _apply_driving_physics(self, delta_time_seconds: int) -> float:
        """Apply movement physics when vehicle is driving."""
        if not self._has_active_route():
            self._stop_vehicle("No active route")
            return 0.0

        # Calculate distance to travel this tick
        hours = delta_time_seconds / 3600.0
        distance_km = self.speed_kph * hours

        # Update position along current edge
        progress_gained = distance_km / self.EDGE_LENGTH_KM
        self.progress_on_edge += progress_gained

        # Clamp progress to prevent overshooting
        self.progress_on_edge = min(self.progress_on_edge, 1.0)

        return distance_km

    def _apply_stationary_physics(self):
        """Apply physics when vehicle is not moving."""
        self.speed_kph = 0.0

    def _update_fuel_consumption(self, distance_km: float, delta_time_seconds: int):
        """Update fuel consumption based on state and movement."""
        liters_consumed = 0.0

        if self.state == VehicleState.DRIVING and distance_km > 0:
            # Fuel consumption based on distance traveled
            liters_consumed = distance_km / self.mileage_kmpl
        elif self.state == VehicleState.IDLING:
            # Idle fuel consumption based on time
            hours = delta_time_seconds / 3600.0
            liters_consumed = self.IDLE_CONSUMPTION_LPH * hours
        # No fuel consumption when PARKED or REFUELING

        self.fuel_liters = max(0.0, self.fuel_liters - liters_consumed)

    def _check_and_handle_arrivals(self, timestamp: datetime) -> Optional[AnomalyEvent]:
        """Check if vehicle has arrived at a node and handle the arrival."""
        if not self._has_reached_node():
            return None

        # Vehicle has arrived at a node
        if self._is_at_final_destination():
            self._complete_trip()
            return None
        else:
            return self._handle_intermediate_arrival(timestamp)

    def _handle_intermediate_arrival(self, timestamp: datetime) -> Optional[AnomalyEvent]:
        """Handle arrival at an intermediate node."""
        # CRITICAL FIX: Always advance to next edge first
        self._advance_to_next_edge()

        # Then decide whether to stop or continue
        if random.random() < self.behavioral_profile.p_stop_at_node:
            return self._initiate_stop(timestamp)

        # Continue driving without stopping
        return None

    def _advance_to_next_edge(self):
        """Advance vehicle to the next edge in the route."""
        self.current_edge_index += 1
        self.progress_on_edge = 0.0

    def _initiate_stop(self, timestamp: datetime) -> Optional[AnomalyEvent]:
        """Stop the vehicle for idling at current location."""
        self.state = VehicleState.IDLING
        self.speed_kph = 0.0
        self._state_timer_seconds = random.randint(120, 600)  # 2-10 minutes

        print(f"Vehicle {self.vehicle_id} stopping for {self._state_timer_seconds}s at edge {self.current_edge_index}")

        # Check for fuel theft during stop
        if random.random() < self.behavioral_profile.p_theft_given_stop:
            return self._perform_fuel_theft(timestamp)

        return None

    def _resume_driving(self):
        """Resume driving after idle period ends."""
        self.state = VehicleState.DRIVING
        self.speed_kph = self.DEFAULT_SPEED_KPH
        print(f"Vehicle {self.vehicle_id} resuming driving")

    def _complete_trip(self):
        """Complete the current trip and park the vehicle."""
        self.state = VehicleState.PARKED
        self.speed_kph = 0.0
        print(f"Vehicle {self.vehicle_id} completed trip and parked")

    def _stop_vehicle(self, reason: str):
        """Stop vehicle for various reasons."""
        self.state = VehicleState.PARKED
        self.speed_kph = 0.0
        print(f"Vehicle {self.vehicle_id} stopped: {reason}")

    def _has_active_route(self) -> bool:
        """Check if vehicle has an active route."""
        return (len(self.route) >= 2 and
                self.current_edge_index < len(self.route) - 1)

    def _has_reached_node(self) -> bool:
        """Check if vehicle has reached the end of current edge."""
        return self.progress_on_edge >= self.POSITION_TOLERANCE

    def _is_at_final_destination(self) -> bool:
        """Check if vehicle is at the final destination."""
        return self.current_edge_index >= len(self.route) - 2

    def _perform_fuel_theft(self, timestamp: datetime) -> AnomalyEvent:
        """Simulate fuel theft incident."""
        theft_pct = random.uniform(self.behavioral_profile.theft_pct_min, self.behavioral_profile.theft_pct_max)
        liters_stolen = (theft_pct / 100.0) * self.tank_capacity_liters
        fuel_before = self.fuel_percentage

        self.fuel_liters = max(0.0, self.fuel_liters - liters_stolen)
        fuel_after = self.fuel_percentage

        print(f"!!! ANOMALY !!! Vehicle {self.vehicle_id} fuel theft of {liters_stolen:.2f}L.")

        return AnomalyEvent(
            vehicle_id=self.vehicle_id,
            timestamp=timestamp.isoformat() + "Z",
            event_type="FUEL_THEFT",
            details={
                "liters_stolen": round(liters_stolen, 2),
                "fuel_pct_before": round(fuel_before, 2),
                "fuel_pct_after": round(fuel_after, 2),
                "theft_percentage": round(theft_pct, 2)
            }
        )

    def _calculate_coordinates(self) -> Tuple[float, float]:
        """Calculate current GPS coordinates based on route position."""
        if not self.route or self.current_edge_index >= len(self.route) - 1:
            # At final destination or no route
            if self.route:
                return self.route[-1]  # Return final coordinates
            return (0.0, 0.0)

        # Interpolate between current and next node
        start_node = self.route[self.current_edge_index]
        end_node = self.route[self.current_edge_index + 1]

        lon0, lat0 = start_node
        lon1, lat1 = end_node

        # Linear interpolation based on progress
        progress = min(self.progress_on_edge, 1.0)  # Ensure valid progress
        lon = lon0 + progress * (lon1 - lon0)
        lat = lat0 + progress * (lat1 - lat0)

        return lat, lon

    def _create_telemetry_reading(self, timestamp: datetime) -> TelemetryReading:
        """Generate telemetry reading from current vehicle state."""
        lat, lon = self._calculate_coordinates()

        return TelemetryReading(
            vehicle_id=self.vehicle_id,
            timestamp=timestamp.isoformat() + "Z",
            latitude=round(lat, 6),  # Increased precision for GPS coordinates
            longitude=round(lon, 6),
            speed_kph=round(self.speed_kph, 2),
            fuel_percentage=round(self.fuel_percentage, 2)
        )

    def get_debug_info(self) -> Dict[str, Any]:
        """Get detailed debug information about vehicle state."""
        return {
            "vehicle_id": self.vehicle_id,
            "state": self.state.name,
            "current_edge_index": self.current_edge_index,
            "progress_on_edge": round(self.progress_on_edge, 4),
            "route_length": len(self.route),
            "speed_kph": self.speed_kph,
            "fuel_liters": round(self.fuel_liters, 2),
            "fuel_percentage": round(self.fuel_percentage, 2),
            "state_timer_seconds": self._state_timer_seconds,
            "coordinates": self._calculate_coordinates()
        }