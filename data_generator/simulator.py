# Main simulation orchestrator
from .world import GridWorld
from .vehicle import VehicleAgent
from .models import BehavioralProfile, VehicleState, TelemetryReading, AnomalyEvent
import pprint
from datetime import datetime, timedelta

import networkx as nx

def run_simulation():
    # --- 1. SETUP: Create a suitable world and a vehicle ---
    print("--- 1. SETUP ---")
    # A 101x101 world gives us nodes from (0,0) to (100,100)
    world = GridWorld(width=101, height=101, num_refueling_stations=50, verbose=True)

    # Create an "Opportunistic Thief" profile for an interesting journey
    profile_opportunist = BehavioralProfile(
        p_stop_at_node=0.15,      # 15% chance to stop at any intermediate node
        p_theft_given_stop=0.40   # If it stops, 40% chance of theft
    )

    # Instantiate the NEW, corrected agent
    vehicle = VehicleAgent(
        "V-LONGHAUL-01",
        world,
        profile_opportunist,
        mileage_kmpl=5.0
    )
    print("Setup complete.\n")


    # --- 2. PLAN THE TRIP ---
    print("--- 2. PLANNING TRIP from (0,0) to (60,60) ---")
    start_node = (0, 0)
    end_node = (60, 60)
    main_route = nx.shortest_path(world.graph, source=start_node, target=end_node)
    vehicle.assign_new_trip(main_route, speed_kph=60.0)
    print(f"Total trip distance: {len(main_route) - 1} km.\n")


    # --- 3. RUN THE SIMULATION (ROBUST LOGIC) ---
    print("--- 3. STARTING SIMULATION ---")
    telemetry_data = []
    anomaly_log = []

    sim_time = datetime.now()
    time_step_seconds = 30 #  30 second resolution

    # Run until the vehicle is PARKED and has a reason to stop (e.g., end of trip)
    # Max steps to prevent any unforeseen infinite loops
    for step in range(1000):
        # --- A) Advance the simulation by one time step ---
        reading, anomaly = vehicle.tick(sim_time, time_step_seconds)
        telemetry_data.append(reading)
        if anomaly:
            anomaly_log.append(anomaly)

        # --- B) Check for end-of-trip conditions ---
        if vehicle.state == VehicleState.PARKED:
            print(f"\n--- Trip Complete at {sim_time.isoformat()} ---")
            print("Vehicle has reached its final destination and parked.")
            break

        # --- C) Advance time ---
        sim_time += timedelta(seconds=time_step_seconds)

        if step == 999999:
            print("\n--- Max simulation steps reached. Ending simulation. ---")

    print("\n--- SIMULATION COMPLETE ---")

    # --- 4. DISPLAY RESULTS ---
    print("\n--- Full Telemetry Log ---")
    pprint.pprint([reading.__dict__ for reading in telemetry_data])

    print("\n\n--- Ground-Truth Anomaly Log ---")
    if anomaly_log:
        pprint.pprint([event._asdict() for event in anomaly_log])
    else:
        print("No anomaly events were generated during this trip.")

    print("\n--- Final Vehicle State ---")
    pprint.pprint(vehicle.get_debug_info())
    
if __name__ == "__main__":
    run_simulation()