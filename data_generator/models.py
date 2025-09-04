# All the imports at the top
import math
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Dict, Any, List, Tuple, Optional, NamedTuple
from dataclasses import dataclass
import random

class VehicleState(Enum):
    DRIVING = auto()
    IDLING = auto()
    PARKED = auto()
    REFUELING = auto()

@dataclass
class BehavioralProfile:
    p_stop_at_node: float = 0.10
    p_theft_given_stop: float = 0.05
    # NEW: Define the range of theft
    theft_pct_min: float = 1.0
    theft_pct_max: float = 15.0


@dataclass
class TelemetryReading:
    vehicle_id: str
    timestamp: str
    latitude: float
    longitude: float
    speed_kph: float
    fuel_percentage: float

class AnomalyEvent(NamedTuple):
    vehicle_id: str
    timestamp: str
    event_type: str
    details: Dict[str, Any]