"""
Updated telemetry data models to handle both legacy and SimulationEngine formats.
"""
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, validator
from enum import Enum


class TelemetryReading(BaseModel):
    """Vehicle telemetry reading - matches your data generator output exactly."""
    vehicle_id: str = Field(..., description="Unique vehicle identifier")
    timestamp: str = Field(..., description="ISO timestamp with Z suffix")
    latitude: float = Field(..., ge=-90, le=90, description="GPS latitude")
    longitude: float = Field(..., ge=-180, le=180, description="GPS longitude")
    speed_kph: float = Field(..., ge=0, description="Speed in km/h")
    fuel_percentage: float = Field(..., ge=0, le=100, description="Fuel level %")

    @validator('timestamp')
    def validate_timestamp(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError("Invalid timestamp format. Expected ISO format with Z suffix")

    @property
    def parsed_timestamp(self) -> datetime:
        return datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))

    class Config:
        schema_extra = {
            "example": {
                "vehicle_id": "V1",
                "timestamp": "2025-09-04T12:01:00Z",
                "latitude": 22.57,
                "longitude": 88.36,
                "speed_kph": 42.0,
                "fuel_percentage": 73.1
            }
        }


class TelemetryBatch(BaseModel):
    """Legacy batch format for backward compatibility."""
    readings: List[TelemetryReading] = Field(..., min_items=1, max_items=1000)
    
    class Config:
        schema_extra = {
            "example": {
                "readings": [
                    {
                        "vehicle_id": "V1",
                        "timestamp": "2025-09-04T12:01:00Z",
                        "latitude": 22.57,
                        "longitude": 88.36,
                        "speed_kph": 42.0,
                        "fuel_percentage": 73.1
                    }
                ]
            }
        }


class SimulationEnginePayload(BaseModel):
    """New payload format from SimulationEngine."""
    timestamp: str = Field(..., description="Batch creation timestamp")
    batch_size: int = Field(..., ge=1, le=10000, description="Number of telemetry records")
    telemetry_data: List[TelemetryReading] = Field(..., description="Array of telemetry readings")

    @validator('timestamp')
    def validate_timestamp(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError("Invalid timestamp format")

    @validator('telemetry_data')
    def validate_telemetry_count(cls, v, values):
        if 'batch_size' in values and len(v) != values['batch_size']:
            raise ValueError("telemetry_data length must match batch_size")
        return v

    class Config:
        schema_extra = {
            "example": {
                "timestamp": "2025-09-04T12:01:00Z",
                "batch_size": 2,
                "telemetry_data": [
                    {
                        "vehicle_id": "V-STANDARD-001",
                        "timestamp": "2025-09-04T12:01:00Z",
                        "latitude": 22.57,
                        "longitude": 88.36,
                        "speed_kph": 42.0,
                        "fuel_percentage": 73.1
                    },
                    {
                        "vehicle_id": "V-ROGUE-002",
                        "timestamp": "2025-09-04T12:01:30Z",
                        "latitude": 22.58,
                        "longitude": 88.37,
                        "speed_kph": 0.0,
                        "fuel_percentage": 68.5
                    }
                ]
            }
        }


class UnifiedTelemetryPayload(BaseModel):
    """Unified payload that can handle both formats."""
    
    # Legacy format fields (optional)
    readings: Optional[List[TelemetryReading]] = None
    
    # SimulationEngine format fields (optional)
    timestamp: Optional[str] = None
    batch_size: Optional[int] = None
    telemetry_data: Optional[List[TelemetryReading]] = None

    @validator('telemetry_data')
    def validate_batch_consistency(cls, v, values):
        if v is not None and 'batch_size' in values and values['batch_size'] is not None:
            if len(v) != values['batch_size']:
                raise ValueError("telemetry_data length must match batch_size")
        return v

    def get_telemetry_readings(self) -> List[TelemetryReading]:
        """Extract telemetry readings regardless of format."""
        if self.telemetry_data is not None:
            # SimulationEngine format
            return self.telemetry_data
        elif self.readings is not None:
            # Legacy format
            return self.readings
        else:
            raise ValueError("No telemetry data found in payload")

    def get_format_type(self) -> str:
        """Determine which format this payload uses."""
        if self.telemetry_data is not None:
            return "simulation_engine"
        elif self.readings is not None:
            return "legacy"
        else:
            return "unknown"


class IngestionResponse(BaseModel):
    """Enhanced response after successful telemetry ingestion."""
    status: str = "success"
    processed_count: int
    duplicate_count: int = 0
    error_count: int = 0
    processing_time_ms: float
    format_detected: str = Field(..., description="Format type: 'legacy' or 'simulation_engine'")
    batch_timestamp: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "status": "success",
                "processed_count": 150,
                "duplicate_count": 5,
                "error_count": 0,
                "processing_time_ms": 234.5,
                "format_detected": "simulation_engine",
                "batch_timestamp": "2025-09-04T12:01:00Z"
            }
        }


class AnomalyEventType(str, Enum):
    FUEL_THEFT = "FUEL_THEFT"
    ROUTE_DEVIATION = "ROUTE_DEVIATION"
    UNUSUAL_STOP = "UNUSUAL_STOP"


class AnomalyEvent(BaseModel):
    vehicle_id: str
    timestamp: str
    event_type: AnomalyEventType
    details: Dict[str, Any]
    confidence_score: Optional[float] = Field(None, ge=0, le=1)

    class Config:
        schema_extra = {
            "example": {
                "vehicle_id": "V1",
                "timestamp": "2025-09-04T12:01:00Z",
                "event_type": "FUEL_THEFT",
                "details": {
                    "liters_stolen": 25.5,
                    "fuel_pct_before": 75.2,
                    "fuel_pct_after": 70.1,
                    "theft_percentage": 5.1
                },
                "confidence_score": 0.95
            }
        }


class HourlyKPI(BaseModel):
    vehicle_id: str
    hour_start: datetime
    utilization_percentage: float = Field(..., ge=0, le=100)
    avg_speed_kph: float = Field(..., ge=0)
    distance_km: float = Field(..., ge=0)
    idle_time_minutes: float = Field(..., ge=0)
    fuel_consumed_liters: Optional[float] = Field(None, ge=0)
    
    class Config:
        schema_extra = {
            "example": {
                "vehicle_id": "V1",
                "hour_start": "2025-09-04T12:00:00Z",
                "utilization_percentage": 85.5,
                "avg_speed_kph": 45.2,
                "distance_km": 38.7,
                "idle_time_minutes": 8.5,
                "fuel_consumed_liters": 9.68
            }
        }


class VehicleStats(BaseModel):
    vehicle_id: str
    period_start: datetime
    period_end: datetime
    total_distance_km: float
    avg_speed_kph: float
    total_idle_time_minutes: float
    utilization_percentage: float
    fuel_efficiency_kmpl: Optional[float] = None
    anomaly_count: int = 0

    class Config:
        schema_extra = {
            "example": {
                "vehicle_id": "V1",
                "period_start": "2025-09-04T00:00:00Z",
                "period_end": "2025-09-04T23:59:59Z",
                "total_distance_km": 245.8,
                "avg_speed_kph": 52.3,
                "total_idle_time_minutes": 45.2,
                "utilization_percentage": 78.5,
                "fuel_efficiency_kmpl": 4.2,
                "anomaly_count": 2
            }
        }