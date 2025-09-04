"""
Query API endpoints for retrieving vehicle statistics and anomaly alerts.
"""
from datetime import datetime, timedelta
from typing import List, Optional
import statistics

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import JSONResponse

from models.telemetry import VehicleStats, AnomalyEvent
from utils.logging_config import get_logger

# Create router
router = APIRouter()
logger = get_logger(__name__)

# Mock anomaly events storage (will be replaced with database)
anomaly_store = []


def calculate_vehicle_stats(vehicle_id: str, from_time: str, to_time: str) -> Optional[VehicleStats]:
    """
    Calculate vehicle statistics from stored telemetry data.
    This is a simplified version - production would use database aggregations.
    """
    global telemetry_store
    
    # Filter readings for the vehicle and time range
    vehicle_readings = []
    for reading in telemetry_store:
        if reading["vehicle_id"] == vehicle_id:
            reading_time = datetime.fromisoformat(reading["timestamp"].replace('Z', '+00:00'))
            from_dt = datetime.fromisoformat(from_time.replace('Z', '+00:00'))
            to_dt = datetime.fromisoformat(to_time.replace('Z', '+00:00'))
            
            if from_dt <= reading_time <= to_dt:
                vehicle_readings.append(reading)
    
    if not vehicle_readings:
        return None
    
    # Sort by timestamp
    vehicle_readings.sort(key=lambda x: x["timestamp"])
    
    # Calculate basic statistics
    speeds = [r["speed_kph"] for r in vehicle_readings]
    avg_speed = statistics.mean(speeds) if speeds else 0.0
    
    # Calculate distance (simplified - sum of speeds)
    # In reality, you'd use GPS coordinates and time deltas
    total_distance = sum(speeds) * (1/60)  # Rough approximation
    
    # Count idle time (speed = 0)
    idle_readings = [r for r in vehicle_readings if r["speed_kph"] == 0]
    idle_time_minutes = len(idle_readings) * 0.5  # Assuming 30-second readings
    
    # Calculate utilization (moving time / total time)
    moving_readings = len(vehicle_readings) - len(idle_readings)
    utilization_pct = (moving_readings / len(vehicle_readings)) * 100 if vehicle_readings else 0
    
    # Fuel efficiency (mock calculation)
    fuel_start = vehicle_readings[0]["fuel_percentage"]
    fuel_end = vehicle_readings[-1]["fuel_percentage"]
    fuel_consumed_pct = fuel_start - fuel_end
    fuel_efficiency = total_distance / max(fuel_consumed_pct, 0.1) if fuel_consumed_pct > 0 else None
    
    return VehicleStats(
        vehicle_id=vehicle_id,
        period_start=datetime.fromisoformat(from_time.replace('Z', '+00:00')),
        period_end=datetime.fromisoformat(to_time.replace('Z', '+00:00')),
        total_distance_km=round(total_distance, 2),
        avg_speed_kph=round(avg_speed, 2),
        total_idle_time_minutes=round(idle_time_minutes, 2),
        utilization_percentage=round(utilization_pct, 2),
        fuel_efficiency_kmpl=round(fuel_efficiency, 2) if fuel_efficiency else None,
        anomaly_count=len([a for a in anomaly_store if a["vehicle_id"] == vehicle_id])
    )


@router.get("/stats/vehicle/{vehicle_id}", response_model=VehicleStats)
async def get_vehicle_stats(
    vehicle_id: str,
    from_time: Optional[str] = Query(None, description="Start time (ISO format with Z)"),
    to_time: Optional[str] = Query(None, description="End time (ISO format with Z)")
) -> VehicleStats:
    """
    Get vehicle statistics for a given time period.
    
    - **vehicle_id**: The ID of the vehicle
    - **from_time**: Start time (defaults to 24 hours ago)
    - **to_time**: End time (defaults to now)
    """
    try:
        # Default time range to last 24 hours if not provided
        if not to_time:
            to_time = datetime.now().isoformat() + "Z"
        if not from_time:
            from_dt = datetime.now() - timedelta(hours=24)
            from_time = from_dt.isoformat() + "Z"
        
        logger.info(f"Getting stats for vehicle {vehicle_id} from {from_time} to {to_time}")
        
        stats = calculate_vehicle_stats(vehicle_id, from_time, to_time)
        
        if not stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No telemetry data found for vehicle {vehicle_id} in the specified time range"
            )
        
        return stats
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid timestamp format: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error calculating stats for vehicle {vehicle_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error while calculating statistics"
        )


@router.get("/alerts")
async def get_alerts(
    alert_type: Optional[str] = Query(None, description="Filter by alert type (e.g., 'fuel_theft')"),
    vehicle_id: Optional[str] = Query(None, description="Filter by vehicle ID"),
    from_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    to_time: Optional[str] = Query(None, description="End time (ISO format)"),
    limit: int = Query(100, le=1000, description="Maximum number of alerts to return")
):
    """
    Get anomaly alerts with optional filtering.
    
    - **alert_type**: Filter by specific alert type (e.g., 'FUEL_THEFT')
    - **vehicle_id**: Filter by vehicle ID
    - **from_time**: Start time filter
    - **to_time**: End time filter
    - **limit**: Maximum number of results
    """
    try:
        # For now, return mock data since we haven't implemented anomaly detection yet
        # In production, this would query the database
        
        mock_alerts = [
            {
                "vehicle_id": "V1",
                "timestamp": "2025-09-04T10:30:00Z",
                "event_type": "FUEL_THEFT",
                "details": {
                    "liters_stolen": 15.5,
                    "fuel_pct_before": 75.0,
                    "fuel_pct_after": 68.9,
                    "theft_percentage": 6.1
                },
                "confidence_score": 0.92
            },
            {
                "vehicle_id": "V2",
                "timestamp": "2025-09-04T14:15:00Z",
                "event_type": "FUEL_THEFT",
                "details": {
                    "liters_stolen": 8.2,
                    "fuel_pct_before": 45.3,
                    "fuel_pct_after": 42.1,
                    "theft_percentage": 3.2
                },
                "confidence_score": 0.87
            }
        ]
        
        # Apply filters
        filtered_alerts = mock_alerts
        
        if alert_type:
            filtered_alerts = [a for a in filtered_alerts if a["event_type"].lower() == alert_type.lower()]
        
        if vehicle_id:
            filtered_alerts = [a for a in filtered_alerts if a["vehicle_id"] == vehicle_id]
        
        # Apply limit
        filtered_alerts = filtered_alerts[:limit]
        
        logger.info(f"Retrieved {len(filtered_alerts)} alerts with filters: type={alert_type}, vehicle={vehicle_id}")
        
        return {
            "alerts": filtered_alerts,
            "count": len(filtered_alerts),
            "filters_applied": {
                "alert_type": alert_type,
                "vehicle_id": vehicle_id,
                "from_time": from_time,
                "to_time": to_time
            }
        }
        
    except Exception as e:
        logger.error(f"Error retrieving alerts: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error while retrieving alerts"
        )


@router.get("/stats/summary")
async def get_platform_summary():
    """
    Get overall platform statistics.
    Useful for monitoring and dashboards.
    """
    global telemetry_store, anomaly_store
    
    try:
        vehicle_ids = set(r["vehicle_id"] for r in telemetry_store)
        
        # Calculate some basic aggregations
        total_readings = len(telemetry_store)
        unique_vehicles = len(vehicle_ids)
        total_anomalies = len(anomaly_store)
        
        # Recent activity (last hour)
        one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat() + "Z"
        recent_readings = [
            r for r in telemetry_store 
            if r["timestamp"] >= one_hour_ago
        ]
        
        return {
            "platform_stats": {
                "total_readings": total_readings,
                "unique_vehicles": unique_vehicles,
                "total_anomalies": total_anomalies,
                "recent_readings_1h": len(recent_readings)
            },
            "vehicle_list": sorted(list(vehicle_ids)),
            "last_updated": datetime.now().isoformat() + "Z"
        }
        
    except Exception as e:
        logger.error(f"Error generating platform summary: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error while generating summary"
        )