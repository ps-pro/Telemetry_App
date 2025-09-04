"""
Robust telemetry ingestion API endpoints with comprehensive format support.
Handles SimulationEngine format, legacy format, and provides extensive debugging.
"""
import time
import json
from typing import List, Dict, Any, Union
from datetime import datetime

from fastapi import APIRouter, HTTPException, status, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

from utils.logging_config import get_logger

# Create router
router = APIRouter()
logger = get_logger(__name__)

# Enhanced in-memory storage with comprehensive metadata
telemetry_store = []
ingestion_metadata = {
    "total_batches_received": 0,
    "simulation_engine_format_count": 0,
    "legacy_format_count": 0,
    "raw_format_count": 0,
    "total_readings": 0,
    "duplicate_readings": 0,
    "processing_errors": 0,
    "last_batch_timestamp": None,
    "start_time": datetime.now(),
    "recent_payloads": []  # Keep last 5 payloads for debugging
}


class TelemetryReading(BaseModel):
    """Core telemetry reading model."""
    vehicle_id: str
    timestamp: str
    latitude: float
    longitude: float
    speed_kph: float
    fuel_percentage: float


def extract_telemetry_readings(payload: Dict[str, Any]) -> tuple[List[Dict[str, Any]], str]:
    """
    Extract telemetry readings from any supported payload format.
    Returns: (list_of_readings, format_type)
    """
    readings = []
    format_type = "unknown"
    
    try:
        # Format 1: SimulationEngine format {"timestamp": "...", "batch_size": N, "telemetry_data": [...]}
        if "telemetry_data" in payload and "batch_size" in payload:
            format_type = "simulation_engine"
            telemetry_data = payload["telemetry_data"]
            
            for reading_data in telemetry_data:
                # Ensure all required fields are present
                reading = {
                    "vehicle_id": reading_data.get("vehicle_id", "UNKNOWN"),
                    "timestamp": reading_data.get("timestamp", ""),
                    "latitude": float(reading_data.get("latitude", 0.0)),
                    "longitude": float(reading_data.get("longitude", 0.0)),
                    "speed_kph": float(reading_data.get("speed_kph", 0.0)),
                    "fuel_percentage": float(reading_data.get("fuel_percentage", 0.0))
                }
                readings.append(reading)
            
            logger.debug(f"Extracted {len(readings)} readings from SimulationEngine format")
            return readings, format_type
        
        # Format 2: Legacy format {"readings": [...]}
        elif "readings" in payload:
            format_type = "legacy"
            readings_data = payload["readings"]
            
            for reading_data in readings_data:
                reading = {
                    "vehicle_id": reading_data.get("vehicle_id", "UNKNOWN"),
                    "timestamp": reading_data.get("timestamp", ""),
                    "latitude": float(reading_data.get("latitude", 0.0)),
                    "longitude": float(reading_data.get("longitude", 0.0)),
                    "speed_kph": float(reading_data.get("speed_kph", 0.0)),
                    "fuel_percentage": float(reading_data.get("fuel_percentage", 0.0))
                }
                readings.append(reading)
            
            logger.debug(f"Extracted {len(readings)} readings from legacy format")
            return readings, format_type
        
        # Format 3: Raw array format [{"vehicle_id": ...}, ...]
        elif isinstance(payload, list):
            format_type = "raw_array"
            
            for reading_data in payload:
                reading = {
                    "vehicle_id": reading_data.get("vehicle_id", "UNKNOWN"),
                    "timestamp": reading_data.get("timestamp", ""),
                    "latitude": float(reading_data.get("latitude", 0.0)),
                    "longitude": float(reading_data.get("longitude", 0.0)),
                    "speed_kph": float(reading_data.get("speed_kph", 0.0)),
                    "fuel_percentage": float(reading_data.get("fuel_percentage", 0.0))
                }
                readings.append(reading)
            
            logger.debug(f"Extracted {len(readings)} readings from raw array format")
            return readings, format_type
        
        # Format 4: Direct single reading format
        elif "vehicle_id" in payload and "timestamp" in payload:
            format_type = "single_reading"
            
            reading = {
                "vehicle_id": payload.get("vehicle_id", "UNKNOWN"),
                "timestamp": payload.get("timestamp", ""),
                "latitude": float(payload.get("latitude", 0.0)),
                "longitude": float(payload.get("longitude", 0.0)),
                "speed_kph": float(payload.get("speed_kph", 0.0)),
                "fuel_percentage": float(payload.get("fuel_percentage", 0.0))
            }
            readings.append(reading)
            
            logger.debug(f"Extracted single reading from direct format")
            return readings, format_type
        
        else:
            logger.warning(f"Unrecognized payload format. Keys: {list(payload.keys())}")
            return [], "unknown"
    
    except Exception as e:
        logger.error(f"Error extracting telemetry readings: {str(e)}")
        return [], "error"


def check_duplicates(readings: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int]:
    """
    Check for duplicate readings based on vehicle_id + timestamp.
    Returns (unique_readings, duplicate_count)
    """
    global telemetry_store
    
    existing_keys = {f"{r['vehicle_id']}_{r['timestamp']}" for r in telemetry_store}
    unique_readings = []
    duplicates = 0
    
    for reading in readings:
        key = f"{reading['vehicle_id']}_{reading['timestamp']}"
        if key not in existing_keys:
            unique_readings.append(reading)
            existing_keys.add(key)
        else:
            duplicates += 1
            logger.debug(f"Duplicate reading detected: {key}")
    
    return unique_readings, duplicates


def process_telemetry_async(readings: List[Dict[str, Any]], format_type: str, batch_metadata: Dict[str, Any]):
    """
    Background processing of telemetry data with comprehensive logging.
    """
    global telemetry_store, ingestion_metadata
    
    logger.info(f"Background processing: {len(readings)} readings from {format_type} format")
    
    processed_count = 0
    for reading in readings:
        try:
            # Add processing metadata
            enhanced_reading = reading.copy()
            enhanced_reading["_metadata"] = {
                "ingestion_format": format_type,
                "processed_at": datetime.now().isoformat(),
                "batch_id": batch_metadata.get("batch_id", ingestion_metadata["total_batches_received"]),
                "batch_timestamp": batch_metadata.get("batch_timestamp")
            }
            
            telemetry_store.append(enhanced_reading)
            processed_count += 1
            
        except Exception as e:
            logger.error(f"Error processing individual reading: {e}")
            ingestion_metadata["processing_errors"] += 1
    
    # Update global metadata
    ingestion_metadata["total_readings"] += processed_count
    ingestion_metadata["last_batch_timestamp"] = datetime.now().isoformat()
    
    logger.info(f"Background processing complete: {processed_count}/{len(readings)} readings stored successfully")


@router.post("/ingest")
async def ingest_telemetry_universal(
    request: Request,
    background_tasks: BackgroundTasks
):
    """
    Universal telemetry ingestion endpoint that accepts any reasonable format.
    Provides comprehensive debugging and error reporting.
    """
    start_time = time.time()
    global ingestion_metadata
    
    try:
        # Parse raw request body
        raw_body = await request.body()
        logger.debug(f"Received request body length: {len(raw_body)} bytes")
        
        # Parse JSON payload
        try:
            payload = json.loads(raw_body.decode('utf-8'))
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON payload: {str(e)}")
            return JSONResponse(
                status_code=400,
                content={
                    "status": "json_error",
                    "error": f"Invalid JSON: {str(e)}",
                    "processed_count": 0,
                    "duplicate_count": 0,
                    "error_count": 1,
                    "processing_time_ms": (time.time() - start_time) * 1000
                }
            )
        
        # Store recent payload for debugging (keep last 5)
        ingestion_metadata["recent_payloads"].append({
            "timestamp": datetime.now().isoformat(),
            "payload_keys": list(payload.keys()) if isinstance(payload, dict) else "array",
            "payload_size": len(str(payload))
        })
        if len(ingestion_metadata["recent_payloads"]) > 5:
            ingestion_metadata["recent_payloads"].pop(0)
        
        logger.debug(f"Payload structure: {type(payload)}")
        if isinstance(payload, dict):
            logger.debug(f"Payload keys: {list(payload.keys())}")
        elif isinstance(payload, list):
            logger.debug(f"Payload is array with {len(payload)} items")
        
        # Extract telemetry readings
        readings, format_type = extract_telemetry_readings(payload)
        
        if not readings:
            logger.warning("No telemetry readings found in payload")
            return JSONResponse(
                content={
                    "status": "no_data",
                    "message": "No telemetry readings found in payload",
                    "processed_count": 0,
                    "duplicate_count": 0,
                    "error_count": 0,
                    "processing_time_ms": (time.time() - start_time) * 1000,
                    "format_detected": format_type,
                    "payload_debug": {
                        "payload_type": type(payload).__name__,
                        "payload_keys": list(payload.keys()) if isinstance(payload, dict) else "not_dict"
                    }
                }
            )
        
        logger.info(f"Extracted {len(readings)} readings from {format_type} format")
        
        # Update format counters
        ingestion_metadata["total_batches_received"] += 1
        if format_type == "simulation_engine":
            ingestion_metadata["simulation_engine_format_count"] += 1
        elif format_type == "legacy":
            ingestion_metadata["legacy_format_count"] += 1
        else:
            ingestion_metadata["raw_format_count"] += 1
        
        # Check for duplicates
        unique_readings, duplicate_count = check_duplicates(readings)
        ingestion_metadata["duplicate_readings"] += duplicate_count
        
        if not unique_readings:
            logger.info("All readings were duplicates")
            return JSONResponse(
                content={
                    "status": "success",
                    "message": "All readings were duplicates",
                    "processed_count": 0,
                    "duplicate_count": duplicate_count,
                    "error_count": 0,
                    "processing_time_ms": (time.time() - start_time) * 1000,
                    "format_detected": format_type
                }
            )
        
        # Prepare batch metadata
        batch_metadata = {
            "batch_id": ingestion_metadata["total_batches_received"],
            "batch_timestamp": payload.get("timestamp") if isinstance(payload, dict) else None,
            "original_format": format_type
        }
        
        # Process readings in background
        background_tasks.add_task(process_telemetry_async, unique_readings, format_type, batch_metadata)
        
        processing_time = (time.time() - start_time) * 1000
        
        logger.info(
            f"Ingestion successful: {len(unique_readings)} processed, "
            f"{duplicate_count} duplicates, {processing_time:.2f}ms, format: {format_type}"
        )
        
        return JSONResponse(
            content={
                "status": "success",
                "processed_count": len(unique_readings),
                "duplicate_count": duplicate_count,
                "error_count": 0,
                "processing_time_ms": processing_time,
                "format_detected": format_type,
                "batch_timestamp": batch_metadata["batch_timestamp"]
            }
        )
        
    except Exception as e:
        logger.error(f"Unexpected error during ingestion: {str(e)}", exc_info=True)
        ingestion_metadata["processing_errors"] += 1
        
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": str(e),
                "processed_count": 0,
                "duplicate_count": 0,
                "error_count": 1,
                "processing_time_ms": (time.time() - start_time) * 1000
            }
        )


@router.get("/ingest/stats")
async def get_ingestion_stats():
    """
    Comprehensive ingestion statistics with debugging information.
    """
    global telemetry_store, ingestion_metadata
    
    if not telemetry_store:
        return {
            "total_readings": 0,
            "unique_vehicles": 0,
            "metadata": ingestion_metadata,
            "message": "No data received yet"
        }
    
    # Calculate detailed statistics
    vehicle_ids = set(r["vehicle_id"] for r in telemetry_store)
    timestamps = [r["timestamp"] for r in telemetry_store]
    
    # Format breakdown from stored data
    format_counts = {}
    for r in telemetry_store:
        if "_metadata" in r and "ingestion_format" in r["_metadata"]:
            fmt = r["_metadata"]["ingestion_format"]
            format_counts[fmt] = format_counts.get(fmt, 0) + 1
    
    # Vehicle activity analysis
    vehicle_activity = {}
    for r in telemetry_store:
        vid = r["vehicle_id"]
        if vid not in vehicle_activity:
            vehicle_activity[vid] = {"count": 0, "latest_timestamp": None, "formats": set()}
        
        vehicle_activity[vid]["count"] += 1
        
        if not vehicle_activity[vid]["latest_timestamp"] or r["timestamp"] > vehicle_activity[vid]["latest_timestamp"]:
            vehicle_activity[vid]["latest_timestamp"] = r["timestamp"]
        
        if "_metadata" in r and "ingestion_format" in r["_metadata"]:
            vehicle_activity[vid]["formats"].add(r["_metadata"]["ingestion_format"])
    
    # Convert sets to lists for JSON serialization
    for vid in vehicle_activity:
        vehicle_activity[vid]["formats"] = list(vehicle_activity[vid]["formats"])
    
    # Recent activity (last 10 readings)
    recent_readings = telemetry_store[-10:] if len(telemetry_store) >= 10 else telemetry_store
    recent_summary = []
    for r in recent_readings:
        recent_summary.append({
            "vehicle_id": r["vehicle_id"],
            "timestamp": r["timestamp"],
            "speed_kph": r["speed_kph"],
            "fuel_percentage": r["fuel_percentage"],
            "format": r.get("_metadata", {}).get("ingestion_format", "unknown")
        })
    
    return {
        "total_readings": len(telemetry_store),
        "unique_vehicles": len(vehicle_ids),
        "vehicle_list": sorted(list(vehicle_ids)),
        "latest_timestamp": max(timestamps) if timestamps else None,
        "oldest_timestamp": min(timestamps) if timestamps else None,
        "format_breakdown": {
            "stored_readings_by_format": format_counts,
            "batches_by_format": {
                "simulation_engine": ingestion_metadata["simulation_engine_format_count"],
                "legacy": ingestion_metadata["legacy_format_count"],
                "raw": ingestion_metadata["raw_format_count"]
            }
        },
        "processing_statistics": {
            "total_batches_received": ingestion_metadata["total_batches_received"],
            "total_readings_processed": ingestion_metadata["total_readings"],
            "duplicate_readings": ingestion_metadata["duplicate_readings"],
            "processing_errors": ingestion_metadata["processing_errors"],
            "uptime_seconds": (datetime.now() - ingestion_metadata["start_time"]).total_seconds()
        },
        "vehicle_activity": vehicle_activity,
        "recent_readings": recent_summary,
        "debug_info": {
            "recent_payload_info": ingestion_metadata["recent_payloads"]
        }
    }


@router.get("/ingest/health")
async def ingestion_health_check():
    """
    Detailed health check for the ingestion system.
    """
    global ingestion_metadata, telemetry_store
    
    current_time = datetime.now()
    uptime = current_time - ingestion_metadata["start_time"]
    
    # Check recent activity
    last_batch_time = ingestion_metadata.get("last_batch_timestamp")
    is_receiving_data = False
    
    if last_batch_time:
        last_batch_dt = datetime.fromisoformat(last_batch_time)
        time_since_last_batch = (current_time - last_batch_dt).total_seconds()
        is_receiving_data = time_since_last_batch < 300  # 5 minutes
    
    # Determine health status
    if ingestion_metadata["processing_errors"] > 10:
        health_status = "degraded"
    elif ingestion_metadata["total_batches_received"] == 0:
        health_status = "waiting"
    elif is_receiving_data:
        health_status = "healthy"
    else:
        health_status = "stale"
    
    return {
        "status": health_status,
        "timestamp": current_time.isoformat(),
        "uptime_seconds": uptime.total_seconds(),
        "statistics": {
            "total_batches": ingestion_metadata["total_batches_received"],
            "total_readings": len(telemetry_store),
            "processing_errors": ingestion_metadata["processing_errors"],
            "success_rate": "N/A" if ingestion_metadata["total_batches_received"] == 0 else f"{((ingestion_metadata['total_batches_received'] - ingestion_metadata['processing_errors']) / ingestion_metadata['total_batches_received'] * 100):.1f}%"
        },
        "format_support": {
            "simulation_engine_batches": ingestion_metadata["simulation_engine_format_count"],
            "legacy_batches": ingestion_metadata["legacy_format_count"],
            "raw_batches": ingestion_metadata["raw_format_count"]
        },
        "last_activity": last_batch_time,
        "is_receiving_data": is_receiving_data
    }


@router.delete("/ingest/clear")
async def clear_telemetry_store():
    """
    Clear all stored telemetry data and reset metadata.
    WARNING: Only for development/testing!
    """
    global telemetry_store, ingestion_metadata
    
    count = len(telemetry_store)
    telemetry_store.clear()
    
    # Reset metadata but preserve start time
    start_time = ingestion_metadata["start_time"]
    ingestion_metadata.clear()
    ingestion_metadata.update({
        "total_batches_received": 0,
        "simulation_engine_format_count": 0,
        "legacy_format_count": 0,
        "raw_format_count": 0,
        "total_readings": 0,
        "duplicate_readings": 0,
        "processing_errors": 0,
        "last_batch_timestamp": None,
        "start_time": start_time,
        "recent_payloads": []
    })
    
    logger.info(f"Cleared {count} telemetry readings and reset all metadata")
    
    return {
        "status": "cleared",
        "removed_count": count,
        "message": "All telemetry data and metadata has been cleared"
    }


@router.get("/ingest/debug")
async def get_debug_info():
    """
    Debug endpoint for troubleshooting ingestion issues.
    """
    global telemetry_store, ingestion_metadata
    
    sample_readings = telemetry_store[:3] if telemetry_store else []
    
    return {
        "ingestion_metadata": ingestion_metadata,
        "sample_stored_readings": sample_readings,
        "storage_info": {
            "total_stored": len(telemetry_store),
            "memory_size_estimate": len(str(telemetry_store)) if telemetry_store else 0
        },
        "supported_formats": [
            "SimulationEngine: {timestamp, batch_size, telemetry_data}",
            "Legacy: {readings}",
            "Raw Array: [{vehicle_id, timestamp, ...}, ...]",
            "Single Reading: {vehicle_id, timestamp, ...}"
        ]
    }