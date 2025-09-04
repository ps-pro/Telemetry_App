"""
Database-powered telemetry ingestion API endpoints.
Replaces in-memory storage with PostgreSQL + TimescaleDB persistence.
"""
import time
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from decimal import Decimal

from fastapi import APIRouter, HTTPException, status, BackgroundTasks, Request, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import func, text, and_, desc

from database.connection import get_db_session, DatabaseService
from database.models import (
    Vehicle, TelemetryReading, AnomalyEvent, IngestionBatch,
    IngestionFormatEnum
)
from utils.logging_config import get_logger

# Create router
router = APIRouter()
logger = get_logger(__name__)


def extract_telemetry_readings(payload: Dict[str, Any]) -> tuple[List[Dict[str, Any]], str]:
    """
    Extract telemetry readings from any supported payload format.
    Returns: (list_of_readings, format_type)
    """
    readings = []
    format_type = "unknown"
    
    try:
        # Format 1: SimulationEngine format
        if "telemetry_data" in payload and "batch_size" in payload:
            format_type = "simulation_engine"
            telemetry_data = payload["telemetry_data"]
            
            for reading_data in telemetry_data:
                reading = {
                    "vehicle_id": str(reading_data.get("vehicle_id", "UNKNOWN")),
                    "timestamp": datetime.fromisoformat(
                        reading_data.get("timestamp", "").replace('Z', '+00:00')
                    ),
                    "latitude": Decimal(str(reading_data.get("latitude", 0.0))),
                    "longitude": Decimal(str(reading_data.get("longitude", 0.0))),
                    "speed_kph": Decimal(str(reading_data.get("speed_kph", 0.0))),
                    "fuel_percentage": Decimal(str(reading_data.get("fuel_percentage", 0.0))),
                    "ingestion_format": "SIMULATION_ENGINE"
                }
                readings.append(reading)
            
            logger.debug(f"Extracted {len(readings)} readings from SimulationEngine format")
            return readings, format_type
        
        # Format 2: Legacy format
        elif "readings" in payload:
            format_type = "legacy"
            readings_data = payload["readings"]
            
            for reading_data in readings_data:
                reading = {
                    "vehicle_id": str(reading_data.get("vehicle_id", "UNKNOWN")),
                    "timestamp": datetime.fromisoformat(
                        reading_data.get("timestamp", "").replace('Z', '+00:00')
                    ),
                    "latitude": Decimal(str(reading_data.get("latitude", 0.0))),
                    "longitude": Decimal(str(reading_data.get("longitude", 0.0))),
                    "speed_kph": Decimal(str(reading_data.get("speed_kph", 0.0))),
                    "fuel_percentage": Decimal(str(reading_data.get("fuel_percentage", 0.0))),
                    "ingestion_format": "LEGACY"
                }
                readings.append(reading)
            
            logger.debug(f"Extracted {len(readings)} readings from legacy format")
            return readings, format_type
        
        # Format 3: Raw array format
        elif isinstance(payload, list):
            format_type = "raw_array"
            
            for reading_data in payload:
                reading = {
                    "vehicle_id": str(reading_data.get("vehicle_id", "UNKNOWN")),
                    "timestamp": datetime.fromisoformat(
                        reading_data.get("timestamp", "").replace('Z', '+00:00')
                    ),
                    "latitude": Decimal(str(reading_data.get("latitude", 0.0))),
                    "longitude": Decimal(str(reading_data.get("longitude", 0.0))),
                    "speed_kph": Decimal(str(reading_data.get("speed_kph", 0.0))),
                    "fuel_percentage": Decimal(str(reading_data.get("fuel_percentage", 0.0))),
                    "ingestion_format": "RAW_ARRAY"
                }
                readings.append(reading)
            
            logger.debug(f"Extracted {len(readings)} readings from raw array format")
            return readings, format_type
        
        # Format 4: Single reading
        elif "vehicle_id" in payload and "timestamp" in payload:
            format_type = "single_reading"
            
            reading = {
                "vehicle_id": str(payload.get("vehicle_id", "UNKNOWN")),
                "timestamp": datetime.fromisoformat(
                    payload.get("timestamp", "").replace('Z', '+00:00')
                ),
                "latitude": Decimal(str(payload.get("latitude", 0.0))),
                "longitude": Decimal(str(payload.get("longitude", 0.0))),
                "speed_kph": Decimal(str(payload.get("speed_kph", 0.0))),
                "fuel_percentage": Decimal(str(payload.get("fuel_percentage", 0.0))),
                "ingestion_format": "SINGLE_READING"
            }
            readings.append(reading)
            
            logger.debug("Extracted single reading from direct format")
            return readings, format_type
        
        else:
            logger.warning(f"Unrecognized payload format. Keys: {list(payload.keys()) if isinstance(payload, dict) else 'not_dict'}")
            return [], "unknown"
    
    except Exception as e:
        logger.error(f"Error extracting telemetry readings: {str(e)}")
        return [], "error"


def check_duplicates_db(session: Session, readings: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int]:
    """
    Check for duplicates using database queries.
    Returns (unique_readings, duplicate_count)
    """
    if not readings:
        return [], 0
    
    unique_readings = []
    duplicate_count = 0
    
    # Build list of (vehicle_id, timestamp) pairs to check
    check_pairs = [(r["vehicle_id"], r["timestamp"]) for r in readings]
    
    # Query existing readings in batch
    existing_pairs = set()
    if check_pairs:
        # Create conditions for batch lookup
        conditions = []
        for vehicle_id, timestamp in check_pairs:
            conditions.append(
                and_(
                    TelemetryReading.vehicle_id == vehicle_id,
                    TelemetryReading.timestamp == timestamp
                )
            )
        
        # Query in chunks to avoid too large OR conditions
        chunk_size = 100
        for i in range(0, len(conditions), chunk_size):
            chunk_conditions = conditions[i:i + chunk_size]
            if chunk_conditions:
                from sqlalchemy import or_
                existing = session.query(
                    TelemetryReading.vehicle_id,
                    TelemetryReading.timestamp
                ).filter(or_(*chunk_conditions)).all()
                
                existing_pairs.update({(v_id, ts) for v_id, ts in existing})
    
    # Filter out duplicates
    for reading in readings:
        key_pair = (reading["vehicle_id"], reading["timestamp"])
        if key_pair not in existing_pairs:
            unique_readings.append(reading)
        else:
            duplicate_count += 1
            logger.debug(f"Duplicate detected: {key_pair}")
    
    return unique_readings, duplicate_count


async def process_telemetry_batch_db(
    readings: List[Dict[str, Any]], 
    format_type: str, 
    batch_metadata: Dict[str, Any],
    session: Session
):
    """
    Process telemetry readings with database storage.
    """
    if not readings:
        return {"processed": 0, "errors": 0}
    
    logger.info(f"Processing {len(readings)} readings from {format_type} format")
    
    try:
        # Create database service
        db_service = DatabaseService(session)
        
        # Add batch metadata to readings
        for reading in readings:
            reading.update({
                "batch_id": batch_metadata.get("batch_id"),
                "processing_metadata": {
                    "batch_timestamp": batch_metadata.get("batch_timestamp"),
                    "original_format": format_type,
                    "processed_at": datetime.now().isoformat()
                }
            })
        
        # Bulk insert readings
        processed_count = db_service.bulk_insert_telemetry(readings)
        
        # Commit the transaction
        session.commit()
        
        logger.info(f"Successfully processed {processed_count} telemetry readings")
        return {"processed": processed_count, "errors": 0}
        
    except Exception as e:
        logger.error(f"Error processing telemetry batch: {e}")
        session.rollback()
        return {"processed": 0, "errors": len(readings)}


@router.post("/ingest")
async def ingest_telemetry_db(
    request: Request,
    background_tasks: BackgroundTasks,
    session: Session = Depends(get_db_session)
):
    """
    Database-powered universal telemetry ingestion endpoint.
    Stores data in PostgreSQL + TimescaleDB with comprehensive tracking.
    """
    start_time = time.time()
    
    try:
        # Parse request body
        raw_body = await request.body()
        logger.debug(f"Received request body length: {len(raw_body)} bytes")
        
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
                    "format_detected": format_type
                }
            )
        
        logger.info(f"Extracted {len(readings)} readings from {format_type} format")
        
        # Create ingestion batch record
        batch_record = IngestionBatch(
            batch_timestamp=datetime.fromisoformat(payload.get("timestamp", datetime.now().isoformat()).replace('Z', '+00:00')) if isinstance(payload, dict) else datetime.now(),
            ingestion_format=format_type.upper(),
            total_readings=len(readings),
            payload_metadata={
                "payload_keys": list(payload.keys()) if isinstance(payload, dict) else ["array"],
                "payload_size": len(str(payload))
            }
        )
        session.add(batch_record)
        session.flush()  # Get batch_id
        
        # Check for duplicates
        unique_readings, duplicate_count = check_duplicates_db(session, readings)
        
        # Update batch record with duplicate info
        batch_record.duplicate_readings = duplicate_count
        batch_record.processed_readings = len(unique_readings)
        batch_record.error_readings = len(readings) - len(unique_readings) - duplicate_count
        
        if not unique_readings:
            logger.info("All readings were duplicates")
            batch_record.processing_time_ms = Decimal(str((time.time() - start_time) * 1000))
            session.commit()
            
            return JSONResponse(
                content={
                    "status": "success",
                    "message": "All readings were duplicates",
                    "processed_count": 0,
                    "duplicate_count": duplicate_count,
                    "error_count": 0,
                    "processing_time_ms": float(batch_record.processing_time_ms),
                    "format_detected": format_type,
                    "batch_id": batch_record.batch_id
                }
            )
        
        # Prepare batch metadata
        batch_metadata = {
            "batch_id": batch_record.batch_id,
            "batch_timestamp": batch_record.batch_timestamp.isoformat() if batch_record.batch_timestamp else None,
            "original_format": format_type
        }
        
        # Process readings synchronously for now (can be made async later)
        result = await process_telemetry_batch_db(unique_readings, format_type, batch_metadata, session)
        
        # Update final batch record
        processing_time = (time.time() - start_time) * 1000
        batch_record.processing_time_ms = Decimal(str(processing_time))
        batch_record.processed_readings = result["processed"]
        batch_record.error_readings = result["errors"]
        
        session.commit()
        
        logger.info(
            f"Ingestion complete: {result['processed']} processed, "
            f"{duplicate_count} duplicates, {processing_time:.2f}ms, format: {format_type}"
        )
        
        return JSONResponse(
            content={
                "status": "success",
                "processed_count": result["processed"],
                "duplicate_count": duplicate_count,
                "error_count": result["errors"],
                "processing_time_ms": processing_time,
                "format_detected": format_type,
                "batch_id": batch_record.batch_id,
                "batch_timestamp": batch_metadata["batch_timestamp"]
            }
        )
        
    except Exception as e:
        logger.error(f"Unexpected error during ingestion: {str(e)}", exc_info=True)
        session.rollback()
        
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
async def get_ingestion_stats_db(session: Session = Depends(get_db_session)):
    """
    Database-powered ingestion statistics.
    """
    try:
        # Get basic counts
        total_readings = session.query(TelemetryReading).count()
        total_vehicles = session.query(Vehicle).count()
        total_batches = session.query(IngestionBatch).count()
        total_anomalies = session.query(AnomalyEvent).count()
        
        if total_readings == 0:
            return {
                "total_readings": 0,
                "unique_vehicles": 0,
                "total_batches": 0,
                "total_anomalies": 0,
                "message": "No data in database yet"
            }
        
        # Get vehicle list and activity
        vehicle_activity = session.query(
            TelemetryReading.vehicle_id,
            func.count(TelemetryReading.id).label('reading_count'),
            func.max(TelemetryReading.timestamp).label('latest_timestamp'),
            func.array_agg(TelemetryReading.ingestion_format.distinct()).label('formats')
        ).group_by(TelemetryReading.vehicle_id).all()
        
        vehicle_activity_dict = {}
        vehicle_list = []
        for vehicle_id, count, latest_ts, formats in vehicle_activity:
            vehicle_list.append(vehicle_id)
            vehicle_activity_dict[vehicle_id] = {
                "count": count,
                "latest_timestamp": latest_ts.isoformat() if latest_ts else None,
                "formats": [f if f else 'unknown' for f in (formats or [])]
            }
        
        # Get format breakdown
        format_breakdown = session.query(
            TelemetryReading.ingestion_format,
            func.count(TelemetryReading.id).label('count')
        ).group_by(TelemetryReading.ingestion_format).all()
        
        format_counts = {
            fmt if fmt else 'unknown': count 
            for fmt, count in format_breakdown
        }
        
        # Get batch statistics
        batch_stats = session.query(
            func.sum(IngestionBatch.total_readings).label('total_submitted'),
            func.sum(IngestionBatch.processed_readings).label('total_processed'),
            func.sum(IngestionBatch.duplicate_readings).label('total_duplicates'),
            func.sum(IngestionBatch.error_readings).label('total_errors'),
            func.avg(IngestionBatch.processing_time_ms).label('avg_processing_time')
        ).first()
        
        # Get recent readings (last 10)
        recent_readings = session.query(TelemetryReading).order_by(
            desc(TelemetryReading.timestamp)
        ).limit(10).all()
        
        recent_summary = [
            {
                "vehicle_id": r.vehicle_id,
                "timestamp": r.timestamp.isoformat(),
                "speed_kph": float(r.speed_kph),
                "fuel_percentage": float(r.fuel_percentage),
                "format": r.ingestion_format if r.ingestion_format else 'unknown'
            }
            for r in recent_readings
        ]
        
        # Get time range
        time_range = session.query(
            func.min(TelemetryReading.timestamp).label('oldest'),
            func.max(TelemetryReading.timestamp).label('latest')
        ).first()
        
        return {
            "total_readings": total_readings,
            "unique_vehicles": total_vehicles,
            "total_batches": total_batches,
            "total_anomalies": total_anomalies,
            "vehicle_list": sorted(vehicle_list),
            "time_range": {
                "oldest_timestamp": time_range.oldest.isoformat() if time_range.oldest else None,
                "latest_timestamp": time_range.latest.isoformat() if time_range.latest else None
            },
            "format_breakdown": {
                "readings_by_format": format_counts
            },
            "batch_statistics": {
                "total_submitted": int(batch_stats.total_submitted or 0),
                "total_processed": int(batch_stats.total_processed or 0),
                "total_duplicates": int(batch_stats.total_duplicates or 0),
                "total_errors": int(batch_stats.total_errors or 0),
                "avg_processing_time_ms": float(batch_stats.avg_processing_time or 0)
            },
            "vehicle_activity": vehicle_activity_dict,
            "recent_readings": recent_summary
        }
        
    except Exception as e:
        logger.error(f"Error getting ingestion stats: {e}")
        return {"error": str(e)}


@router.get("/ingest/health")
async def ingestion_health_check_db(session: Session = Depends(get_db_session)):
    """
    Database-powered ingestion health check.
    """
    try:
        # Check database connectivity
        session.execute(text("SELECT 1")).scalar()
        
        # Get recent activity
        recent_batches = session.query(IngestionBatch).filter(
            IngestionBatch.created_at >= datetime.now() - timedelta(minutes=5)
        ).count()
        
        total_readings = session.query(TelemetryReading).count()
        total_batches = session.query(IngestionBatch).count()
        
        # Determine health status
        if recent_batches > 0:
            health_status = "healthy"
        elif total_batches > 0:
            health_status = "stale"
        else:
            health_status = "waiting"
        
        # Get error rate
        error_batches = session.query(IngestionBatch).filter(
            IngestionBatch.error_readings > 0
        ).count()
        
        error_rate = (error_batches / max(total_batches, 1)) * 100
        
        return {
            "status": health_status,
            "timestamp": datetime.now().isoformat(),
            "database_connected": True,
            "statistics": {
                "total_readings": total_readings,
                "total_batches": total_batches,
                "recent_batches_5min": recent_batches,
                "error_rate_percent": round(error_rate, 2)
            },
            "health_indicators": {
                "database_responsive": True,
                "recent_activity": recent_batches > 0,
                "error_rate_acceptable": error_rate < 5.0
            }
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "database_connected": False,
            "error": str(e)
        }


@router.get("/ingest/debug")
async def get_debug_info_db(session: Session = Depends(get_db_session)):
    """
    Database-powered debug information.
    """
    try:
        from database.connection import get_database_manager
        
        db_manager = get_database_manager()
        db_health = db_manager.health_check()
        db_info = db_manager.get_database_info()
        
        # Get sample data
        sample_readings = session.query(TelemetryReading).limit(3).all()
        sample_batches = session.query(IngestionBatch).order_by(
            desc(IngestionBatch.created_at)
        ).limit(3).all()
        
        return {
            "database_health": db_health,
            "database_info": db_info,
            "sample_readings": [r.to_dict() for r in sample_readings],
            "sample_batches": [b.to_dict() for b in sample_batches],
            "supported_formats": [
                "SimulationEngine: {timestamp, batch_size, telemetry_data}",
                "Legacy: {readings}",
                "Raw Array: [{...}, {...}]",
                "Single Reading: {vehicle_id, timestamp, ...}"
            ]
        }
        
    except Exception as e:
        logger.error(f"Debug info failed: {e}")
        return {"error": str(e)}


@router.delete("/ingest/clear")
async def clear_telemetry_data_db(session: Session = Depends(get_db_session)):
    """
    Clear all telemetry data from database.
    WARNING: Only for development/testing!
    """
    try:
        # Count before deletion
        reading_count = session.query(TelemetryReading).count()
        batch_count = session.query(IngestionBatch).count()
        anomaly_count = session.query(AnomalyEvent).count()
        
        # Delete all data
        session.query(TelemetryReading).delete()
        session.query(IngestionBatch).delete()
        session.query(AnomalyEvent).delete()
        
        # Reset sequences
        session.execute(text("ALTER SEQUENCE telemetry.ingestion_batches_batch_id_seq RESTART WITH 1"))
        
        session.commit()
        
        logger.warning(f"Cleared database: {reading_count} readings, {batch_count} batches, {anomaly_count} anomalies")
        
        return {
            "status": "cleared",
            "removed_counts": {
                "telemetry_readings": reading_count,
                "ingestion_batches": batch_count,
                "anomaly_events": anomaly_count
            },
            "message": "All telemetry data has been cleared from database"
        }
        
    except Exception as e:
        logger.error(f"Clear operation failed: {e}")
        session.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear data: {str(e)}"
        )