"""
Database models using SQLAlchemy ORM for telemetry platform.
Matches the TimescaleDB schema with proper relationships and constraints.
"""
import uuid
from datetime import datetime
from typing import Optional, Dict, Any
from decimal import Decimal

from sqlalchemy import Column, String, DateTime, Numeric, Integer, Boolean, Text, ForeignKey, CheckConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB, ENUM
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

# Create base class
Base = declarative_base()

# Define enum types to match PostgreSQL enums
VehicleStateEnum = ENUM(
    'DRIVING', 'IDLING', 'PARKED', 'REFUELING',
    name='vehicle_state',
    schema='telemetry'
)

AnomalyTypeEnum = ENUM(
    'FUEL_THEFT', 'ROUTE_DEVIATION', 'UNUSUAL_STOP',
    name='anomaly_type',
    schema='telemetry'
)

IngestionFormatEnum = ENUM(
    'SIMULATION_ENGINE', 'LEGACY', 'RAW_ARRAY', 'SINGLE_READING',
    name='ingestion_format',
    schema='telemetry'
)


class Vehicle(Base):
    """Vehicle master data model."""
    __tablename__ = 'vehicles'
    __table_args__ = {'schema': 'telemetry'}
    
    vehicle_id = Column(String(50), primary_key=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    tank_capacity_liters = Column(Numeric(8, 2), default=500.0)
    mileage_kmpl = Column(Numeric(6, 2), default=4.0)
    vehicle_type = Column(String(50), default='TRUCK')
    active = Column(Boolean, default=True)
    vehicle_metadata = Column(JSONB, default={})
    
    # Relationships
    telemetry_readings = relationship("TelemetryReading", back_populates="vehicle")
    anomaly_events = relationship("AnomalyEvent", back_populates="vehicle")
    hourly_kpis = relationship("HourlyVehicleKPI", back_populates="vehicle")
    
    def __repr__(self):
        return f"<Vehicle(id='{self.vehicle_id}', type='{self.vehicle_type}')>"


class TelemetryReading(Base):
    """Main telemetry reading model - TimescaleDB hypertable."""
    __tablename__ = 'telemetry_readings'
    __table_args__ = {'schema': 'telemetry'}
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    vehicle_id = Column(String(50), ForeignKey('telemetry.vehicles.vehicle_id'), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    latitude = Column(Numeric(10, 7), nullable=False)
    longitude = Column(Numeric(11, 7), nullable=False)
    speed_kph = Column(Numeric(6, 2), nullable=False)
    fuel_percentage = Column(Numeric(5, 2), nullable=False)
    
    # Processing metadata
    ingested_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    ingestion_format = Column(IngestionFormatEnum, nullable=False)
    batch_id = Column(Integer)
    processing_metadata = Column(JSONB, default={})
    
    # Constraints
    __table_args__ = (
        CheckConstraint('speed_kph >= 0', name='check_speed_non_negative'),
        CheckConstraint('fuel_percentage >= 0 AND fuel_percentage <= 100', name='check_fuel_percentage_range'),
        {'schema': 'telemetry'}
    )
    
    # Relationships
    vehicle = relationship("Vehicle", back_populates="telemetry_readings")
    
    def __repr__(self):
        return f"<TelemetryReading(vehicle='{self.vehicle_id}', timestamp='{self.timestamp}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            'id': str(self.id),
            'vehicle_id': self.vehicle_id,
            'timestamp': self.timestamp.isoformat(),
            'latitude': float(self.latitude),
            'longitude': float(self.longitude),
            'speed_kph': float(self.speed_kph),
            'fuel_percentage': float(self.fuel_percentage),
            'ingested_at': self.ingested_at.isoformat(),
            'ingestion_format': self.ingestion_format,
            'batch_id': self.batch_id
        }


class AnomalyEvent(Base):
    """Anomaly events model - TimescaleDB hypertable."""
    __tablename__ = 'anomaly_events'
    __table_args__ = {'schema': 'telemetry'}
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    vehicle_id = Column(String(50), ForeignKey('telemetry.vehicles.vehicle_id'), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    event_type = Column(AnomalyTypeEnum, nullable=False)
    confidence_score = Column(Numeric(4, 3))
    
    # Event details
    details = Column(JSONB, nullable=False, default={})
    
    # Processing metadata
    detected_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    processed = Column(Boolean, default=False)
    severity = Column(Integer, default=1)
    
    # Constraints
    __table_args__ = (
        CheckConstraint('confidence_score >= 0 AND confidence_score <= 1', name='check_confidence_range'),
        CheckConstraint('severity >= 1 AND severity <= 5', name='check_severity_range'),
        {'schema': 'telemetry'}
    )
    
    # Relationships
    vehicle = relationship("Vehicle", back_populates="anomaly_events")
    
    def __repr__(self):
        return f"<AnomalyEvent(vehicle='{self.vehicle_id}', type='{self.event_type}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            'id': str(self.id),
            'vehicle_id': self.vehicle_id,
            'timestamp': self.timestamp.isoformat(),
            'event_type': self.event_type,
            'confidence_score': float(self.confidence_score) if self.confidence_score else None,
            'details': self.details,
            'detected_at': self.detected_at.isoformat(),
            'processed': self.processed,
            'severity': self.severity
        }


class HourlyVehicleKPI(Base):
    """Hourly aggregated KPIs for vehicles - TimescaleDB hypertable."""
    __tablename__ = 'hourly_vehicle_kpis'
    __table_args__ = {'schema': 'telemetry'}
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    vehicle_id = Column(String(50), ForeignKey('telemetry.vehicles.vehicle_id'), nullable=False)
    hour_start = Column(DateTime(timezone=True), nullable=False)
    hour_end = Column(DateTime(timezone=True), nullable=False)
    
    # KPI metrics
    total_readings = Column(Integer, nullable=False, default=0)
    distance_km = Column(Numeric(10, 2), default=0)
    avg_speed_kph = Column(Numeric(6, 2), default=0)
    max_speed_kph = Column(Numeric(6, 2), default=0)
    idle_time_minutes = Column(Numeric(8, 2), default=0)
    utilization_percentage = Column(Numeric(5, 2), default=0)
    fuel_start_percentage = Column(Numeric(5, 2))
    fuel_end_percentage = Column(Numeric(5, 2))
    fuel_consumed_percentage = Column(Numeric(5, 2))
    anomaly_count = Column(Integer, default=0)
    
    # Processing metadata
    computed_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    
    # Relationships
    vehicle = relationship("Vehicle", back_populates="hourly_kpis")
    
    def __repr__(self):
        return f"<HourlyVehicleKPI(vehicle='{self.vehicle_id}', hour='{self.hour_start}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            'id': str(self.id),
            'vehicle_id': self.vehicle_id,
            'hour_start': self.hour_start.isoformat(),
            'hour_end': self.hour_end.isoformat(),
            'total_readings': self.total_readings,
            'distance_km': float(self.distance_km),
            'avg_speed_kph': float(self.avg_speed_kph),
            'max_speed_kph': float(self.max_speed_kph),
            'idle_time_minutes': float(self.idle_time_minutes),
            'utilization_percentage': float(self.utilization_percentage),
            'fuel_start_percentage': float(self.fuel_start_percentage) if self.fuel_start_percentage else None,
            'fuel_end_percentage': float(self.fuel_end_percentage) if self.fuel_end_percentage else None,
            'fuel_consumed_percentage': float(self.fuel_consumed_percentage) if self.fuel_consumed_percentage else None,
            'anomaly_count': self.anomaly_count,
            'computed_at': self.computed_at.isoformat()
        }


class IngestionBatch(Base):
    """Ingestion batch tracking model."""
    __tablename__ = 'ingestion_batches'
    __table_args__ = {'schema': 'telemetry'}
    
    batch_id = Column(Integer, primary_key=True)
    batch_timestamp = Column(DateTime(timezone=True))
    ingestion_format = Column(IngestionFormatEnum, nullable=False)
    total_readings = Column(Integer, nullable=False, default=0)
    processed_readings = Column(Integer, nullable=False, default=0)
    duplicate_readings = Column(Integer, nullable=False, default=0)
    error_readings = Column(Integer, nullable=False, default=0)
    processing_time_ms = Column(Numeric(10, 2))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    payload_metadata = Column(JSONB, default={})
    
    def __repr__(self):
        return f"<IngestionBatch(id={self.batch_id}, format='{self.ingestion_format}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            'batch_id': self.batch_id,
            'batch_timestamp': self.batch_timestamp.isoformat() if self.batch_timestamp else None,
            'ingestion_format': self.ingestion_format,
            'total_readings': self.total_readings,
            'processed_readings': self.processed_readings,
            'duplicate_readings': self.duplicate_readings,
            'error_readings': self.error_readings,
            'processing_time_ms': float(self.processing_time_ms) if self.processing_time_ms else None,
            'created_at': self.created_at.isoformat(),
            'payload_metadata': self.payload_metadata
        }