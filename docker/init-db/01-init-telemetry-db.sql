-- Initialize telemetry database with TimescaleDB extension
-- Fixed version with correct SQL syntax

-- Connect to the telemetry database
\c telemetry_db;

-- Create TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create PostGIS extension for geospatial data (optional)
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create UUID extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schema for telemetry data
CREATE SCHEMA IF NOT EXISTS telemetry;

-- Set search path
SET search_path TO telemetry, public;

-- Create enum types
CREATE TYPE telemetry.vehicle_state AS ENUM ('DRIVING', 'IDLING', 'PARKED', 'REFUELING');
CREATE TYPE telemetry.anomaly_type AS ENUM ('FUEL_THEFT', 'ROUTE_DEVIATION', 'UNUSUAL_STOP');
CREATE TYPE telemetry.ingestion_format AS ENUM ('SIMULATION_ENGINE', 'LEGACY', 'RAW_ARRAY', 'SINGLE_READING');

-- Create vehicles table (master data)
CREATE TABLE IF NOT EXISTS telemetry.vehicles (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tank_capacity_liters DECIMAL(8,2) DEFAULT 500.0,
    mileage_kmpl DECIMAL(6,2) DEFAULT 4.0,
    vehicle_type VARCHAR(50) DEFAULT 'TRUCK',
    active BOOLEAN DEFAULT TRUE,
    vehicle_metadata JSONB DEFAULT '{}'::jsonb
);

-- Create telemetry_readings table (main time-series data)
CREATE TABLE IF NOT EXISTS telemetry.telemetry_readings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vehicle_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    latitude DECIMAL(10, 7) NOT NULL,
    longitude DECIMAL(11, 7) NOT NULL,
    speed_kph DECIMAL(6, 2) NOT NULL CHECK (speed_kph >= 0),
    fuel_percentage DECIMAL(5, 2) NOT NULL CHECK (fuel_percentage >= 0 AND fuel_percentage <= 100),
    
    -- Processing metadata
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingestion_format telemetry.ingestion_format NOT NULL,
    batch_id INTEGER,
    processing_metadata JSONB DEFAULT '{}'::jsonb,
    
    -- Constraints
    CONSTRAINT fk_vehicle FOREIGN KEY (vehicle_id) REFERENCES telemetry.vehicles(vehicle_id),
    CONSTRAINT unique_vehicle_timestamp UNIQUE (vehicle_id, timestamp)
);

-- Convert telemetry_readings to hypertable (TimescaleDB time-series optimization)
SELECT create_hypertable('telemetry.telemetry_readings', 'timestamp', 
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Create spatial index for location-based queries
CREATE INDEX IF NOT EXISTS idx_telemetry_location 
ON telemetry.telemetry_readings USING GIST (ST_Point(longitude, latitude));

-- Create additional indexes for common queries
CREATE INDEX IF NOT EXISTS idx_telemetry_vehicle_id 
ON telemetry.telemetry_readings (vehicle_id);

CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp 
ON telemetry.telemetry_readings (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_vehicle_timestamp 
ON telemetry.telemetry_readings (vehicle_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_speed 
ON telemetry.telemetry_readings (speed_kph) WHERE speed_kph = 0;

-- Create anomaly_events table
CREATE TABLE IF NOT EXISTS telemetry.anomaly_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vehicle_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    event_type telemetry.anomaly_type NOT NULL,
    confidence_score DECIMAL(4, 3) CHECK (confidence_score >= 0 AND confidence_score <= 1),
    
    -- Event details
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    -- Processing metadata
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    severity INTEGER DEFAULT 1 CHECK (severity >= 1 AND severity <= 5),
    
    -- Constraints
    CONSTRAINT fk_anomaly_vehicle FOREIGN KEY (vehicle_id) REFERENCES telemetry.vehicles(vehicle_id)
);

-- Convert anomaly_events to hypertable
SELECT create_hypertable('telemetry.anomaly_events', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Create indexes for anomaly events
CREATE INDEX IF NOT EXISTS idx_anomaly_vehicle_id 
ON telemetry.anomaly_events (vehicle_id);

CREATE INDEX IF NOT EXISTS idx_anomaly_timestamp 
ON telemetry.anomaly_events (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_anomaly_type 
ON telemetry.anomaly_events (event_type);

-- Create hourly KPI aggregation table
CREATE TABLE IF NOT EXISTS telemetry.hourly_vehicle_kpis (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vehicle_id VARCHAR(50) NOT NULL,
    hour_start TIMESTAMPTZ NOT NULL,
    hour_end TIMESTAMPTZ NOT NULL,
    
    -- KPI metrics
    total_readings INTEGER NOT NULL DEFAULT 0,
    distance_km DECIMAL(10, 2) DEFAULT 0,
    avg_speed_kph DECIMAL(6, 2) DEFAULT 0,
    max_speed_kph DECIMAL(6, 2) DEFAULT 0,
    idle_time_minutes DECIMAL(8, 2) DEFAULT 0,
    utilization_percentage DECIMAL(5, 2) DEFAULT 0,
    fuel_start_percentage DECIMAL(5, 2),
    fuel_end_percentage DECIMAL(5, 2),
    fuel_consumed_percentage DECIMAL(5, 2),
    anomaly_count INTEGER DEFAULT 0,
    
    -- Processing metadata
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT fk_kpi_vehicle FOREIGN KEY (vehicle_id) REFERENCES telemetry.vehicles(vehicle_id),
    CONSTRAINT unique_vehicle_hour UNIQUE (vehicle_id, hour_start)
);

-- Convert KPI table to hypertable
SELECT create_hypertable('telemetry.hourly_vehicle_kpis', 'hour_start',
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE
);

-- Create ingestion_batches table for tracking batch processing
CREATE TABLE IF NOT EXISTS telemetry.ingestion_batches (
    batch_id SERIAL PRIMARY KEY,
    batch_timestamp TIMESTAMPTZ,
    ingestion_format telemetry.ingestion_format NOT NULL,
    total_readings INTEGER NOT NULL DEFAULT 0,
    processed_readings INTEGER NOT NULL DEFAULT 0,
    duplicate_readings INTEGER NOT NULL DEFAULT 0,
    error_readings INTEGER NOT NULL DEFAULT 0,
    processing_time_ms DECIMAL(10, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_metadata JSONB DEFAULT '{}'::jsonb
);

-- Insert some default vehicles for testing
INSERT INTO telemetry.vehicles (vehicle_id, tank_capacity_liters, mileage_kmpl, vehicle_type, vehicle_metadata) 
VALUES 
    ('TEST-STREAM-001', 500.0, 4.0, 'TRUCK', '{"profile": "test", "created_by": "system"}'::jsonb),
    ('TEST-STREAM-002', 500.0, 4.0, 'TRUCK', '{"profile": "test", "created_by": "system"}'::jsonb)
ON CONFLICT (vehicle_id) DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA telemetry TO telemetry_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA telemetry TO telemetry_user;

-- Display setup completion message
\echo 'TimescaleDB telemetry database setup completed successfully!'
\echo 'Created schema: telemetry'
\echo 'Created hypertables: telemetry_readings, anomaly_events, hourly_vehicle_kpis'