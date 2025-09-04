-- TimescaleDB Telemetry Database Initialization
-- Clean version with no PostGIS dependencies

\c telemetry_db;

-- Core extensions only
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schema
CREATE SCHEMA IF NOT EXISTS telemetry;
SET search_path TO telemetry, public;

-- Enum types
CREATE TYPE telemetry.vehicle_state AS ENUM ('DRIVING', 'IDLING', 'PARKED', 'REFUELING');
CREATE TYPE telemetry.anomaly_type AS ENUM ('FUEL_THEFT', 'ROUTE_DEVIATION', 'UNUSUAL_STOP');
CREATE TYPE telemetry.ingestion_format AS ENUM ('SIMULATION_ENGINE', 'LEGACY', 'RAW_ARRAY', 'SINGLE_READING');

-- Tables
CREATE TABLE telemetry.vehicles (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    tank_capacity_liters DECIMAL(8,2) DEFAULT 500.0,
    mileage_kmpl DECIMAL(6,2) DEFAULT 4.0,
    vehicle_type VARCHAR(50) DEFAULT 'TRUCK',
    active BOOLEAN DEFAULT TRUE,
    vehicle_metadata JSONB DEFAULT '{}'
);

CREATE TABLE telemetry.telemetry_readings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vehicle_id VARCHAR(50) NOT NULL REFERENCES telemetry.vehicles(vehicle_id),
    timestamp TIMESTAMPTZ NOT NULL,
    latitude DECIMAL(10, 7) NOT NULL,
    longitude DECIMAL(11, 7) NOT NULL,
    speed_kph DECIMAL(6, 2) NOT NULL CHECK (speed_kph >= 0),
    fuel_percentage DECIMAL(5, 2) NOT NULL CHECK (fuel_percentage >= 0 AND fuel_percentage <= 100),
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    ingestion_format telemetry.ingestion_format NOT NULL,
    batch_id INTEGER,
    processing_metadata JSONB DEFAULT '{}',
    UNIQUE(vehicle_id, timestamp)
);

CREATE TABLE telemetry.anomaly_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vehicle_id VARCHAR(50) NOT NULL REFERENCES telemetry.vehicles(vehicle_id),
    timestamp TIMESTAMPTZ NOT NULL,
    event_type telemetry.anomaly_type NOT NULL,
    confidence_score DECIMAL(4, 3) CHECK (confidence_score >= 0 AND confidence_score <= 1),
    details JSONB DEFAULT '{}',
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    severity INTEGER DEFAULT 1 CHECK (severity >= 1 AND severity <= 5)
);

CREATE TABLE telemetry.ingestion_batches (
    batch_id SERIAL PRIMARY KEY,
    batch_timestamp TIMESTAMPTZ,
    ingestion_format telemetry.ingestion_format NOT NULL,
    total_readings INTEGER DEFAULT 0,
    processed_readings INTEGER DEFAULT 0,
    duplicate_readings INTEGER DEFAULT 0,
    error_readings INTEGER DEFAULT 0,
    processing_time_ms DECIMAL(10, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    payload_metadata JSONB DEFAULT '{}'
);

-- Convert to hypertables
SELECT create_hypertable('telemetry.telemetry_readings', 'timestamp', if_not_exists => TRUE);
SELECT create_hypertable('telemetry.anomaly_events', 'timestamp', if_not_exists => TRUE);

-- Indexes
CREATE INDEX idx_telemetry_vehicle_id ON telemetry.telemetry_readings (vehicle_id);
CREATE INDEX idx_telemetry_timestamp ON telemetry.telemetry_readings (timestamp DESC);
CREATE INDEX idx_anomaly_vehicle_id ON telemetry.anomaly_events (vehicle_id);

-- Test data
INSERT INTO telemetry.vehicles (vehicle_id, vehicle_metadata) 
VALUES 
    ('TEST-STREAM-001', '{"profile": "test"}'::jsonb),
    ('TEST-STREAM-002', '{"profile": "test"}'::jsonb)
ON CONFLICT (vehicle_id) DO NOTHING;

-- Permissions
GRANT ALL ON SCHEMA telemetry TO telemetry_user;
GRANT ALL ON ALL TABLES IN SCHEMA telemetry TO telemetry_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA telemetry TO telemetry_user;

\echo 'Database initialization completed successfully!';
