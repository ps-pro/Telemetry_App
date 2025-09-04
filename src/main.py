"""
Database-integrated FastAPI application entry point.
Uses PostgreSQL + TimescaleDB for telemetry data persistence.
"""
import time
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

# Import database components
from database.connection import initialize_database, shutdown_database, get_db_session, get_database_manager
from database.models import Vehicle, TelemetryReading, AnomalyEvent

# Import routers
from api.database_ingestion import router as ingestion_router
from api.query import router as query_router
from utils.logging_config import setup_logging, get_logger
from utils.config import get_settings

from dotenv import load_dotenv
import os

# Force load .env file
load_dotenv('.env')
print(f"Forced load DATABASE_URL: {os.getenv('DATABASE_URL', 'NOT_SET')}")

print("DEBUG: DATABASE_URL =", get_settings().DATABASE_URL)

# Setup logging and configuration
setup_logging()
logger = get_logger(__name__)
settings = get_settings()


# Application lifespan management with database initialization
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown with database lifecycle management."""
    
    # Startup
    logger.info("Starting Telemetry Platform API with Database Integration...")
    start_time = datetime.now()
    
    try:
        # Initialize database connection and schema
        initialize_database()

        # Add this after: initialize_database()
        try:
            from database.connection import get_database_manager
            db_manager = get_database_manager()
            with db_manager.get_db_session() as session:
                result = session.execute(text("SELECT 1")).scalar()
                print(f"SUCCESS: Database connection test returned: {result}")
        except Exception as e:
            print(f"FAILED: Database connection test failed: {e}")



        logger.info("Database initialization completed successfully")
        
        # Verify database health
        db_manager = get_database_manager()
        health = db_manager.health_check()
        if health["status"] != "healthy":
            raise RuntimeError(f"Database health check failed: {health}")
        
        logger.info(f"Server started successfully at: {start_time}")
        logger.info(f"Database: {health['database_url']}")
        logger.info(f"Connection pool: {health['connection_pool']}")
        
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        raise
    
    yield  # App runs here
    
    # Shutdown
    logger.info("Shutting down Telemetry Platform API...")
    try:
        await shutdown_database()
        logger.info("Database shutdown completed")
    except Exception as e:
        logger.error(f"Shutdown error: {e}")


# Create FastAPI application
app = FastAPI(
    title="Telemetry Mini-Platform with Database",
    description="Vehicle telemetry ingestion and analytics platform with PostgreSQL + TimescaleDB",
    version="3.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Root endpoints
@app.get("/health", tags=["Health"])
async def health_check():
    """Main application health check."""
    db_manager = get_database_manager()
    db_health = db_manager.health_check()
    
    return {
        "status": "healthy" if db_health["status"] == "healthy" else "degraded",
        "timestamp": datetime.now().isoformat(),
        "service": "telemetry-platform",
        "version": "3.0.0",
        "database": {
            "status": db_health["status"],
            "connection_pool": db_health.get("connection_pool")
        },
        "features": [
            "database_persistence", 
            "timescale_hypertables", 
            "simulation_engine", 
            "multi_format_ingestion",
            "automatic_anomaly_detection"
        ]
    }


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with comprehensive API information."""
    return {
        "message": "Telemetry Mini-Platform API v3.0 - Database Edition",
        "version": "3.0.0",
        "description": "Production-ready vehicle telemetry platform with PostgreSQL + TimescaleDB",
        "documentation": "/docs",
        "health": "/health",
        "database_info": "/database/info",
        "endpoints": {
            "ingest": "POST /api/v1/ingest - Universal telemetry ingestion with database persistence",
            "ingest_stats": "GET /api/v1/ingest/stats - Real-time ingestion statistics from database",
            "ingest_health": "GET /api/v1/ingest/health - Ingestion system health with database connectivity",
            "ingest_debug": "GET /api/v1/ingest/debug - Debug information and database diagnostics",
            "stats": "GET /api/v1/stats/vehicle/{vehicle_id} - Vehicle analytics from time-series data",
            "alerts": "GET /api/v1/alerts - Anomaly events from database"
        },
        "database_features": [
            "TimescaleDB hypertables for time-series optimization",
            "Automatic data partitioning by time",
            "Built-in anomaly detection triggers",
            "Continuous aggregates for fast queries",
            "Data retention policies",
            "Connection pooling for high performance"
        ],
        "supported_formats": [
            "SimulationEngine: {timestamp, batch_size, telemetry_data}",
            "Legacy: {readings}",
            "Raw Array: [{...}, {...}]",
            "Single Reading: {vehicle_id, timestamp, ...}"
        ],
        "timestamp": datetime.now().isoformat()
    }


# Database information endpoint
@app.get("/database/info", tags=["Database"])
async def database_info():
    """Get detailed database information."""
    try:
        db_manager = get_database_manager()
        return {
            "health": db_manager.health_check(),
            "info": db_manager.get_database_info(),
            "features": {
                "timescaledb": "Time-series database with automatic partitioning",
                "hypertables": "Optimized for telemetry time-series data",
                "connection_pooling": "High-performance connection management",
                "triggers": "Automatic anomaly detection on data insert",
                "retention": "Automated data lifecycle management"
            }
        }
    except Exception as e:
        logger.error(f"Database info error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Database statistics endpoint
@app.get("/database/stats", tags=["Database"])
async def database_stats(session: Session = Depends(get_db_session)):
    """Get comprehensive database statistics."""
    try:
        # Get table counts
        vehicle_count = session.query(Vehicle).count()
        reading_count = session.query(TelemetryReading).count()
        anomaly_count = session.query(AnomalyEvent).count()
        
        # Get size information
        from sqlalchemy import text
        size_info = session.execute(text("""
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_tables 
            WHERE schemaname = 'telemetry'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """)).fetchall()
        
        # Get recent activity
        recent_activity = session.execute(text("""
            SELECT 
                DATE_TRUNC('hour', timestamp) as hour,
                COUNT(*) as reading_count
            FROM telemetry.telemetry_readings
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY DATE_TRUNC('hour', timestamp)
            ORDER BY hour DESC
            LIMIT 24
        """)).fetchall()
        
        return {
            "table_counts": {
                "vehicles": vehicle_count,
                "telemetry_readings": reading_count,
                "anomaly_events": anomaly_count
            },
            "table_sizes": [
                {
                    "table": f"{schema}.{table}",
                    "size": size,
                    "size_bytes": size_bytes
                }
                for schema, table, size, size_bytes in size_info
            ],
            "recent_activity_24h": [
                {
                    "hour": hour.isoformat() if hour else None,
                    "reading_count": count
                }
                for hour, count in recent_activity
            ],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Database stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# System information endpoint for debugging
@app.get("/system/info", tags=["System"])
async def system_info():
    """Comprehensive system information for monitoring and debugging."""
    
    # Get all routes
    routes_info = []
    for route in app.routes:
        if hasattr(route, 'methods') and hasattr(route, 'path'):
            routes_info.append({
                "path": route.path,
                "methods": list(route.methods) if route.methods else [],
                "name": getattr(route, 'name', None)
            })
    
    return {
        "service": "telemetry-platform",
        "version": "3.0.0",
        "timestamp": datetime.now().isoformat(),
        "configuration": {
            "database_url": settings.DATABASE_URL.split('@')[0] + "@***" if '@' in settings.DATABASE_URL else "***",
            "redis_url": settings.REDIS_URL,
            "debug_mode": settings.DEBUG,
            "log_level": settings.LOG_LEVEL
        },
        "total_routes": len(routes_info),
        "available_routes": routes_info,
        "key_endpoints": {
            "ingestion": "/api/v1/ingest",
            "ingestion_health": "/api/v1/ingest/health",
            "ingestion_stats": "/api/v1/ingest/stats",
            "ingestion_debug": "/api/v1/ingest/debug",
            "database_info": "/database/info",
            "database_stats": "/database/stats"
        },
        "monitoring": {
            "health_check": "/health",
            "system_info": "/system/info",
            "api_docs": "/docs"
        }
    }


# Include the routers
app.include_router(
    ingestion_router,
    prefix="/api/v1",
    tags=["Ingestion"]
)

app.include_router(
    query_router,
    prefix="/api/v1", 
    tags=["Query"]
)


if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting Telemetry Platform API Server with Database Integration...")
    logger.info("Features: PostgreSQL + TimescaleDB, Connection Pooling, Automatic Anomaly Detection")
    
    try:
        uvicorn.run(
            "main:app",
            host=settings.API_HOST,
            port=settings.API_PORT,
            workers=settings.API_WORKERS,
            reload=True,
            log_level=settings.LOG_LEVEL.lower()
        )
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server failed to start: {e}")
        raise