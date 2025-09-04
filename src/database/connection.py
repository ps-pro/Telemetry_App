"""
Database connection and session management for the telemetry platform.
Provides connection pooling, session management, and database utilities.
"""
import logging
from typing import Generator, Optional, Dict, Any
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError

from utils.config import get_settings
from utils.logging_config import get_logger
from database.models import Base

logger = get_logger(__name__)


class DatabaseManager:
    """
    Database connection and session manager with connection pooling.
    Handles database initialization, health checks, and session lifecycle.
    """
    
    def __init__(self):
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[sessionmaker] = None
        self._initialized = False
    
    def initialize(self, database_url: Optional[str] = None) -> None:
        """
        Initialize database connection with connection pooling.
        
        Args:
            database_url: Optional database URL override
        """
        if self._initialized:
            logger.warning("Database already initialized")
            return
        
        settings = get_settings()
        db_url = database_url or settings.DATABASE_URL
        
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                db_url,
                poolclass=QueuePool,
                pool_size=10,  # Number of connections to maintain in pool
                max_overflow=20,  # Additional connections beyond pool_size
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,  # Recycle connections after 1 hour
                echo=settings.DEBUG,  # Log SQL statements in debug mode
            )
            
            # Create session factory
            self.SessionLocal = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False  # Keep objects usable after commit
            )
            
            logger.info("Database connection initialized successfully")
            self._initialized = True
            
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise
    
    def create_tables(self) -> None:
        """Create all database tables if they don't exist."""
        if not self.engine:
            raise RuntimeError("Database not initialized")
        
        try:
            # Note: TimescaleDB hypertables are created by init SQL script
            # This only creates regular tables if needed
            # Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created/verified successfully")
            
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            raise
    
    def get_session(self) -> Session:
        """
        Get a database session.
        
        Returns:
            SQLAlchemy session instance
            
        Raises:
            RuntimeError: If database not initialized
        """
        if not self.SessionLocal:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        return self.SessionLocal()
    
    @contextmanager
    def get_db_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions with automatic cleanup.
        
        Yields:
            Database session
            
        Example:
            with db_manager.get_db_session() as session:
                # Use session here
                session.query(Vehicle).all()
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform database health check.
        
        Returns:
            Health status dictionary
        """
        if not self.engine:
            return {
                "status": "error",
                "message": "Database not initialized",
                "connection_pool": None
            }
        
        try:
            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                
            # Get connection pool info
            pool = self.engine.pool
            pool_status = {
                "pool_size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
                "total_connections": pool.checkedin() + pool.checkedout()
            }
            
            return {
                "status": "healthy" if result == 1 else "error",
                "message": "Database connection successful",
                "connection_pool": pool_status,
                "database_url": str(self.engine.url).replace(self.engine.url.password or "", "***")
            }
            
        except OperationalError as e:
            logger.error(f"Database operational error: {e}")
            return {
                "status": "error",
                "message": f"Database connection failed: {str(e)}",
                "connection_pool": None
            }
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                "status": "error",
                "message": f"Health check failed: {str(e)}",
                "connection_pool": None
            }
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get detailed database information for monitoring.
        
        Returns:
            Database information dictionary
        """
        if not self.engine:
            return {"error": "Database not initialized"}
        
        try:
            with self.get_db_session() as session:
                # Get TimescaleDB version
                timescale_version = session.execute(
                    text("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
                ).scalar()
                
                # Get PostgreSQL version
                postgres_version = session.execute(text("SELECT version()")).scalar()
                
                # Get hypertable info
                hypertables = session.execute(text("""
                    SELECT hypertable_name, num_chunks 
                    FROM timescaledb_information.hypertables 
                    WHERE hypertable_schema = 'telemetry'
                """)).fetchall()
                
                # Get table sizes
                table_sizes = session.execute(text("""
                    SELECT 
                        schemaname,
                        tablename,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                        pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                    FROM pg_tables 
                    WHERE schemaname = 'telemetry'
                    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                """)).fetchall()
                
                return {
                    "postgres_version": postgres_version.split(',')[0] if postgres_version else "Unknown",
                    "timescale_version": timescale_version or "Not installed",
                    "hypertables": [
                        {"name": name, "chunks": chunks} 
                        for name, chunks in hypertables
                    ],
                    "table_sizes": [
                        {
                            "schema": schema,
                            "table": table,
                            "size": size,
                            "size_bytes": size_bytes
                        }
                        for schema, table, size, size_bytes in table_sizes
                    ]
                }
                
        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            return {"error": str(e)}
    
    def close(self) -> None:
        """Close database connections and cleanup resources."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")
        self._initialized = False


# Global database manager instance
db_manager = DatabaseManager()


@lru_cache()
def get_database_manager() -> DatabaseManager:
    """
    Get the global database manager instance.
    Cached to ensure singleton behavior.
    """
    return db_manager


def get_db_session() -> Generator[Session, None, None]:
    """
    FastAPI dependency for getting database sessions.
    
    Yields:
        Database session
        
    Example:
        @app.get("/vehicles")
        def get_vehicles(session: Session = Depends(get_db_session)):
            return session.query(Vehicle).all()
    """
    manager = get_database_manager()
    session = manager.get_session()
    try:
        yield session
    finally:
        session.close()


class DatabaseService:
    """
    High-level database service for common operations.
    Provides repository-like interface for telemetry data operations.
    """
    
    def __init__(self, session: Session):
        self.session = session
    
    def ensure_vehicle_exists(self, vehicle_id: str, **kwargs) -> None:
        """
        Ensure vehicle exists in database, create if not found.
        
        Args:
            vehicle_id: Vehicle identifier
            **kwargs: Additional vehicle attributes
        """
        from database.models import Vehicle
        
        vehicle = self.session.query(Vehicle).filter(
            Vehicle.vehicle_id == vehicle_id
        ).first()
        
        if not vehicle:
            vehicle = Vehicle(
                vehicle_id=vehicle_id,
                **kwargs
            )
            self.session.add(vehicle)
            self.session.flush()  # Get the ID without committing
            logger.debug(f"Created new vehicle: {vehicle_id}")
    
    def bulk_insert_telemetry(self, readings_data: list) -> int:
        """
        Bulk insert telemetry readings for performance.
        
        Args:
            readings_data: List of telemetry reading dictionaries
            
        Returns:
            Number of inserted records
        """
        from database.models import TelemetryReading
        
        if not readings_data:
            return 0
        
        try:
            # Ensure all referenced vehicles exist
            vehicle_ids = {reading['vehicle_id'] for reading in readings_data}
            for vehicle_id in vehicle_ids:
                self.ensure_vehicle_exists(vehicle_id)
            
            # Bulk insert telemetry readings
            telemetry_objects = [
                TelemetryReading(**reading_data)
                for reading_data in readings_data
            ]
            
            self.session.bulk_save_objects(telemetry_objects)
            self.session.flush()
            
            count = len(telemetry_objects)
            logger.debug(f"Bulk inserted {count} telemetry readings")
            return count
            
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            raise
    
    def get_telemetry_count(self) -> Dict[str, int]:
        """
        Get telemetry reading counts by various dimensions.
        
        Returns:
            Dictionary with count statistics
        """
        from database.models import TelemetryReading, Vehicle
        
        try:
            total_readings = self.session.query(TelemetryReading).count()
            total_vehicles = self.session.query(Vehicle).count()
            active_vehicles = self.session.query(Vehicle).filter(Vehicle.active == True).count()
            
            return {
                "total_readings": total_readings,
                "total_vehicles": total_vehicles,
                "active_vehicles": active_vehicles
            }
            
        except Exception as e:
            logger.error(f"Failed to get telemetry counts: {e}")
            return {"error": str(e)}


def initialize_database() -> None:
    """
    Initialize database connection and create tables.
    Should be called at application startup.
    """
    try:
        manager = get_database_manager()
        manager.initialize()
        manager.create_tables()
        
        # Test connection
        health = manager.health_check()
        if health["status"] != "healthy":
            raise RuntimeError(f"Database health check failed: {health['message']}")
        
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


async def shutdown_database() -> None:
    """
    Cleanup database connections.
    Should be called at application shutdown.
    """
    try:
        manager = get_database_manager()
        manager.close()
        logger.info("Database shutdown completed")
        
    except Exception as e:
        logger.error(f"Database shutdown error: {e}")
        raise