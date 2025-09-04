"""
Configuration management with fixed environment loading.
"""
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database Configuration - REMOVED DEFAULT VALUES TO FORCE ENV LOADING
    DATABASE_URL: str
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_TELEMETRY: str = "telemetry-raw"
    KAFKA_TOPIC_KPI: str = "kpi-computed"
    KAFKA_TOPIC_ANOMALY: str = "anomaly-detected"
    
    # API Configuration
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_WORKERS: int = 1
    
    # Celery Configuration
    CELERY_BROKER_URL: str = "redis://localhost:6379/2"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/3"
    
    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"
    
    # Cache Configuration
    CACHE_TTL_SECONDS: int = 300
    IDEMPOTENCY_TTL_SECONDS: int = 86400
    
    # Anomaly Detection Configuration
    FUEL_THEFT_THRESHOLD_PCT: float = 5.0
    GPS_JITTER_THRESHOLD_METERS: float = 100.0
    IDLE_TIME_THRESHOLD_MINUTES: float = 2.0
    
    # Development flags
    DEBUG: bool = False
    TESTING: bool = False
    
    class Config:
        # Look for .env in multiple locations
        env_file = [".env"]
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """
    Get application settings (cached).
    Manually load environment first to ensure proper loading.
    """
    # Manually try to load from different locations
    from dotenv import load_dotenv
    
    env_files = [".env", "../.env", "../../.env"]
    for env_file in env_files:
        if os.path.exists(env_file):
            print(f"Loading environment from: {os.path.abspath(env_file)}")
            load_dotenv(env_file, override=True)
            break
    else:
        print("Warning: No .env file found in expected locations")
        print("Checked:", [os.path.abspath(f) for f in env_files])
    
    # Print current DATABASE_URL for debugging
    db_url = os.getenv('DATABASE_URL', 'NOT_SET')
    print(f"DATABASE_URL from environment: {db_url}")
    
    return Settings()