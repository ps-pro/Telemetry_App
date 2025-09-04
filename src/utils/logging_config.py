"""
Centralized logging configuration for the application.
"""
import logging
import logging.config
import sys
from typing import Dict, Any

from utils.config import get_settings


def get_logging_config() -> Dict[str, Any]:
    """Get logging configuration dictionary."""
    settings = get_settings()
    
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "[{levelname}] {asctime} - {name} - {message}",
                "style": "{",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": "[{levelname}] {asctime} - {name}:{lineno} - {funcName}() - {message}",
                "style": "{",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": settings.LOG_LEVEL,
                "formatter": "default",
                "stream": sys.stdout,
            },
            "file": {
                "class": "logging.FileHandler",
                "level": "INFO",
                "formatter": "detailed",
                "filename": "logs/app.log",
                "mode": "a",
                "encoding": "utf-8",
            },
        },
        "loggers": {
            "": {  # Root logger
                "level": settings.LOG_LEVEL,
                "handlers": ["console"],
                "propagate": False,
            },
            "uvicorn": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
            "fastapi": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
        },
    }


def setup_logging():
    """Setup application logging."""
    # Create logs directory if it doesn't exist
    import os
    os.makedirs("logs", exist_ok=True)
    
    # Configure logging
    config = get_logging_config()
    logging.config.dictConfig(config)
    
    # Test logging
    logger = logging.getLogger(__name__)
    logger.info("Logging configuration loaded successfully")


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)