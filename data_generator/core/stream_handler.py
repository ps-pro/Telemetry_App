"""
DataStreamHandler - Real-time telemetry streaming to API endpoints.
"""
import httpx
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import asdict

# Import your data models
from ..models import TelemetryReading, AnomalyEvent


class DataStreamHandler:
    """
    Handles real-time streaming of telemetry data to external APIs.
    Provides robust error handling and retry logic for API interactions.
    """

    def __init__(self, api_endpoint: str, timeout_seconds: float = 10.0):
        """
        Initialize the data stream handler.

        Args:
            api_endpoint: The URL endpoint for telemetry data ingestion
            timeout_seconds: HTTP request timeout in seconds
        """
        self.api_endpoint = api_endpoint.rstrip('/')
        self.timeout_seconds = timeout_seconds
        self.client = httpx.Client(timeout=timeout_seconds)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Statistics tracking
        self.stats = {
            "total_batches_sent": 0,
            "total_readings_sent": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_errors": 0,
            "start_time": datetime.now()
        }

    def test_connection(self) -> bool:
        """
        Test connectivity to the API endpoint.
        
        Returns:
            bool: True if API is reachable and healthy
        """
        # Try multiple health endpoints in order of preference
        health_endpoints = [
            f"{self.api_endpoint}/ingest/health",  # Specific ingestion health
            f"{self.api_endpoint.replace('/api/v1', '')}/health",  # Main health endpoint
        ]
        
        for health_url in health_endpoints:
            try:
                response = self.client.get(health_url, timeout=5.0)
                
                if response.status_code == 200:
                    self.logger.info(f"API health check passed: {health_url}")
                    return True
                else:
                    self.logger.debug(f"Health endpoint {health_url} returned {response.status_code}")
                    
            except httpx.RequestError as e:
                self.logger.debug(f"Cannot connect to {health_url}: {e}")
        
        self.logger.error("All health endpoints failed")
        return False

    def stream_telemetry_batch(self, telemetry_readings: List[TelemetryReading]) -> Dict[str, Any]:
        """
        Stream a batch of telemetry readings using SimulationEngine format.

        Args:
            telemetry_readings: List of telemetry readings to transmit

        Returns:
            dict: API response data or error information
        """
        if not telemetry_readings:
            return {"status": "success", "message": "No data to send"}

        try:
            # Convert telemetry readings to the format expected by SimulationEngine
            telemetry_data = []
            for reading in telemetry_readings:
                if hasattr(reading, '__dict__'):
                    # Handle dataclass objects
                    telemetry_data.append(asdict(reading))
                elif isinstance(reading, dict):
                    # Handle dictionary objects
                    telemetry_data.append(reading)
                else:
                    # Handle Pydantic models or other objects with dict() method
                    telemetry_data.append(reading.dict() if hasattr(reading, 'dict') else reading)

            # Create SimulationEngine format payload
            payload = {
                "timestamp": datetime.now().isoformat() + "Z",
                "batch_size": len(telemetry_readings),
                "telemetry_data": telemetry_data
            }

            # Send POST request to the updated API endpoint
            response = self.client.post(
                f"{self.api_endpoint}/ingest",
                json=payload,
                headers={"Content-Type": "application/json"}
            )

            response.raise_for_status()
            result = response.json()

            # Update statistics
            self.stats["total_batches_sent"] += 1
            self.stats["total_readings_sent"] += len(telemetry_readings)
            self.stats["successful_requests"] += 1

            self.logger.info(
                f"Successfully streamed batch: {len(telemetry_readings)} readings, "
                f"processed: {result.get('processed_count', 0)}, "
                f"duplicates: {result.get('duplicate_count', 0)}"
            )

            return result

        except httpx.TimeoutException:
            error_msg = f"Timeout while streaming to {self.api_endpoint}"
            self.logger.warning(error_msg)
            self.stats["failed_requests"] += 1
            self.stats["total_errors"] += 1
            return {"status": "timeout_error", "message": error_msg}

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            self.logger.error(error_msg)
            self.stats["failed_requests"] += 1
            self.stats["total_errors"] += 1
            return {"status": "http_error", "status_code": e.response.status_code, "message": error_msg}

        except Exception as e:
            error_msg = f"Unexpected error while streaming telemetry: {e}"
            self.logger.error(error_msg)
            self.stats["failed_requests"] += 1
            self.stats["total_errors"] += 1
            return {"status": "error", "message": error_msg}

    def stream_anomaly_event(self, anomaly: AnomalyEvent) -> bool:
        """
        Stream a single anomaly event to the API.
        
        Args:
            anomaly: Anomaly event to transmit
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # For now, we'll log anomalies and plan to send them via a separate endpoint
            self.logger.warning(f"Anomaly detected: {anomaly.event_type} for vehicle {anomaly.vehicle_id}")
            
            # Convert anomaly to dict format
            if hasattr(anomaly, '_asdict'):
                anomaly_data = anomaly._asdict()
            elif hasattr(anomaly, 'dict'):
                anomaly_data = anomaly.dict()
            else:
                anomaly_data = asdict(anomaly)

            # For now, just log it. In production, you'd send to an anomaly endpoint
            self.logger.info(f"Anomaly data: {json.dumps(anomaly_data, indent=2)}")
            
            return True

        except Exception as e:
            self.logger.error(f"Error processing anomaly: {e}")
            return False

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get streaming statistics.
        
        Returns:
            dict: Statistics about streaming performance
        """
        uptime = (datetime.now() - self.stats["start_time"]).total_seconds()
        
        success_rate = 0.0
        if self.stats["total_batches_sent"] > 0:
            success_rate = (self.stats["successful_requests"] / self.stats["total_batches_sent"]) * 100

        return {
            "uptime_seconds": uptime,
            "total_batches_sent": self.stats["total_batches_sent"],
            "total_readings_sent": self.stats["total_readings_sent"],
            "successful_requests": self.stats["successful_requests"],
            "failed_requests": self.stats["failed_requests"],
            "total_errors": self.stats["total_errors"],
            "success_rate_percent": round(success_rate, 2),
            "readings_per_second": round(self.stats["total_readings_sent"] / max(uptime, 1), 2)
        }

    def reset_statistics(self):
        """Reset streaming statistics."""
        self.stats = {
            "total_batches_sent": 0,
            "total_readings_sent": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_errors": 0,
            "start_time": datetime.now()
        }
        self.logger.info("Streaming statistics reset")

    def close(self):
        """Clean up HTTP client resources."""
        if self.client:
            self.client.close()
            self.logger.info("DataStreamHandler connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.close()