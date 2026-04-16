"""Publish events to Azure Event Hub for Fabric RTI ingestion."""
from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


class EventHubPublisher:
    """Publishes sensor/equipment/weather events to Azure Event Hub.
    
    Events are serialized as JSON and sent as Event Hub messages.
    Supports batching for throughput.
    
    Connection string can be provided directly or via EVENTHUB_CONNECTION_STRING env var.
    """
    
    def __init__(
        self,
        connection_string: str | None = None,
        eventhub_name: str | None = None,
    ):
        self._connection_string = connection_string or os.environ.get("EVENTHUB_CONNECTION_STRING", "")
        self._eventhub_name = eventhub_name or os.environ.get("EVENTHUB_NAME", "greenhouse-telemetry")
        self._producer = None
        self._connected = False
    
    def connect(self) -> None:
        """Establish connection to Event Hub."""
        if not self._connection_string:
            logger.warning("No Event Hub connection string provided. Publishing will be simulated.")
            return
        try:
            from azure.eventhub import EventHubProducerClient
            self._producer = EventHubProducerClient.from_connection_string(
                conn_str=self._connection_string,
                eventhub_name=self._eventhub_name,
            )
            self._connected = True
            logger.info(f"Connected to Event Hub: {self._eventhub_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Event Hub: {e}")
            self._connected = False
    
    def publish(self, events: list[dict], event_type: str = "sensor") -> int:
        """Publish a batch of events.
        
        Args:
            events: List of dicts (already serialized via to_dict())
            event_type: Type label added to each event for routing
            
        Returns:
            Number of events published
        """
        if not events:
            return 0
        
        # Add event_type metadata to each event
        enriched = []
        for event in events:
            e = event.copy()
            e["event_type"] = event_type
            enriched.append(e)
        
        if not self._connected or not self._producer:
            logger.debug(f"[DRY RUN] Would publish {len(enriched)} {event_type} events")
            return len(enriched)
        
        try:
            from azure.eventhub import EventData
            batch = self._producer.create_batch()
            count = 0
            for event in enriched:
                event_data = EventData(json.dumps(event, default=str))
                try:
                    batch.add(event_data)
                    count += 1
                except ValueError:
                    # Batch is full, send and start new one
                    self._producer.send_batch(batch)
                    batch = self._producer.create_batch()
                    batch.add(event_data)
                    count += 1
            if count > 0:
                self._producer.send_batch(batch)
            logger.debug(f"Published {count} {event_type} events")
            return count
        except Exception as e:
            logger.error(f"Failed to publish events: {e}")
            return 0
    
    def publish_sensor_readings(self, readings: list) -> int:
        return self.publish([r.to_dict() for r in readings], "sensor_telemetry")
    
    def publish_weather(self, readings: list) -> int:
        return self.publish([r.to_dict() for r in readings], "weather")
    
    def publish_equipment(self, states: list) -> int:
        return self.publish([s.to_dict() for s in states], "equipment_state")
    
    def close(self) -> None:
        """Close the Event Hub connection."""
        if self._producer:
            self._producer.close()
            self._connected = False
            logger.info("Event Hub connection closed")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, *args):
        self.close()
