"""Real-time streaming data generator with live anomaly triggering.

Continuously produces sensor, equipment, and weather data for all 16 zones
and publishes to Azure Event Hub for Fabric RTI ingestion.  Anomalies can
be triggered live — either programmatically via ``trigger()`` or from a
separate CLI process via a file-based IPC mechanism.

Thread safety
-------------
The generator loop runs in its own thread (or the caller's thread when
``blocking=True``).  ``trigger()`` and ``resolve()`` may be called from
any thread; all shared state is protected by ``AnomalyEngine._lock`` and
the generator's own ``_lock``.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.config import (
    GREENHOUSES,
    SENSOR_INTERVAL_SECONDS,
    get_adjacent_zones,
    get_zones_for_greenhouse,
)
from src.models.sensors import SensorGenerator
from src.models.weather import WeatherGenerator
from src.anomalies.engine import AnomalyEngine
from src.anomalies.scenarios import register_all, ALL_SCENARIOS
from src.outputs.eventhub_publisher import EventHubPublisher

logger = logging.getLogger(__name__)

# File-based IPC paths (written by the CLI, consumed by the loop)
TRIGGER_FILE = Path(".aganalytics-trigger.json")
RESOLVE_FILE = Path(".aganalytics-resolve.json")


class RealtimeGenerator:
    """Continuous real-time data generator with live anomaly triggering.

    Produces sensor readings every 30 seconds for all 16 zones,
    publishes to Event Hub for Fabric RTI ingestion.
    Anomalies can be triggered live via the ``trigger()`` method or
    by dropping a JSON trigger file that the run-loop picks up.
    """

    def __init__(
        self,
        connection_string: str | None = None,
        eventhub_name: str = "greenhouse-telemetry",
        seed: int = 42,
        speed: float = 1.0,  # 1.0 = real-time, 60.0 = 1 hour per minute
    ) -> None:
        self.speed = speed
        self._running = False
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

        # Sub-generators (deterministic seeds matching historical generator)
        self._weather = {
            "brightharvest": WeatherGenerator("brightharvest", seed=seed),
            "mucci-valley": WeatherGenerator("mucci-valley", seed=seed + 1),
        }
        self._sensors = SensorGenerator(seed=seed + 2)
        self._anomaly_engine = AnomalyEngine(seed=seed + 6)
        register_all(self._anomaly_engine)

        # Output
        self._publisher = EventHubPublisher(connection_string, eventhub_name)

        # Stats (guarded by _lock)
        self._stats: dict[str, int] = {
            "sensor_events": 0,
            "equipment_events": 0,
            "weather_events": 0,
            "ticks": 0,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, blocking: bool = True) -> None:
        """Start the real-time generator.

        Args:
            blocking: If True, runs in the calling thread (blocks until
                      ``stop()`` is called or KeyboardInterrupt).
                      If False, spawns a daemon background thread.
        """
        self._publisher.connect()
        self._running = True
        logger.info(
            "Starting real-time generator (speed=%.1fx, interval=%.1fs)",
            self.speed,
            SENSOR_INTERVAL_SECONDS / self.speed,
        )

        if blocking:
            self._run_loop()
        else:
            self._thread = threading.Thread(
                target=self._run_loop, name="realtime-gen", daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        """Stop the generator gracefully."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None
        self._publisher.close()
        logger.info("Real-time generator stopped.")

    @property
    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Live anomaly control (thread-safe)
    # ------------------------------------------------------------------

    def trigger(
        self,
        scenario: str,
        greenhouse_id: str = "brightharvest",
        zone_id: str = "BH-Z05",
        severity: float = 0.8,
        duration_minutes: int = 10,
    ) -> str:
        """Trigger a live anomaly scenario.

        Returns the ``anomaly_id`` assigned by the engine.
        """
        # Determine affected zones (primary + first adjacent for cascading)
        affected = [zone_id]
        if scenario in ("hvac-failure", "irrigation-failure"):
            try:
                adj = get_adjacent_zones(zone_id)
                if adj:
                    affected.append(adj[0].zone_id)
            except KeyError:
                pass  # unknown zone — just use the primary

        anomaly_id = self._anomaly_engine.trigger(
            scenario_name=scenario,
            greenhouse_id=greenhouse_id,
            zone_ids=affected,
            severity=severity,
            duration=timedelta(minutes=duration_minutes),
        )
        logger.info(
            "🔴 Triggered %s on %s (zones=%s, severity=%.1f, %d min): %s",
            scenario, greenhouse_id, affected, severity, duration_minutes, anomaly_id,
        )
        return anomaly_id

    def resolve(
        self,
        scenario: str | None = None,
        anomaly_id: str | None = None,
    ) -> list[str]:
        """Manually resolve active anomalies."""
        resolved = self._anomaly_engine.resolve(
            anomaly_id=anomaly_id, scenario_name=scenario,
        )
        if resolved:
            logger.info("✅ Resolved anomalies: %s", resolved)
        return resolved

    def status(self) -> dict[str, Any]:
        """Get current status including active anomalies and stats."""
        with self._lock:
            stats_copy = dict(self._stats)
        return {
            "running": self._running,
            "stats": stats_copy,
            "active_anomalies": self._anomaly_engine.get_active(),
            "registered_scenarios": list(ALL_SCENARIOS.keys()),
        }

    # ------------------------------------------------------------------
    # Main generation loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        """Main generation loop — runs until ``_running`` is set to False."""
        tick = 0
        sleep_interval = SENSOR_INTERVAL_SECONDS / self.speed

        while self._running:
            current = datetime.now(timezone.utc)

            try:
                # ---- File-based IPC: check for trigger file ----
                self._check_trigger_file(current)
                self._check_resolve_file()

                # ---- Weather (every 10th tick ≈ every 5 min) ----
                if tick % 10 == 0:
                    weather_events: list[dict] = []
                    for _gh_id, wg in self._weather.items():
                        wx = wg.generate(current)
                        weather_events.append(wx.to_dict())
                    self._publisher.publish(weather_events, "weather")
                    with self._lock:
                        self._stats["weather_events"] += len(weather_events)

                # ---- Sensor readings (every tick) ----
                weather_snapshot = {
                    gh: wg.generate(current) for gh, wg in self._weather.items()
                }
                readings = self._sensors.tick(current, weather_snapshot)

                sensor_events: list[dict] = []
                for reading in readings:
                    d = reading.to_dict()
                    d = self._anomaly_engine.apply(reading.zone_id, d, current)
                    sensor_events.append(d)

                self._publisher.publish(sensor_events, "sensor_telemetry")
                with self._lock:
                    self._stats["sensor_events"] += len(sensor_events)

                # ---- Equipment state (every 2nd tick ≈ every 60 s) ----
                if tick % 2 == 0:
                    equipment = self._sensors.get_equipment_states(current)
                    self._publisher.publish(
                        [e.to_dict() for e in equipment], "equipment_state",
                    )
                    with self._lock:
                        self._stats["equipment_events"] += len(equipment)

                # ---- Update tick counter ----
                with self._lock:
                    self._stats["ticks"] += 1
                tick += 1

                # ---- Periodic logging (every 120 ticks ≈ 60 min at 1×) ----
                if tick % 120 == 0:
                    active = self._anomaly_engine.get_active()
                    with self._lock:
                        s = dict(self._stats)
                    logger.info(
                        "Tick %d: %d sensor events, %d equipment, %d weather, "
                        "%d active anomalies",
                        tick, s["sensor_events"], s["equipment_events"],
                        s["weather_events"], len(active),
                    )

            except Exception:
                logger.error("Error in generation loop", exc_info=True)

            time.sleep(sleep_interval)

    # ------------------------------------------------------------------
    # File-based IPC helpers
    # ------------------------------------------------------------------

    def _check_trigger_file(self, current: datetime) -> None:
        """Pick up and process a trigger file written by the CLI."""
        if not TRIGGER_FILE.exists():
            return
        try:
            data = json.loads(TRIGGER_FILE.read_text(encoding="utf-8"))
            self.trigger(
                scenario=data["scenario"],
                greenhouse_id=data.get("greenhouse_id", "brightharvest"),
                zone_id=data.get("zone_id", "BH-Z05"),
                severity=data.get("severity", 0.8),
                duration_minutes=data.get("duration_minutes", 10),
            )
            TRIGGER_FILE.unlink(missing_ok=True)
        except Exception as exc:
            logger.error("Failed to process trigger file: %s", exc)
            # Remove corrupt file so we don't keep retrying
            TRIGGER_FILE.unlink(missing_ok=True)

    def _check_resolve_file(self) -> None:
        """Pick up and process a resolve file written by the CLI."""
        if not RESOLVE_FILE.exists():
            return
        try:
            data = json.loads(RESOLVE_FILE.read_text(encoding="utf-8"))
            self.resolve(
                scenario=data.get("scenario"),
                anomaly_id=data.get("anomaly_id"),
            )
            RESOLVE_FILE.unlink(missing_ok=True)
        except Exception as exc:
            logger.error("Failed to process resolve file: %s", exc)
            RESOLVE_FILE.unlink(missing_ok=True)
