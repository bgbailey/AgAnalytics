"""Historical data generation (2-year backfill).

Orchestrates ALL generators to produce 2 years of realistic greenhouse data
(April 2024 – April 2026) and writes it to partitioned Parquet files.  This
is the seed data for the OneLake Bronze layer.

Coordination order per tick:
    weather → sensors → equipment → crops → energy → supply chain

Anomalies are injected from a deterministic pre-computed schedule so the
same seed always yields the same dataset.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from src.anomalies.engine import AnomalyEngine
from src.anomalies.scenarios import register_all
from src.anomalies.schedule import ScheduledAnomaly, generate_historical_schedule
from src.config import (
    HISTORICAL_END,
    HISTORICAL_START,
    ZONES,
    get_crop_for_zone,
    get_zones_for_greenhouse,
)
from src.models.crops import CropSimulator
from src.models.energy import EnergySimulator
from src.models.greenhouse import ZoneState
from src.models.sensors import SensorGenerator
from src.models.supply_chain import SupplyChainSimulator
from src.models.weather import WeatherGenerator
from src.outputs.parquet_writer import ParquetWriter

logger = logging.getLogger(__name__)


class HistoricalGenerator:
    """Orchestrates 2-year historical data generation.

    Coordinates weather → sensors → equipment → crops → energy → supply chain
    with anomaly injection from the scheduled timeline.
    """

    def __init__(self, output_dir: str | Path, seed: int = 42) -> None:
        self.output_dir = Path(output_dir)
        self.seed = seed

        # Initialize all sub-generators with deterministic seeds
        self._weather_generators = {
            "brightharvest": WeatherGenerator("brightharvest", seed=seed),
            "mucci-valley": WeatherGenerator("mucci-valley", seed=seed + 1),
        }
        self._sensor_generator = SensorGenerator(seed=seed + 2)
        self._crop_simulator = CropSimulator(seed=seed + 3)
        self._energy_simulator = EnergySimulator(seed=seed + 4)
        self._supply_chain = SupplyChainSimulator(seed=seed + 5)
        self._anomaly_engine = AnomalyEngine(seed=seed + 6)

        # Register anomaly profiles and load schedule
        register_all(self._anomaly_engine)
        self._anomaly_schedule: list[ScheduledAnomaly] = generate_historical_schedule(
            HISTORICAL_START.date(), HISTORICAL_END.date(), seed=seed + 7
        )

        # Output writer
        self._writer = ParquetWriter(self.output_dir)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        progress_callback: Callable[[datetime, float, dict[str, int]], None] | None = None,
    ) -> dict[str, int]:
        """Generate all historical data.

        Args:
            start: Override start (default: ``HISTORICAL_START``).
            end:   Override end (default: ``HISTORICAL_END``).
            progress_callback: Called with ``(current_time, total_seconds, stats_dict)``
                               once per simulated day.

        Returns:
            Row-count statistics per data type.
        """
        start = start or HISTORICAL_START
        end = end or HISTORICAL_END
        total_seconds = (end - start).total_seconds()

        # Initialize crop plantings at the start date
        self._crop_simulator.initialize_plantings(start.date())

        # Cumulative row counters (written + still buffered)
        row_counts: dict[str, int] = {
            "iot_telemetry": 0,
            "weather": 0,
            "equipment": 0,
            "daily_harvest": 0,
            "energy": 0,
            "shipments": 0,
        }

        # Pre-schedule anomaly triggers — walk through sorted schedule
        anomaly_idx = 0

        # Optional rich progress bar
        rich_progress = self._make_progress_bar(total_seconds)
        task_id = None
        if rich_progress is not None:
            task_id = rich_progress.add_task(
                "[green]Generating historical data…", total=total_seconds,
            )
            rich_progress.start()

        current = start
        wall_start = time.monotonic()

        try:
            while current < end:
                elapsed = (current - start).total_seconds()

                # --- Check if any scheduled anomalies should start ----------
                while anomaly_idx < len(self._anomaly_schedule):
                    scheduled = self._anomaly_schedule[anomaly_idx]
                    if scheduled.start_time <= current:
                        self._anomaly_engine.trigger(
                            scenario_name=scheduled.scenario_name,
                            greenhouse_id=scheduled.greenhouse_id,
                            zone_ids=scheduled.zone_ids,
                            severity=scheduled.severity,
                            duration=scheduled.duration,
                            start_time=scheduled.start_time,
                            auto_recover=scheduled.auto_recover,
                            metadata=scheduled.metadata or {},
                        )
                        anomaly_idx += 1
                    else:
                        break

                # --- Every 30 s: sensor readings ----------------------------
                weather = {
                    gh_id: wg.generate(current)
                    for gh_id, wg in self._weather_generators.items()
                }

                sensor_readings = self._sensor_generator.tick(current, weather)

                # Apply anomalies to sensor readings and write raw dicts
                for reading in sensor_readings:
                    reading_dict = reading.to_dict()
                    reading_dict = self._anomaly_engine.apply(
                        reading.zone_id, reading_dict, current,
                    )
                    self._writer._buffers["iot_telemetry"].append(reading_dict)
                row_counts["iot_telemetry"] += len(sensor_readings)

                if len(self._writer._buffers["iot_telemetry"]) >= self._writer._flush_threshold:
                    self._writer.flush("iot_telemetry")

                # --- Every 60 s: equipment state ----------------------------
                elapsed_int = int(elapsed)
                if elapsed_int % 60 == 0:
                    equipment = self._sensor_generator.get_equipment_states(current)
                    self._writer.write_equipment(equipment)
                    row_counts["equipment"] += len(equipment)

                # --- Every 300 s (5 min): weather ---------------------------
                if elapsed_int % 300 == 0:
                    for _gh_id, wx_reading in weather.items():
                        self._writer.write_weather([wx_reading])
                        row_counts["weather"] += 1

                # --- Every 3600 s (hourly): energy --------------------------
                if elapsed_int % 3600 == 0:
                    for gh_id in ("brightharvest", "mucci-valley"):
                        gh_zone_states = [
                            self._sensor_generator.get_zone_state(z.zone_id)
                            for z in get_zones_for_greenhouse(gh_id)
                        ]
                        energy = self._energy_simulator.generate_hourly(
                            current, gh_id, gh_zone_states, weather[gh_id],
                        )
                        self._writer.write_energy([energy])
                        row_counts["energy"] += 1

                # --- Daily at 06:00 UTC: harvests + supply chain ------------
                if current.hour == 6 and current.minute == 0 and current.second == 0:
                    zone_states = {
                        zid: self._sensor_generator.get_zone_state(zid)
                        for zid in ZONES
                    }
                    harvests = self._crop_simulator.generate_daily_harvest(
                        current.date(), zone_states,
                    )
                    if harvests:
                        self._writer.write_harvests(harvests)
                        row_counts["daily_harvest"] += len(harvests)

                    # Supply chain: shipments based on available harvest
                    for gh_id in ("brightharvest", "mucci-valley"):
                        available: dict[str, float] = {}
                        for h in harvests:
                            if h.greenhouse_id == gh_id:
                                available[h.crop_id] = (
                                    available.get(h.crop_id, 0.0) + h.harvest_weight_kg
                                )
                        if available:
                            shipments = self._supply_chain.generate_daily_shipments(
                                current.date(), gh_id, available,
                            )
                            if shipments:
                                self._writer.write_shipments(shipments)
                                row_counts["shipments"] += len(shipments)

                # --- Progress reporting -------------------------------------
                if elapsed_int > 0 and elapsed_int % 86400 == 0:
                    if progress_callback is not None:
                        progress_callback(current, total_seconds, dict(row_counts))

                if rich_progress is not None and task_id is not None:
                    rich_progress.update(task_id, completed=elapsed)

                # Advance time by 30 seconds
                current += timedelta(seconds=30)

        finally:
            if rich_progress is not None:
                rich_progress.stop()

        # Final flush of all remaining buffered data
        self._writer.flush_all()

        # Persist anomaly history to JSON for Gold-layer fact_anomaly_events
        anomaly_history = self._anomaly_engine.get_history()
        self._write_anomaly_history(anomaly_history)

        wall_elapsed = time.monotonic() - wall_start
        logger.info(
            "Historical generation complete in %.1f s — rows: %s",
            wall_elapsed, row_counts,
        )

        return row_counts

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _write_anomaly_history(self, history: list[dict[str, Any]]) -> None:
        """Write anomaly history to ``output_dir/anomaly_history.json``."""
        out_path = self.output_dir / "anomaly_history.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, default=str)
        logger.info("Anomaly history written to %s (%d events)", out_path, len(history))

    @staticmethod
    def _make_progress_bar(total_seconds: float) -> Any:
        """Return a ``rich.progress.Progress`` instance if rich is available."""
        try:
            from rich.progress import (
                BarColumn,
                MofNCompleteColumn,
                Progress,
                TextColumn,
                TimeElapsedColumn,
                TimeRemainingColumn,
            )

            return Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(bar_width=40),
                TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                transient=False,
            )
        except ImportError:
            return None


# ---------------------------------------------------------------------------
# CLI quick-test: generate 1 day and verify output
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import shutil
    import tempfile

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    test_dir = Path("historical_test_output")
    if test_dir.exists():
        shutil.rmtree(test_dir)

    one_day_start = datetime(2025, 1, 15, 0, 0, tzinfo=timezone.utc)
    one_day_end = one_day_start + timedelta(days=1)

    print("=" * 72)
    print("  HistoricalGenerator — 1-day smoke test")
    print(f"  Range: {one_day_start.date()} → {one_day_end.date()}")
    print(f"  Output: {test_dir.resolve()}")
    print("=" * 72)

    gen = HistoricalGenerator(output_dir=str(test_dir), seed=42)

    t0 = time.monotonic()
    stats = gen.generate(start=one_day_start, end=one_day_end)
    elapsed = time.monotonic() - t0

    print(f"\n  Completed in {elapsed:.1f}s")
    print("  Row counts:")
    for dtype, count in sorted(stats.items()):
        print(f"    {dtype:<20s} {count:>10,}")

    # Verify Parquet files were created
    parquet_files = list(test_dir.rglob("*.parquet"))
    json_files = list(test_dir.rglob("*.json"))
    print(f"\n  Parquet files created: {len(parquet_files)}")
    print(f"  JSON files created:   {len(json_files)}")

    if parquet_files:
        print("\n  Sample paths:")
        for pf in sorted(parquet_files)[:8]:
            print(f"    {pf.relative_to(test_dir)}")

    # Validate a sample file
    try:
        import pandas as pd

        sample = pd.read_parquet(parquet_files[0])
        print(f"\n  Sample file rows: {len(sample)}, columns: {list(sample.columns)[:6]}…")
    except Exception as exc:
        print(f"\n  (Could not read sample: {exc})")

    total = sum(stats.values())
    print(f"\n  Total rows: {total:,}")
    assert len(parquet_files) > 0, "No Parquet files were created!"
    assert stats["iot_telemetry"] > 0, "No sensor readings generated!"
    print("\n✓ Smoke test passed.")

    # Clean up
    shutil.rmtree(test_dir)
    print(f"  (Cleaned up {test_dir})")
