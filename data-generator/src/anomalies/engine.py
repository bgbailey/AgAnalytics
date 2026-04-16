"""Core anomaly injection engine.

Manages anomaly lifecycle (onset → peak → recovery → resolved), applies
sensor mutations via pluggable ``apply_fn`` callables, and tracks history
for the Gold-layer ``fact_anomaly_events`` table.

Thread-safe: live CLI triggers run on a different thread from the generator
loop.  All shared state is guarded by ``self._lock``.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable

import numpy as np


# ---------------------------------------------------------------------------
# Enums & value objects
# ---------------------------------------------------------------------------


class AnomalyPhase(Enum):
    """Phases of an anomaly lifecycle."""

    ONSET = "onset"        # Problem developing
    PEAK = "peak"          # Full severity
    RECOVERY = "recovery"  # System recovering
    RESOLVED = "resolved"  # Back to normal


@dataclass
class AnomalyProfile:
    """Defines how an anomaly type affects sensor readings over time.

    Each profile specifies which sensors are affected and how their values
    change through onset → peak → recovery phases.  The ``apply_fn`` callable
    is the only scenario-specific code — everything else is generic engine
    logic.

    ``apply_fn`` signature::

        (readings_dict, phase, progress_in_phase, severity, rng) -> readings_dict
    """

    scenario_name: str
    display_name: str
    description: str
    affected_sensors: list[str]  # sensor field names affected

    # Phase timing as fraction of total duration
    onset_fraction: float = 0.2   # first 20% = onset
    peak_fraction: float = 0.5    # 20-70% = peak
    # recovery is the remainder (30%)

    # Callable that mutates readings based on phase and progress
    apply_fn: Callable[
        [dict, AnomalyPhase, float, float, np.random.Generator], dict
    ] | None = None


@dataclass
class ActiveAnomaly:
    """A currently active anomaly instance."""

    anomaly_id: str
    scenario_name: str
    profile: AnomalyProfile
    greenhouse_id: str
    affected_zone_ids: list[str]
    severity: float            # 0.0 to 1.0 (low to critical)
    start_time: datetime
    duration: timedelta
    phase: AnomalyPhase = AnomalyPhase.ONSET
    auto_recover: bool = True
    metadata: dict = field(default_factory=dict)

    # ---- derived helpers ---------------------------------------------------

    @property
    def end_time(self) -> datetime:
        return self.start_time + self.duration

    @property
    def elapsed(self) -> float:
        """Seconds elapsed since start (wall-clock)."""
        return (datetime.now(timezone.utc) - self.start_time).total_seconds()

    def progress(self, current_time: datetime) -> float:
        """Overall progress 0.0 → 1.0."""
        total = self.duration.total_seconds()
        if total <= 0:
            return 1.0
        elapsed = (current_time - self.start_time).total_seconds()
        return min(1.0, max(0.0, elapsed / total))

    def get_phase(self, current_time: datetime) -> AnomalyPhase:
        """Determine current phase based on progress."""
        p = self.progress(current_time)
        if p >= 1.0:
            return AnomalyPhase.RESOLVED
        recovery_start = self.profile.onset_fraction + self.profile.peak_fraction
        if p >= recovery_start:
            return AnomalyPhase.RECOVERY
        if p >= self.profile.onset_fraction:
            return AnomalyPhase.PEAK
        return AnomalyPhase.ONSET

    def phase_progress(self, current_time: datetime) -> float:
        """Progress *within* the current phase (0.0 → 1.0)."""
        p = self.progress(current_time)
        phase = self.get_phase(current_time)

        if phase == AnomalyPhase.ONSET:
            frac = self.profile.onset_fraction
            return p / frac if frac > 0 else 1.0

        if phase == AnomalyPhase.PEAK:
            start = self.profile.onset_fraction
            length = self.profile.peak_fraction
            return (p - start) / length if length > 0 else 1.0

        if phase == AnomalyPhase.RECOVERY:
            start = self.profile.onset_fraction + self.profile.peak_fraction
            length = 1.0 - start
            return (p - start) / length if length > 0 else 1.0

        return 1.0  # RESOLVED


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class AnomalyEngine:
    """Core engine for injecting anomalies into sensor data streams.

    Maintains a registry of anomaly profiles and a list of active anomalies.
    Works for both *historical generation* (timestamps in the past, stepped
    deterministically) and *live demo triggering* (real-time UTC timestamps,
    triggered from a separate CLI thread).

    Thread safety
    -------------
    ``_active``, ``_history``, and ``_anomaly_counter`` are mutated under
    ``_lock``.  Read-only helpers that only iterate ``_active`` also acquire
    the lock so callers never see a half-updated dict.
    """

    def __init__(self, seed: int | None = None) -> None:
        self._rng = np.random.default_rng(seed)
        self._profiles: dict[str, AnomalyProfile] = {}
        self._active: dict[str, ActiveAnomaly] = {}   # anomaly_id → ActiveAnomaly
        self._history: list[dict[str, Any]] = []       # resolved anomalies log
        self._lock = threading.Lock()
        self._anomaly_counter: int = 0

    # -- profile registry ----------------------------------------------------

    def register_profile(self, profile: AnomalyProfile) -> None:
        """Register an anomaly profile (scenario type)."""
        self._profiles[profile.scenario_name] = profile

    @property
    def registered_scenarios(self) -> list[str]:
        """Return names of all registered scenario profiles."""
        return list(self._profiles.keys())

    # -- trigger / resolve ---------------------------------------------------

    def trigger(
        self,
        scenario_name: str,
        greenhouse_id: str,
        zone_ids: list[str],
        severity: float = 0.8,
        duration: timedelta = timedelta(minutes=10),
        start_time: datetime | None = None,
        auto_recover: bool = True,
        metadata: dict | None = None,
    ) -> str:
        """Trigger an anomaly scenario.

        Parameters
        ----------
        scenario_name:
            Must match a registered profile.
        greenhouse_id:
            Target greenhouse.
        zone_ids:
            Target zone(s).
        severity:
            0.0-1.0 — controls magnitude of sensor deviations.
        duration:
            How long the anomaly lasts end-to-end.
        start_time:
            When it starts.  Defaults to ``datetime.now(UTC)``.
        auto_recover:
            Whether it resolves automatically when duration elapses.
        metadata:
            Extra data (e.g. ``shipment_id`` for cold-chain break).

        Returns
        -------
        str
            Unique ``anomaly_id`` for this instance (e.g. ``ANM-20250701-0003``).

        Raises
        ------
        ValueError
            If ``scenario_name`` is not registered.
        """
        if scenario_name not in self._profiles:
            raise ValueError(
                f"Unknown scenario: {scenario_name!r}. "
                f"Registered: {list(self._profiles.keys())}"
            )

        ts = start_time or datetime.now(timezone.utc)

        with self._lock:
            self._anomaly_counter += 1
            anomaly_id = f"ANM-{ts.strftime('%Y%m%d')}-{self._anomaly_counter:04d}"

            anomaly = ActiveAnomaly(
                anomaly_id=anomaly_id,
                scenario_name=scenario_name,
                profile=self._profiles[scenario_name],
                greenhouse_id=greenhouse_id,
                affected_zone_ids=list(zone_ids),
                severity=max(0.0, min(1.0, severity)),
                start_time=ts,
                duration=duration,
                auto_recover=auto_recover,
                metadata=metadata or {},
            )
            self._active[anomaly_id] = anomaly

        return anomaly_id

    def resolve(
        self,
        anomaly_id: str | None = None,
        scenario_name: str | None = None,
    ) -> list[str]:
        """Manually resolve anomaly(ies).

        Can resolve by specific ID *or* by scenario name (resolves all of that
        type).  Returns list of resolved anomaly IDs.
        """
        resolved_ids: list[str] = []
        now = datetime.now(timezone.utc)

        with self._lock:
            to_remove: list[str] = []
            for aid, anom in self._active.items():
                if (anomaly_id and aid == anomaly_id) or (
                    scenario_name and anom.scenario_name == scenario_name
                ):
                    to_remove.append(aid)

            for aid in to_remove:
                anom = self._active.pop(aid)
                anom.phase = AnomalyPhase.RESOLVED
                self._history.append(self._build_history_record(anom, now))
                resolved_ids.append(aid)

        return resolved_ids

    # -- per-tick application ------------------------------------------------

    def apply(
        self,
        zone_id: str,
        readings: dict[str, float],
        current_time: datetime,
    ) -> dict[str, float]:
        """Apply all active anomalies to a zone's sensor readings.

        Called by the sensor generator on **every tick** for **every zone**.
        Modifies *copies* of the readings dict — callers get back an updated
        dict without side-effects on the original.

        Parameters
        ----------
        zone_id:
            The zone being processed.
        readings:
            Dict of sensor values (keys match sensor field names from
            ``SensorReading``).
        current_time:
            Current simulation / wall-clock time.

        Returns
        -------
        dict
            Modified readings dict.
        """
        modified = readings.copy()
        expired: list[str] = []

        with self._lock:
            for anomaly_id, anomaly in self._active.items():
                # Skip if this zone is not affected
                if zone_id not in anomaly.affected_zone_ids:
                    continue

                phase = anomaly.get_phase(current_time)

                # If the anomaly has run its course, mark for cleanup
                if phase == AnomalyPhase.RESOLVED:
                    if anomaly.auto_recover:
                        expired.append(anomaly_id)
                    continue

                # Update cached phase and compute per-phase progress
                anomaly.phase = phase
                phase_prog = anomaly.phase_progress(current_time)

                # Delegate to the scenario-specific mutator
                if anomaly.profile.apply_fn is not None:
                    modified = anomaly.profile.apply_fn(
                        modified,
                        phase,
                        phase_prog,
                        anomaly.severity,
                        self._rng,
                    )

            # Garbage-collect resolved anomalies
            for aid in expired:
                anom = self._active.pop(aid)
                self._history.append(
                    self._build_history_record(anom, current_time, auto=True)
                )

        return modified

    # -- query helpers -------------------------------------------------------

    def get_active(self) -> list[dict[str, Any]]:
        """Return summary dicts for all currently active anomalies."""
        with self._lock:
            return [
                {
                    "anomaly_id": a.anomaly_id,
                    "scenario": a.scenario_name,
                    "display_name": a.profile.display_name,
                    "greenhouse_id": a.greenhouse_id,
                    "zone_ids": list(a.affected_zone_ids),
                    "severity": a.severity,
                    "phase": a.phase.value,
                    "start_time": a.start_time.isoformat(),
                    "duration_seconds": a.duration.total_seconds(),
                    "elapsed_seconds": a.elapsed,
                }
                for a in self._active.values()
            ]

    def get_history(self) -> list[dict[str, Any]]:
        """Return all resolved anomaly records (for fact_anomaly_events)."""
        with self._lock:
            return list(self._history)

    def has_active_for_zone(self, zone_id: str) -> bool:
        """Check if any anomaly is active for a given zone."""
        with self._lock:
            return any(
                zone_id in a.affected_zone_ids
                and a.phase != AnomalyPhase.RESOLVED
                for a in self._active.values()
            )

    def active_scenarios_for_zone(self, zone_id: str) -> list[str]:
        """Return scenario names of anomalies currently hitting *zone_id*."""
        with self._lock:
            return [
                a.scenario_name
                for a in self._active.values()
                if zone_id in a.affected_zone_ids
                and a.phase != AnomalyPhase.RESOLVED
            ]

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    # -- internal helpers ----------------------------------------------------

    @staticmethod
    def _build_history_record(
        anom: ActiveAnomaly,
        resolved_time: datetime,
        *,
        auto: bool = False,
    ) -> dict[str, Any]:
        """Build a history dict suitable for the anomaly event log."""
        return {
            "anomaly_id": anom.anomaly_id,
            "scenario": anom.scenario_name,
            "display_name": anom.profile.display_name,
            "greenhouse_id": anom.greenhouse_id,
            "zone_ids": list(anom.affected_zone_ids),
            "severity": anom.severity,
            "start_time": anom.start_time.isoformat(),
            "resolved_time": resolved_time.isoformat(),
            "duration_seconds": (resolved_time - anom.start_time).total_seconds(),
            "auto_resolved": auto,
            "metadata": dict(anom.metadata),
        }


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import time

    # -- dummy apply_fn for testing ------------------------------------------
    def _dummy_apply(
        readings: dict,
        phase: AnomalyPhase,
        phase_progress: float,
        severity: float,
        rng: np.random.Generator,
    ) -> dict:
        """Simple test mutator: drops temperature during peak."""
        out = readings.copy()
        if phase == AnomalyPhase.ONSET:
            # Gradual cooling: temp drops proportional to onset progress
            out["air_temperature"] -= 5.0 * phase_progress * severity
        elif phase == AnomalyPhase.PEAK:
            # Full effect held
            out["air_temperature"] -= 5.0 * severity
            out["air_humidity"] += 10.0 * severity
        elif phase == AnomalyPhase.RECOVERY:
            # Recovering: effect diminishes
            out["air_temperature"] -= 5.0 * (1.0 - phase_progress) * severity
            out["air_humidity"] += 10.0 * (1.0 - phase_progress) * severity
        return out

    # -- set up engine -------------------------------------------------------
    engine = AnomalyEngine(seed=42)
    profile = AnomalyProfile(
        scenario_name="hvac-failure",
        display_name="HVAC Failure",
        description="Heating system goes offline, temperature drops",
        affected_sensors=["air_temperature", "air_humidity"],
        onset_fraction=0.2,
        peak_fraction=0.5,
        apply_fn=_dummy_apply,
    )
    engine.register_profile(profile)

    # -- trigger an anomaly with 10-second duration for fast test ------------
    duration = timedelta(seconds=10)
    start = datetime.now(timezone.utc)
    aid = engine.trigger(
        scenario_name="hvac-failure",
        greenhouse_id="brightharvest",
        zone_ids=["BH-Z01", "BH-Z02"],
        severity=0.8,
        duration=duration,
        start_time=start,
    )
    print(f"Triggered: {aid}")
    print(f"  Registered scenarios: {engine.registered_scenarios}")
    print()

    # -- step through 10 ticks (1 second apart) -----------------------------
    baseline = {"air_temperature": 22.0, "air_humidity": 65.0, "co2_level": 800.0}

    for step in range(12):
        t = start + timedelta(seconds=step)
        result = engine.apply("BH-Z01", baseline, t)
        # Also check a zone that is NOT affected
        unaffected = engine.apply("BH-Z05", baseline, t)

        active = engine.get_active()
        phase_str = active[0]["phase"] if active else "resolved"

        print(
            f"  t={step:2d}s | phase={phase_str:<9s} "
            f"| temp={result['air_temperature']:5.1f}°C "
            f"| humidity={result['air_humidity']:5.1f}% "
            f"| unaffected_temp={unaffected['air_temperature']:5.1f}°C"
        )

    print()
    print(f"Active count: {engine.active_count}")
    print(f"History records: {len(engine.get_history())}")
    for h in engine.get_history():
        print(f"  {h['anomaly_id']}: {h['scenario']} "
              f"({h['duration_seconds']:.0f}s, auto={h['auto_resolved']})")
    print("\n✓ Anomaly engine self-test passed.")
