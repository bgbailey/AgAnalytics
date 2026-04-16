"""Historical anomaly scheduling — places anomalies across 2 years of data.

Distributes 10-12 anomaly events per year with seasonal patterns:
- HVAC failures cluster in winter (Dec-Feb)
- Irrigation failures more common in summer (Jun-Aug)
- Nutrient drift can happen anytime
- Cold chain breaks happen year-round but more in summer (heat)
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any

import numpy as np

from src.config import ZONES, get_zones_for_greenhouse, get_adjacent_zones


@dataclass
class ScheduledAnomaly:
    """A pre-scheduled anomaly for historical data generation."""
    start_time: datetime
    scenario_name: str
    greenhouse_id: str
    zone_ids: list[str]
    severity: float
    duration: timedelta
    auto_recover: bool = True
    metadata: dict[str, Any] | None = None


def generate_historical_schedule(
    start_date: date,
    end_date: date,
    seed: int = 42,
    events_per_year: int = 11,
) -> list[ScheduledAnomaly]:
    """Generate a deterministic schedule of anomaly events.
    
    Creates ~events_per_year anomalies per year, distributed across:
    - 3-4 HVAC failures (winter-weighted)
    - 2-3 nutrient drifts (uniform)
    - 2-3 irrigation failures (summer-weighted)
    - 2-3 cold chain breaks (summer-weighted)
    
    Adjacent zones are included for cascading scenarios.
    """
    rng = np.random.default_rng(seed)
    schedule = []
    
    # Generate year by year
    current_year_start = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
    final = datetime(end_date.year, end_date.month, end_date.day, tzinfo=timezone.utc)
    
    while current_year_start < final:
        year_end = min(current_year_start + timedelta(days=365), final)
        year_days = (year_end - current_year_start).days
        
        # Distribute events across the year
        # HVAC failures: winter months (Nov-Feb)
        for _ in range(rng.integers(3, 5)):
            winter_day = rng.choice([*range(0, 60), *range(305, 365)])  # Jan-Feb or Nov-Dec
            day_offset = winter_day % year_days
            ts = current_year_start + timedelta(days=int(day_offset), hours=int(rng.integers(2, 8)))
            if ts >= final:
                continue
            gh = rng.choice(["brightharvest", "mucci-valley"])
            zones = get_zones_for_greenhouse(gh)
            primary = rng.choice(zones)
            affected = [primary.zone_id]
            # 50% chance adjacent zones are also affected
            if rng.random() > 0.5:
                adj = get_adjacent_zones(primary.zone_id)
                affected.extend([z.zone_id for z in adj[:1]])
            schedule.append(ScheduledAnomaly(
                start_time=ts,
                scenario_name="hvac-failure",
                greenhouse_id=gh,
                zone_ids=affected,
                severity=float(rng.uniform(0.5, 1.0)),
                duration=timedelta(minutes=int(rng.integers(8, 25))),
            ))
        
        # Nutrient drifts: any time of year
        for _ in range(rng.integers(2, 4)):
            day_offset = int(rng.integers(0, year_days))
            ts = current_year_start + timedelta(days=day_offset, hours=int(rng.integers(8, 18)))
            if ts >= final:
                continue
            gh = rng.choice(["brightharvest", "mucci-valley"])
            zones = get_zones_for_greenhouse(gh)
            primary = rng.choice(zones)
            schedule.append(ScheduledAnomaly(
                start_time=ts,
                scenario_name="nutrient-drift",
                greenhouse_id=gh,
                zone_ids=[primary.zone_id],
                severity=float(rng.uniform(0.4, 0.9)),
                duration=timedelta(minutes=int(rng.integers(12, 30))),
            ))
        
        # Irrigation failures: summer-weighted
        for _ in range(rng.integers(2, 4)):
            summer_day = int(rng.integers(120, 270))  # May-Sept
            day_offset = summer_day % year_days
            ts = current_year_start + timedelta(days=day_offset, hours=int(rng.integers(10, 16)))
            if ts >= final:
                continue
            gh = rng.choice(["brightharvest", "mucci-valley"])
            zones = get_zones_for_greenhouse(gh)
            primary = rng.choice(zones)
            schedule.append(ScheduledAnomaly(
                start_time=ts,
                scenario_name="irrigation-failure",
                greenhouse_id=gh,
                zone_ids=[primary.zone_id],
                severity=float(rng.uniform(0.5, 0.9)),
                duration=timedelta(minutes=int(rng.integers(10, 20))),
            ))
        
        # Cold chain breaks: summer-weighted
        for _ in range(rng.integers(2, 3)):
            day_offset = int(rng.integers(90, 280))  # Apr-Oct
            day_offset = day_offset % year_days
            ts = current_year_start + timedelta(days=day_offset, hours=int(rng.integers(6, 14)))
            if ts >= final:
                continue
            gh = rng.choice(["brightharvest", "mucci-valley"])
            schedule.append(ScheduledAnomaly(
                start_time=ts,
                scenario_name="cold-chain-break",
                greenhouse_id=gh,
                zone_ids=[],  # doesn't affect zones directly
                severity=float(rng.uniform(0.3, 0.8)),
                duration=timedelta(minutes=int(rng.integers(15, 45))),
                metadata={"shipment_affected": True},
            ))
        
        current_year_start = year_end
    
    # Sort by start time
    schedule.sort(key=lambda a: a.start_time)
    return schedule


def print_schedule(schedule: list[ScheduledAnomaly]) -> None:
    """Pretty-print the anomaly schedule."""
    print(f"\nAnomaly Schedule: {len(schedule)} events")
    print("-" * 90)
    for i, a in enumerate(schedule, 1):
        zones_str = ", ".join(a.zone_ids) if a.zone_ids else "(supply chain)"
        print(f"  {i:2d}. {a.start_time.strftime('%Y-%m-%d %H:%M')} | "
              f"{a.scenario_name:<20s} | {a.greenhouse_id:<15s} | "
              f"sev={a.severity:.1f} | {int(a.duration.total_seconds()//60)}min | {zones_str}")


if __name__ == "__main__":
    from datetime import date as _date
    schedule = generate_historical_schedule(
        start_date=_date(2024, 4, 1),
        end_date=_date(2026, 4, 1),
    )
    print_schedule(schedule)
