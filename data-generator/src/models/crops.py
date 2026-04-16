"""Crop growth, yield, and quality simulation.

Generates :class:`DailyHarvest` and :class:`WeeklyCropHealth` records for
every grow zone.  The model captures two fundamentally different planting
strategies:

  * **Leafy greens** (BrightHarvest): 25-35 day succession cycles — harvest
    the entire zone at end-of-cycle, replant immediately.
  * **Vine crops / berries** (Mucci Valley): 180-300 day cycles — continuous
    daily harvest once the crop reaches maturity.

Yield is driven by environmental quality factors derived from
:class:`ZoneState` snapshots.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np

from src.config import (
    CROPS,
    ZONES,
    CropConfig,
    ZoneConfig,
    get_crop_for_zone,
    get_zones_for_greenhouse,
)
from src.models.greenhouse import DailyHarvest, WeeklyCropHealth, ZoneState

# ---------------------------------------------------------------------------
# Yield constants per crop category
# ---------------------------------------------------------------------------

_BASE_YIELD_PER_SQFT: dict[str, float] = {
    "leafy_green": 0.015,  # kg / sqft / day at peak
    "vine_crop": 0.008,
    "berry": 0.005,
}

# Vine crops / berries begin daily harvest once this fraction of the total
# growth cycle has elapsed.
_MATURITY_FRACTION: dict[str, float] = {
    "vine_crop": 0.35,  # ~63 days for a 180-day cucumber
    "berry": 0.40,
}

# Package weights (kg per case) used by supply chain
CASE_WEIGHT_KG: dict[str, float] = {
    "leafy_green": 5.0,
    "vine_crop": 10.0,
    "berry": 4.0,
}

# Optimal DLI targets (mol/m²/day) by category.  PAR (µmol/m²/s) is
# accumulated over the photoperiod and converted:
#   DLI = PAR * photoperiod_hrs * 3600 / 1_000_000
_OPTIMAL_DLI: dict[str, float] = {
    "leafy_green": 17.0,
    "vine_crop": 25.0,
    "berry": 20.0,
}


# ---------------------------------------------------------------------------
# CropSimulator
# ---------------------------------------------------------------------------


class CropSimulator:
    """Simulates crop growth, yield, and quality for all zones."""

    def __init__(self, seed: int | None = None) -> None:
        self._rng = np.random.default_rng(seed)

        # Track planting cycles per zone
        self._planting_dates: dict[str, date] = {}   # zone_id -> current planting date
        self._cycle_number: dict[str, int] = {}       # zone_id -> which cycle we're on

        # Cumulative yield accumulator for leafy greens within a cycle
        self._leafy_accum: dict[str, float] = {}      # zone_id -> kg accumulated

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def initialize_plantings(self, start_date: date) -> None:
        """Set initial planting dates, staggered across zones."""
        for zone_id, zone_cfg in ZONES.items():
            crop = get_crop_for_zone(zone_id)
            self._cycle_number[zone_id] = 1
            self._leafy_accum[zone_id] = 0.0

            if crop.category == "leafy_green":
                # Stagger by 3-5 days per zone pair so harvest windows spread
                zone_idx = int(zone_id.split("-Z")[1]) - 1  # 0-based
                offset = (zone_idx // 2) * 4  # 0, 4, 8, 12 days offset
                self._planting_dates[zone_id] = start_date + timedelta(days=offset)
            else:
                # Vine crops / berries: all planted at start
                self._planting_dates[zone_id] = start_date

    # ------------------------------------------------------------------
    # Environmental quality factors
    # ------------------------------------------------------------------

    @staticmethod
    def _dli_factor(zone_state: ZoneState, crop: CropConfig) -> float:
        """Light factor: ratio of actual DLI to optimal.

        Assumes cumulative_dli on the zone state is kept up-to-date by the
        sensor tick.  A rough fallback uses instantaneous PAR * 16h.
        """
        optimal = _OPTIMAL_DLI.get(crop.category, 20.0)
        if zone_state.cumulative_dli > 0:
            actual = zone_state.cumulative_dli
        else:
            # Fallback: estimate from instantaneous PAR
            actual = zone_state.current_par * 16.0 * 3600.0 / 1_000_000.0
        return min(1.2, actual / optimal) if optimal > 0 else 1.0

    @staticmethod
    def _temp_factor(zone_state: ZoneState, crop: CropConfig) -> float:
        """Temperature factor: each °C off optimal costs ~8 %."""
        deviation = abs(zone_state.current_temp - crop.optimal_temp)
        return max(0.3, 1.0 - deviation * 0.08)

    @staticmethod
    def _nutrient_factor(zone_state: ZoneState, crop: CropConfig) -> float:
        """EC/pH deviation factor."""
        ec_dev = abs(zone_state.current_ec - crop.optimal_ec)
        ph_dev = abs(zone_state.current_ph - crop.optimal_ph)
        ec_factor = max(0.5, 1.0 - ec_dev * 0.15)
        ph_factor = max(0.5, 1.0 - ph_dev * 0.20)
        return ec_factor * ph_factor

    def _combined_factor(self, zone_state: ZoneState, crop: CropConfig) -> float:
        """Product of all environmental quality factors, with noise."""
        base = (
            self._dli_factor(zone_state, crop)
            * self._temp_factor(zone_state, crop)
            * self._nutrient_factor(zone_state, crop)
        )
        # Small daily noise ±5 %
        noise = 1.0 + self._rng.normal(0, 0.025)
        return float(np.clip(base * noise, 0.1, 1.3))

    # ------------------------------------------------------------------
    # Grade distribution
    # ------------------------------------------------------------------

    def _grade_split(self, quality_factor: float) -> tuple[float, float, float]:
        """Return (grade_a_pct, grade_b_pct, grade_c_pct) summing to ~100."""
        # When quality_factor is 1.0 → ~80/15/5 split
        a_base = 80.0 * quality_factor
        a_pct = float(np.clip(a_base + self._rng.normal(0, 3), 40, 95))
        remaining = 100.0 - a_pct
        b_frac = self._rng.uniform(0.55, 0.75)
        b_pct = remaining * b_frac
        c_pct = remaining - b_pct
        return round(a_pct, 1), round(b_pct, 1), round(c_pct, 1)

    # ------------------------------------------------------------------
    # Daily harvest
    # ------------------------------------------------------------------

    def generate_daily_harvest(
        self,
        current_date: date,
        zone_states: dict[str, ZoneState],
    ) -> list[DailyHarvest]:
        """Generate harvest data for zones that are in harvest phase today."""
        harvests: list[DailyHarvest] = []

        for zone_id, zone_cfg in ZONES.items():
            crop = get_crop_for_zone(zone_id)
            state = zone_states.get(zone_id)
            if state is None:
                continue

            planting = self._planting_dates.get(zone_id)
            if planting is None:
                continue
            days_since = (current_date - planting).days
            if days_since < 0:
                continue

            factor = self._combined_factor(state, crop)
            base_yield = _BASE_YIELD_PER_SQFT[crop.category] * zone_cfg.size_sqft

            if crop.category == "leafy_green":
                harvest = self._leafy_harvest(
                    current_date, zone_id, zone_cfg, crop, state,
                    days_since, base_yield, factor,
                )
                if harvest is not None:
                    harvests.append(harvest)

            elif crop.category in ("vine_crop", "berry"):
                maturity_days = int(
                    crop.growth_cycle_days
                    * _MATURITY_FRACTION.get(crop.category, 0.35)
                )
                if days_since >= maturity_days:
                    harvest = self._vine_daily_harvest(
                        current_date, zone_id, zone_cfg, crop, state,
                        days_since, base_yield, factor,
                    )
                    harvests.append(harvest)

        return harvests

    def _leafy_harvest(
        self,
        current_date: date,
        zone_id: str,
        zone_cfg: ZoneConfig,
        crop: CropConfig,
        state: ZoneState,
        days_since: int,
        base_daily_yield: float,
        factor: float,
    ) -> DailyHarvest | None:
        """Leafy greens: accumulate daily, harvest whole zone at end of cycle."""
        # Accumulate potential yield for this day
        growth_progress = min(1.0, days_since / crop.growth_cycle_days)
        # Yield ramps up: low early, peaks in last third
        growth_curve = 0.3 + 0.7 * (growth_progress ** 0.6)
        daily_kg = base_daily_yield * factor * growth_curve
        self._leafy_accum[zone_id] = self._leafy_accum.get(zone_id, 0.0) + daily_kg

        # Harvest on the last day of the cycle
        if days_since < crop.growth_cycle_days:
            return None

        harvest_kg = self._leafy_accum[zone_id]
        waste_pct = self._rng.uniform(5.0, 12.0) * (1.0 / max(factor, 0.5))
        waste_pct = float(np.clip(waste_pct, 5.0, 20.0))
        waste_kg = harvest_kg * waste_pct / 100.0
        net_kg = harvest_kg - waste_kg

        case_wt = CASE_WEIGHT_KG[crop.category]
        units = max(1, int(net_kg / case_wt))

        a, b, c = self._grade_split(factor)
        revenue = net_kg * crop.market_price_per_kg

        # Succession planting: replant next day
        self._planting_dates[zone_id] = current_date + timedelta(days=1)
        self._cycle_number[zone_id] = self._cycle_number.get(zone_id, 1) + 1
        self._leafy_accum[zone_id] = 0.0

        return DailyHarvest(
            date=current_date,
            greenhouse_id=zone_cfg.greenhouse_id,
            zone_id=zone_id,
            crop_id=crop.crop_id,
            harvest_weight_kg=round(net_kg, 1),
            harvest_units=units,
            grade_a_pct=a,
            grade_b_pct=b,
            grade_c_pct=c,
            waste_kg=round(waste_kg, 1),
            waste_pct=round(waste_pct, 1),
            days_to_harvest=days_since,
            revenue_estimate_usd=round(revenue, 2),
        )

    def _vine_daily_harvest(
        self,
        current_date: date,
        zone_id: str,
        zone_cfg: ZoneConfig,
        crop: CropConfig,
        state: ZoneState,
        days_since: int,
        base_daily_yield: float,
        factor: float,
    ) -> DailyHarvest:
        """Vine crops / berries: continuous daily harvest once mature."""
        # Bell-shaped production curve: ramps up, peaks mid-harvest, tapers
        maturity_frac = _MATURITY_FRACTION.get(crop.category, 0.35)
        maturity_days = int(crop.growth_cycle_days * maturity_frac)
        harvest_window = crop.growth_cycle_days - maturity_days
        harvest_day = days_since - maturity_days
        if harvest_window > 0:
            progress = harvest_day / harvest_window
            # Bell curve peaking around 40-60 % of harvest window
            production_curve = math.exp(-((progress - 0.5) ** 2) / 0.08)
        else:
            production_curve = 1.0

        daily_kg = base_daily_yield * factor * production_curve
        waste_pct = self._rng.uniform(5.0, 10.0) * (1.0 / max(factor, 0.5))
        waste_pct = float(np.clip(waste_pct, 3.0, 18.0))
        waste_kg = daily_kg * waste_pct / 100.0
        net_kg = daily_kg - waste_kg

        case_wt = CASE_WEIGHT_KG[crop.category]
        units = max(1, int(net_kg / case_wt))

        a, b, c = self._grade_split(factor)
        revenue = net_kg * crop.market_price_per_kg

        # Replant if past cycle end
        if days_since >= crop.growth_cycle_days:
            self._planting_dates[zone_id] = current_date + timedelta(days=1)
            self._cycle_number[zone_id] = self._cycle_number.get(zone_id, 1) + 1

        return DailyHarvest(
            date=current_date,
            greenhouse_id=zone_cfg.greenhouse_id,
            zone_id=zone_id,
            crop_id=crop.crop_id,
            harvest_weight_kg=round(net_kg, 1),
            harvest_units=units,
            grade_a_pct=a,
            grade_b_pct=b,
            grade_c_pct=c,
            waste_kg=round(waste_kg, 1),
            waste_pct=round(waste_pct, 1),
            days_to_harvest=days_since,
            revenue_estimate_usd=round(revenue, 2),
        )

    # ------------------------------------------------------------------
    # Weekly health snapshot
    # ------------------------------------------------------------------

    def generate_weekly_health(
        self,
        week_start: date,
        zone_states: dict[str, ZoneState],
    ) -> list[WeeklyCropHealth]:
        """Generate weekly crop health measurements (every Wednesday)."""
        records: list[WeeklyCropHealth] = []

        for zone_id, zone_cfg in ZONES.items():
            crop = get_crop_for_zone(zone_id)
            state = zone_states.get(zone_id)
            if state is None:
                continue

            planting = self._planting_dates.get(zone_id)
            if planting is None:
                continue
            days_since = max(0, (week_start - planting).days)
            progress = min(1.0, days_since / crop.growth_cycle_days) if crop.growth_cycle_days > 0 else 0

            # Plant height: grows during vegetative, plateaus at harvest
            if crop.category == "leafy_green":
                max_height = self._rng.uniform(20, 35)  # cm
            elif crop.category == "vine_crop":
                max_height = self._rng.uniform(180, 280)
            else:
                max_height = self._rng.uniform(25, 40)

            height = max_height * min(1.0, progress * 1.2)
            height += self._rng.normal(0, height * 0.03)
            height = max(1.0, height)

            # Leaf count
            if crop.category == "leafy_green":
                max_leaves = self._rng.uniform(15, 30)
            elif crop.category == "vine_crop":
                max_leaves = self._rng.uniform(60, 120)
            else:
                max_leaves = self._rng.uniform(30, 60)
            leaf_count = max_leaves * min(1.0, progress * 1.1)
            leaf_count += self._rng.normal(0, leaf_count * 0.05)
            leaf_count = max(2.0, leaf_count)

            # Predicted quality grade based on environmental conditions
            factor = self._combined_factor(state, crop)
            if factor >= 0.85:
                grade = "A"
            elif factor >= 0.65:
                grade = "B"
            else:
                grade = "C"

            # Predicted yield (simple projection)
            base_yield = _BASE_YIELD_PER_SQFT[crop.category] * zone_cfg.size_sqft
            remaining_days = max(0, crop.growth_cycle_days - days_since)
            predicted_yield = base_yield * factor * remaining_days

            records.append(WeeklyCropHealth(
                week_start=week_start,
                greenhouse_id=zone_cfg.greenhouse_id,
                zone_id=zone_id,
                crop_id=crop.crop_id,
                avg_plant_height_cm=round(height, 1),
                avg_leaf_count=round(leaf_count, 1),
                predicted_quality_grade=grade,
                predicted_yield_kg=round(predicted_yield, 1),
            ))

        return records


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from datetime import date

    print("=" * 72)
    print("  CropSimulator — quick smoke test")
    print("=" * 72)

    sim = CropSimulator(seed=42)
    start = date(2025, 1, 1)
    sim.initialize_plantings(start)

    # Build dummy zone states at optimal conditions
    zone_states: dict[str, ZoneState] = {}
    for zid, zcfg in ZONES.items():
        zone_states[zid] = ZoneState.from_config(zcfg)

    # Simulate 40 days to see a leafy-green cycle complete
    total_harvests = 0
    for day_offset in range(40):
        d = start + timedelta(days=day_offset)
        harvests = sim.generate_daily_harvest(d, zone_states)
        if harvests:
            total_harvests += len(harvests)
            for h in harvests:
                print(
                    f"  {h.date}  {h.zone_id}  {h.crop_id:<18s}"
                    f"  {h.harvest_weight_kg:>8.1f} kg"
                    f"  A={h.grade_a_pct:.0f}%  ${h.revenue_estimate_usd:,.0f}"
                )

    print(f"\n  Total harvests over 40 days: {total_harvests}")

    # Weekly health
    health = sim.generate_weekly_health(start + timedelta(days=14), zone_states)
    print(f"\n  Weekly health records: {len(health)}")
    for h in health[:4]:
        print(
            f"  {h.zone_id}  {h.crop_id:<18s}"
            f"  height={h.avg_plant_height_cm:.0f}cm"
            f"  leaves={h.avg_leaf_count:.0f}"
            f"  grade={h.predicted_quality_grade}"
        )
