"""Energy consumption derived from equipment state and weather.

Generates hourly :class:`EnergyReading` records for each greenhouse.
Electricity, natural gas, water, recycled water, and purchased CO₂ are all
computed from the zone-level equipment states aggregated across 8 zones, then
combined with a weather-dependent base load.
"""

from __future__ import annotations

from datetime import datetime

import numpy as np

from src.config import (
    ZONES,
    get_zones_for_greenhouse,
)
from src.models.greenhouse import EnergyReading, WeatherReading, ZoneState

# ---------------------------------------------------------------------------
# Per-zone energy coefficients
# ---------------------------------------------------------------------------

# Electricity
_LIGHTING_KWH_PER_ZONE_HR = 0.4       # supplemental grow lights
_COOLING_KWH_PER_ZONE_HR = 0.2        # fans + evaporative cooling at 100 %
_PUMP_KWH_PER_ZONE_HR = 0.1           # irrigation + recirc pumps
_BASE_LOAD_KWH_PER_GH_HR = 2.0        # controls, sensors, office

# Natural gas
_GAS_M3_PER_ZONE_HR_FULL = 0.5        # at 100 % heating output

# Water
_IRRIGATION_L_PER_MIN = 25.0          # per zone when pump is running
_AVG_IRRIGATION_MIN_PER_HR = 10.0     # average irrigation minutes / hr
_RECYCLE_FRACTION = 0.80               # 75-85 % water recycled (midpoint)

# CO₂ supplementation
_CO2_KG_PER_ZONE_HR = 0.5             # external CO₂ when injecting
_CO2_FROM_GAS_KG_PER_M3 = 1.8         # CO₂ produced per m³ natural gas


# ---------------------------------------------------------------------------
# EnergySimulator
# ---------------------------------------------------------------------------


class EnergySimulator:
    """Derives energy consumption from equipment states and weather."""

    def __init__(self, seed: int | None = None) -> None:
        self._rng = np.random.default_rng(seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_hourly(
        self,
        timestamp: datetime,
        greenhouse_id: str,
        zone_states: list[ZoneState],
        weather: WeatherReading,
    ) -> EnergyReading:
        """Generate one hourly energy record for a greenhouse.

        Args:
            timestamp:     Hour boundary (UTC).
            greenhouse_id: Greenhouse identifier.
            zone_states:   List of zone states for this greenhouse's 8 zones.
            weather:       Weather conditions at this greenhouse.
        """
        n_zones = len(zone_states) or 1

        # ----- Electricity (kWh) -------------------------------------------
        lighting_kwh = sum(
            _LIGHTING_KWH_PER_ZONE_HR
            for zs in zone_states
            if zs.lights_on
        )
        cooling_kwh = sum(
            _COOLING_KWH_PER_ZONE_HR * (zs.cooling_output / 100.0)
            for zs in zone_states
        )
        pump_kwh = sum(
            _PUMP_KWH_PER_ZONE_HR
            for zs in zone_states
            if zs.irrigation_on or zs.recirc_on
        )
        base_kwh = _BASE_LOAD_KWH_PER_GH_HR
        # Small random noise ±3 %
        noise = 1.0 + self._rng.normal(0, 0.015)
        electricity_kwh = (lighting_kwh + cooling_kwh + pump_kwh + base_kwh) * noise

        # ----- Natural gas (m³) --------------------------------------------
        gas_m3 = sum(
            _GAS_M3_PER_ZONE_HR_FULL * (zs.heating_output / 100.0)
            for zs in zone_states
        )
        # Weather influence: colder outside → slightly more gas beyond what
        # heating_output already captures (pipe losses, pre-heat, etc.)
        if weather.outside_temperature < 0:
            gas_m3 *= 1.0 + abs(weather.outside_temperature) * 0.005
        gas_m3 *= 1.0 + self._rng.normal(0, 0.02)
        gas_m3 = max(0.0, gas_m3)

        # ----- Water (liters) ----------------------------------------------
        irrigating_zones = [zs for zs in zone_states if zs.irrigation_on]
        # Each irrigating zone runs ~10 min/hr on average
        avg_min = _AVG_IRRIGATION_MIN_PER_HR + self._rng.normal(0, 2)
        avg_min = float(np.clip(avg_min, 5, 20))
        water_liters = len(irrigating_zones) * _IRRIGATION_L_PER_MIN * avg_min
        # Non-irrigating zones still use some water (misting, cleaning)
        water_liters += (n_zones - len(irrigating_zones)) * self._rng.uniform(5, 20)
        water_liters *= 1.0 + self._rng.normal(0, 0.03)
        water_liters = max(0.0, water_liters)

        # ----- Recycled water ----------------------------------------------
        recycle_frac = _RECYCLE_FRACTION + self._rng.normal(0, 0.02)
        recycle_frac = float(np.clip(recycle_frac, 0.70, 0.90))
        water_recycled = water_liters * recycle_frac

        # ----- CO₂ purchased (kg) ------------------------------------------
        co2_from_injection = sum(
            _CO2_KG_PER_ZONE_HR * min(1.0, zs.co2_injection / 2.0)
            for zs in zone_states
            if zs.co2_injection > 0
        )
        # Subtract CO₂ already produced as a by-product of gas combustion
        co2_from_gas = gas_m3 * _CO2_FROM_GAS_KG_PER_M3
        co2_purchased = max(0.0, co2_from_injection - co2_from_gas)
        co2_purchased *= 1.0 + self._rng.normal(0, 0.02)
        co2_purchased = max(0.0, co2_purchased)

        return EnergyReading(
            timestamp=timestamp,
            greenhouse_id=greenhouse_id,
            electricity_kwh=round(electricity_kwh, 2),
            natural_gas_m3=round(gas_m3, 2),
            water_liters=round(water_liters, 1),
            water_recycled_liters=round(water_recycled, 1),
            co2_purchased_kg=round(co2_purchased, 2),
        )


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from datetime import datetime, timedelta, timezone

    from src.config import ZONES, get_zones_for_greenhouse
    from src.models.greenhouse import WeatherReading, ZoneState

    print("=" * 72)
    print("  EnergySimulator — quick smoke test")
    print("=" * 72)

    sim = EnergySimulator(seed=42)
    ts = datetime(2025, 7, 15, 12, 0, tzinfo=timezone.utc)

    for gh_id, label in [
        ("brightharvest", "BrightHarvest Greens"),
        ("mucci-valley", "Mucci Valley Farms"),
    ]:
        zones = get_zones_for_greenhouse(gh_id)
        states = [ZoneState.from_config(z) for z in zones]
        # Simulate some equipment activity
        for i, s in enumerate(states):
            s.lights_on = i % 2 == 0
            s.heating_output = 40.0 if i < 4 else 0.0
            s.cooling_output = 0.0 if i < 4 else 30.0
            s.irrigation_on = i % 3 == 0
            s.co2_injection = 1.2 if i % 2 == 0 else 0.0

        wx = WeatherReading(
            timestamp=ts,
            greenhouse_id=gh_id,
            outside_temperature=28.0,
            outside_humidity=65.0,
            wind_speed=12.0,
            wind_direction=180.0,
            precipitation=0.0,
            solar_radiation=600.0,
            barometric_pressure=1013.0,
            cloud_cover=30.0,
        )

        reading = sim.generate_hourly(ts, gh_id, states, wx)
        print(f"\n  {label}:")
        print(f"    Electricity : {reading.electricity_kwh:>8.2f} kWh")
        print(f"    Natural gas : {reading.natural_gas_m3:>8.2f} m³")
        print(f"    Water       : {reading.water_liters:>8.1f} L")
        print(f"    Recycled    : {reading.water_recycled_liters:>8.1f} L")
        print(f"    CO₂ purch.  : {reading.co2_purchased_kg:>8.2f} kg")
