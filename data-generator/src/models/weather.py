"""Weather pattern generation for greenhouse locations.

Produces realistic outdoor weather data for BrightHarvest Greens (Rochelle, IL)
and Mucci Valley Farms (Kingsville, ON).  The generated weather drives greenhouse
simulation — outdoor temperature affects heating/cooling demand, solar radiation
affects PAR light, etc.

Physics-based model with:
  - Sinusoidal annual and daily temperature cycles
  - Latitude-dependent day-length and solar radiation
  - Autocorrelated random walks for multi-day weather systems
  - Correlated cloud cover, precipitation, barometric pressure
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np

from src.config import GREENHOUSES, WEATHER_PARAMS, WeatherParams
from src.models.greenhouse import WeatherReading

# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------

_TWO_PI = 2.0 * math.pi
_STEP_SECONDS = 60  # internal simulation step for random walks (1 minute)


@dataclass
class _WalkState:
    """Mutable state for the autocorrelated random walks."""

    timestamp: datetime
    temp_offset: float  # multi-day weather-system temperature offset (°C)
    cloud_cover: float  # 0-100 %
    pressure_offset: float  # deviation from 1013 hPa
    wind_direction: float  # degrees 0-360
    wind_gust_offset: float  # additional wind speed (km/h)


class WeatherGenerator:
    """Generates realistic weather patterns for a greenhouse location.

    Uses sinusoidal seasonal curves with daily cycles, noise,
    and random weather events (cold snaps, heat waves, storms).
    """

    def __init__(self, greenhouse_id: str, seed: int | None = None) -> None:
        """Initialize with greenhouse config and optional random seed."""
        self._greenhouse_id = greenhouse_id
        self._gh = GREENHOUSES[greenhouse_id]
        self._params: WeatherParams = WEATHER_PARAMS[greenhouse_id]
        self._lat_rad = math.radians(self._gh.latitude)
        self._rng = np.random.default_rng(seed)
        self._state: _WalkState | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, timestamp: datetime) -> WeatherReading:
        """Generate weather conditions for a specific timestamp."""
        self._advance_to(timestamp)
        return self._sample(timestamp)

    def generate_range(
        self,
        start: datetime,
        end: datetime,
        interval_seconds: int = 300,
    ) -> list[WeatherReading]:
        """Generate weather for a time range at the given interval."""
        readings: list[WeatherReading] = []
        ts = start
        delta = timedelta(seconds=interval_seconds)
        while ts <= end:
            readings.append(self.generate(ts))
            ts += delta
        return readings

    # ------------------------------------------------------------------
    # Random-walk state management
    # ------------------------------------------------------------------

    def _init_state(self, timestamp: datetime) -> None:
        """Bootstrap random-walk state at *timestamp*."""
        self._state = _WalkState(
            timestamp=timestamp,
            temp_offset=self._rng.normal(0, 2.0),
            cloud_cover=self._rng.uniform(20, 60),
            pressure_offset=self._rng.normal(0, 5.0),
            wind_direction=self._rng.uniform(0, 360),
            wind_gust_offset=abs(self._rng.normal(0, 3.0)),
        )

    def _advance_to(self, target: datetime) -> None:
        """Step the random walks forward to *target*.

        If *target* is before the current state timestamp the state is
        re-initialised (simple replay — acceptable because the RNG is seeded).
        """
        if self._state is None:
            self._init_state(target)
            return

        gap_seconds = (target - self._state.timestamp).total_seconds()

        if gap_seconds < 0:
            # Jumping backwards — re-seed and re-init
            self._rng = np.random.default_rng(
                int(self._rng.integers(0, 2**31))
            )
            self._init_state(target)
            return

        if gap_seconds == 0:
            return

        # Number of internal 1-minute steps to simulate
        steps = max(1, int(gap_seconds / _STEP_SECONDS))
        # Cap at a reasonable maximum to avoid huge loops on multi-year jumps
        steps = min(steps, 525_960)  # ~1 year of 1-min steps

        self._step_walks(steps)
        self._state.timestamp = target

    def _step_walks(self, n: int) -> None:
        """Advance random walks by *n* internal steps."""
        s = self._state
        assert s is not None

        # Autocorrelation coefficients per 1-minute step
        # (derived from hourly values: per-step = hourly ** (1/60))
        temp_ac = 0.95 ** (1.0 / 60.0)   # ~0.99914
        cloud_ac = 0.98 ** (1.0 / 60.0)   # ~0.99966
        pres_ac = 0.97 ** (1.0 / 60.0)    # ~0.99949
        wind_dir_ac = 0.99 ** (1.0 / 60.0)
        wind_gust_ac = 0.90 ** (1.0 / 60.0)

        # Pre-generate all noise in one shot for speed
        noise = self._rng.normal(size=(n, 5))

        for i in range(n):
            s.temp_offset = (
                temp_ac * s.temp_offset
                + (1 - temp_ac) * noise[i, 0] * 3.0
            )

            s.cloud_cover = (
                cloud_ac * s.cloud_cover
                + (1 - cloud_ac) * (50.0 + noise[i, 1] * 80.0)
            )
            s.cloud_cover = float(np.clip(s.cloud_cover, 0.0, 100.0))

            s.pressure_offset = (
                pres_ac * s.pressure_offset
                + (1 - pres_ac) * noise[i, 2] * 15.0
            )
            s.pressure_offset = float(
                np.clip(s.pressure_offset, -23.0, 27.0)
            )

            s.wind_direction = (
                wind_dir_ac * s.wind_direction
                + (1 - wind_dir_ac) * (s.wind_direction + noise[i, 3] * 60.0)
            ) % 360.0

            s.wind_gust_offset = (
                wind_gust_ac * s.wind_gust_offset
                + (1 - wind_gust_ac) * abs(noise[i, 4]) * 10.0
            )
            s.wind_gust_offset = max(0.0, s.wind_gust_offset)

    # ------------------------------------------------------------------
    # Deterministic physics for a single timestamp
    # ------------------------------------------------------------------

    def _sample(self, ts: datetime) -> WeatherReading:
        """Build a :class:`WeatherReading` from current walk state + physics."""
        assert self._state is not None
        p = self._params
        doy = ts.timetuple().tm_yday
        hour = ts.hour + ts.minute / 60.0 + ts.second / 3600.0

        # --- Temperature ---
        annual_cycle = p.temp_annual_amplitude * math.sin(
            _TWO_PI * (doy - 80) / 365.0
        )
        daily_cycle = p.temp_daily_amplitude * math.sin(
            _TWO_PI * (hour - 6.0) / 24.0
        )
        temp_noise = self._rng.normal(0, 1.5)
        temperature = (
            p.temp_annual_mean
            + annual_cycle
            + daily_cycle
            + self._state.temp_offset
            + temp_noise
        )

        # --- Day length & solar radiation ---
        declination_deg = 23.45 * math.sin(
            _TWO_PI * (284 + doy) / 365.0
        )
        decl_rad = math.radians(declination_deg)
        cos_hour_angle = -math.tan(self._lat_rad) * math.tan(decl_rad)
        cos_hour_angle = float(np.clip(cos_hour_angle, -1.0, 1.0))
        hour_angle_rad = math.acos(cos_hour_angle)
        day_length = 2.0 * math.degrees(hour_angle_rad) / 15.0  # hours

        solar_noon = 12.0
        sunrise = solar_noon - day_length / 2.0
        sunset = solar_noon + day_length / 2.0

        if sunrise < hour < sunset and day_length > 0:
            sun_frac = (hour - sunrise) / day_length
            # Peak radiation scales seasonally (300 W/m² winter → 800 summer)
            summer_factor = 0.5 + 0.5 * math.sin(
                _TWO_PI * (doy - 80) / 365.0
            )
            peak_radiation = 300.0 + 500.0 * summer_factor
            raw_radiation = peak_radiation * math.sin(math.pi * sun_frac)
            cloud_factor = 1.0 - 0.7 * (self._state.cloud_cover / 100.0)
            solar_radiation = max(0.0, raw_radiation * cloud_factor)
        else:
            solar_radiation = 0.0

        # --- Humidity ---
        # Seasonal mean: higher in summer
        summer_frac = 0.5 + 0.5 * math.sin(_TWO_PI * (doy - 80) / 365.0)
        seasonal_humidity = (
            p.humidity_winter_mean
            + (p.humidity_summer_mean - p.humidity_winter_mean) * summer_frac
        )
        # Daily cycle: higher at night (inverse of temp daily cycle)
        humidity_daily = -15.0 * math.sin(_TWO_PI * (hour - 6.0) / 24.0)
        humidity_noise = self._rng.normal(0, 3.0)
        humidity = seasonal_humidity + humidity_daily + humidity_noise
        humidity = float(np.clip(humidity, 25.0, 100.0))

        # --- Wind ---
        base_wind = 12.0 + 4.0 * math.sin(
            _TWO_PI * (doy - 80) / 365.0
        )  # slightly windier in spring
        wind_noise = self._rng.normal(0, 2.0)
        wind_speed = max(0.0, base_wind + self._state.wind_gust_offset + wind_noise)
        wind_direction = self._state.wind_direction % 360.0

        # --- Barometric Pressure ---
        pressure = 1013.0 + self._state.pressure_offset
        pressure = float(np.clip(pressure, 990.0, 1040.0))

        # --- Cloud Cover (from walk state) ---
        # Bias cloud cover higher in winter
        winter_cloud_bias = 10.0 * (
            0.5 - 0.5 * math.sin(_TWO_PI * (doy - 80) / 365.0)
        )
        cloud_cover = float(
            np.clip(self._state.cloud_cover + winter_cloud_bias, 0.0, 100.0)
        )

        # --- Precipitation ---
        precipitation = 0.0
        # Base probability ~5-10% of hours, higher with low pressure & clouds
        precip_prob = 0.03  # base per 5-min slot
        if pressure < 1005.0:
            precip_prob += 0.05
        if pressure < 1000.0:
            precip_prob += 0.10
        if cloud_cover > 70:
            precip_prob += 0.05
        if cloud_cover > 85:
            precip_prob += 0.05

        if self._rng.random() < precip_prob:
            # Light to moderate precipitation
            precipitation = float(self._rng.exponential(2.0))
            precipitation = min(precipitation, 8.0)
            # Snow if cold (still reported as mm/hr water equivalent)
            if temperature < 0:
                precipitation *= 0.7  # lighter accumulation

        return WeatherReading(
            timestamp=ts,
            greenhouse_id=self._greenhouse_id,
            outside_temperature=round(temperature, 1),
            outside_humidity=round(humidity, 1),
            wind_speed=round(wind_speed, 1),
            wind_direction=round(wind_direction % 360, 0),
            precipitation=round(precipitation, 2),
            solar_radiation=round(solar_radiation, 1),
            barometric_pressure=round(pressure, 1),
            cloud_cover=round(cloud_cover, 0),
        )


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from datetime import datetime, timedelta, timezone

    start = datetime(2025, 7, 15, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=24)

    for gh_id, label in [
        ("brightharvest", "BrightHarvest Greens — Rochelle, IL"),
        ("mucci-valley", "Mucci Valley Farms — Kingsville, ON"),
    ]:
        print(f"\n{'='*72}")
        print(f"  {label}")
        print(f"{'='*72}")
        gen = WeatherGenerator(gh_id, seed=42)
        readings = gen.generate_range(start, end, interval_seconds=3600)
        print(
            f"{'Hour':>5}  {'Temp °C':>8}  {'Humid %':>8}  {'Solar W/m²':>10}"
            f"  {'Wind km/h':>10}  {'Press hPa':>10}  {'Cloud %':>8}"
            f"  {'Precip mm/h':>11}"
        )
        print("-" * 80)
        for r in readings:
            print(
                f"{r.timestamp.strftime('%H:%M'):>5}"
                f"  {r.outside_temperature:>8.1f}"
                f"  {r.outside_humidity:>8.1f}"
                f"  {r.solar_radiation:>10.1f}"
                f"  {r.wind_speed:>10.1f}"
                f"  {r.barometric_pressure:>10.1f}"
                f"  {r.cloud_cover:>8.0f}"
                f"  {r.precipitation:>11.2f}"
            )
