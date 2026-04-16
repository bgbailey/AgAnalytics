"""IoT sensor telemetry generator for greenhouse zones.

Physics-based simulation that produces realistic 30-second sensor readings
driven by external weather, HVAC control logic, and crop setpoints.  Each
zone maintains internal state (temperature, humidity, CO₂, substrate, etc.)
and a PID-like HVAC controller that tracks crop-specific setpoints.

The physics are intentionally simplified but calibrated to feel right to
anyone who has operated a commercial greenhouse:
  - Glass-envelope heat-transfer with thermal mass lag
  - Mutual-exclusion heating/cooling with dead-band
  - Transpiration-driven humidity, vent-dilution CO₂
  - Irrigation scheduling with substrate moisture decay
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import numpy as np

from src.config import (
    CROPS,
    SENSORS,
    ZONES,
    CropConfig,
    SensorConfig,
    ZoneConfig,
    get_crop_for_zone,
)
from src.models.greenhouse import EquipmentState, SensorReading, WeatherReading, ZoneState

# ---------------------------------------------------------------------------
# Physics constants
# ---------------------------------------------------------------------------

_GLASS_TRANSMITTANCE = 0.65       # fraction of solar that enters
_SOLAR_TO_PAR = 2.0               # W/m² → µmol/m²/s rough conversion

# Temperature equilibrium model
# T_eq = outdoor + solar_offset + heating_offset - cooling_offset
# Temperature exponentially approaches T_eq with time constant tau.
_THERMAL_MASS_TAU = 20.0 * 60.0  # 20 minutes in seconds
_SUBSTRATE_TEMP_TAU = 30.0 * 60.0 # 30-minute lag for substrate temp
_SOLAR_GAIN_COEFF = 0.02          # °C per W/m² of effective solar entering
_HEATING_OFFSET = 45.0            # °C shift at 100 % — sized to hold 20 °C at −25 °C outdoor
_COOLING_OFFSET = 20.0            # °C shift at 100 % — pad-fan + active chiller
_VENT_TAU_FACTOR = 3.0            # vents at 100 % make heat exchange 4× faster

_DEAD_BAND = 0.5                  # ±°C around setpoint
_CURTAIN_TEMP_THRESHOLD = 5.0     # outdoor °C
_CURTAIN_SOLAR_THRESHOLD = 50.0   # W/m²
_LIGHT_PAR_THRESHOLD = 200.0      # µmol/m²/s
_DAYTIME_START = 6                # hour (inclusive)
_DAYTIME_END = 22                 # hour (exclusive)

_OUTDOOR_CO2 = 420.0              # ambient CO₂ ppm
_CO2_CONSUMPTION_RATE = 50.0      # ppm/hr at full light per zone
_CO2_INJECTION_RATE = 200.0       # ppm/hr at full injection output
_SUPPLEMENTAL_PAR = 350.0         # µmol/m²/s from grow lights
_SUPPLEMENTAL_WATTS = 600.0       # watts of supplemental lights

_TRANSPIRATION_BASE = 2.0         # %RH/hr base transpiration rate
_MOISTURE_DECAY_RATE = 3.0        # %VWC/hr between irrigations
_MOISTURE_GAIN_RATE = 15.0        # %VWC/hr during irrigation

# Irrigation schedule parameters by crop category
_IRRIGATION_SCHEDULES: dict[str, tuple[int, int]] = {
    # (cycle_interval_minutes, duration_minutes)
    "leafy_green": (180, 10),     # every 3 h, 10 min
    "vine_crop":   (240, 15),     # every 4 h, 15 min
    "berry":       (240, 15),     # same as vine
}

_IRRIGATION_FLOW = 25.0           # L/min during irrigation

_MAX_DT = 300.0                   # cap dt to 5 minutes


# ---------------------------------------------------------------------------
# Sensor-field mapping (sensor_type → ZoneState attribute suffix)
# ---------------------------------------------------------------------------

_SENSOR_FIELD_MAP: dict[str, str] = {
    "air_temperature":       "temp",
    "air_humidity":          "humidity",
    "co2_level":             "co2",
    "par_light":             "par",
    "substrate_temperature": "substrate_temp",
    "substrate_moisture":    "substrate_moisture",
    "substrate_ec":          "ec",
    "substrate_ph":          "ph",
    "water_flow_rate":       "water_flow",
    "vpd":                   "vpd",
}


# ---------------------------------------------------------------------------
# Helper: VPD calculation
# ---------------------------------------------------------------------------

def _calc_vpd(temp_c: float, rh_pct: float) -> float:
    """Compute Vapor Pressure Deficit in kPa."""
    svp = 0.6108 * math.exp(17.27 * temp_c / (temp_c + 237.3))
    return svp * (1.0 - rh_pct / 100.0)


# ---------------------------------------------------------------------------
# SensorGenerator
# ---------------------------------------------------------------------------


class SensorGenerator:
    """Generates realistic sensor telemetry for all greenhouse zones.

    Maintains internal zone states and simulates physics-based
    environmental responses to weather and HVAC control.
    """

    def __init__(self, seed: int | None = None) -> None:
        self._rng = np.random.default_rng(seed)
        self._zone_states: dict[str, ZoneState] = {}
        self._last_timestamp: datetime | None = None

        for zone_id, zone_config in ZONES.items():
            self._zone_states[zone_id] = ZoneState.from_config(zone_config)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def tick(
        self,
        timestamp: datetime,
        weather: dict[str, WeatherReading],
    ) -> list[SensorReading]:
        """Advance simulation by one tick and return readings for all zones.

        Args:
            timestamp: Current simulation time (UTC).
            weather:   Dict of *greenhouse_id* → WeatherReading.

        Returns:
            A list of :class:`SensorReading` (one per zone, 16 total).
        """
        # Calculate dt -------------------------------------------------------
        if self._last_timestamp is not None:
            dt = (timestamp - self._last_timestamp).total_seconds()
            dt = max(0.0, min(dt, _MAX_DT))
        else:
            dt = 30.0  # default first tick
        self._last_timestamp = timestamp

        readings: list[SensorReading] = []

        for zone_id, state in self._zone_states.items():
            zone_cfg = ZONES[zone_id]
            crop = CROPS[state.crop_id]
            wx = weather.get(zone_cfg.greenhouse_id)
            if wx is None:
                # Should not happen in normal operation; skip zone
                readings.append(state.to_sensor_reading(timestamp))
                continue

            # 1. HVAC control logic
            self._update_hvac(state, crop, wx, timestamp, dt)

            # 2. Temperature physics
            self._update_temperature(state, crop, wx, dt)

            # 3. Humidity physics
            self._update_humidity(state, crop, wx, dt)

            # 4. CO₂ physics
            self._update_co2(state, crop, wx, dt)

            # 5. PAR light
            self._update_par(state, wx)

            # 6. Substrate (temp lag, moisture, EC, pH)
            self._update_substrate(state, crop, wx, timestamp, dt)

            # 7. VPD
            state.current_vpd = _calc_vpd(state.current_temp, state.current_humidity)

            # 8. Create noisy reading
            reading = self._noisy_reading(state, timestamp)
            readings.append(reading)

        return readings

    def get_equipment_states(self, timestamp: datetime) -> list[EquipmentState]:
        """Return current equipment states for all zones."""
        return [
            state.to_equipment_state(timestamp)
            for state in self._zone_states.values()
        ]

    def get_zone_state(self, zone_id: str) -> ZoneState:
        """Return current state for a specific zone."""
        return self._zone_states[zone_id]

    # ------------------------------------------------------------------
    # HVAC control logic
    # ------------------------------------------------------------------

    def _update_hvac(
        self,
        state: ZoneState,
        crop: CropConfig,
        wx: WeatherReading,
        ts: datetime,
        dt: float,
    ) -> None:
        setpoint = crop.optimal_temp
        indoor = state.current_temp
        outdoor = wx.outside_temperature

        # Compute the solar offset that temperature physics will see
        curtain_f = 1.0 - 0.6 * (state.curtain_deployed / 100.0)
        solar_offset = wx.solar_radiation * _GLASS_TRANSMITTANCE * curtain_f * _SOLAR_GAIN_COEFF

        # How much HVAC offset is needed so T_eq ≈ setpoint?
        # T_eq = outdoor + solar_offset + (heat/100)*H - (cool/100)*C
        needed = setpoint - outdoor - solar_offset

        # Smooth ramp rate (fraction of error corrected per tick)
        ramp = min(1.0, 0.3 * dt / 30.0)

        if needed > _DEAD_BAND:
            # Need heating
            target_heat = min(100.0, max(0.0, needed / _HEATING_OFFSET * 100.0))
            state.heating_output += (target_heat - state.heating_output) * ramp
            state.cooling_output *= (1.0 - ramp * 0.8)
            state.vent_position *= (1.0 - ramp * 0.6)
        elif needed < -_DEAD_BAND:
            # Need cooling
            target_cool = min(100.0, max(0.0, -needed / _COOLING_OFFSET * 100.0))
            state.cooling_output += (target_cool - state.cooling_output) * ramp
            state.heating_output *= (1.0 - ramp * 0.8)
            # Vents help when outdoor < indoor
            if outdoor < indoor:
                vent_target = min(100.0, (indoor - outdoor) * 5.0)
                state.vent_position += (vent_target - state.vent_position) * ramp * 0.7
            else:
                state.vent_position *= (1.0 - ramp * 0.4)
        else:
            # Dead band — gently ramp down
            state.heating_output *= (1.0 - ramp * 0.3)
            state.cooling_output *= (1.0 - ramp * 0.3)
            state.vent_position *= (1.0 - ramp * 0.2)

        state.heating_output = float(np.clip(state.heating_output, 0.0, 100.0))
        state.cooling_output = float(np.clip(state.cooling_output, 0.0, 100.0))
        state.vent_position = float(np.clip(state.vent_position, 0.0, 100.0))

        # --- Thermal curtain ---
        if wx.outside_temperature < _CURTAIN_TEMP_THRESHOLD or wx.solar_radiation < _CURTAIN_SOLAR_THRESHOLD:
            curtain_target = 100.0
        else:
            curtain_target = 0.0
        state.curtain_deployed += (curtain_target - state.curtain_deployed) * 0.15
        state.curtain_deployed = float(np.clip(state.curtain_deployed, 0.0, 100.0))

        # --- Supplemental lights ---
        hour = ts.hour
        is_daytime = _DAYTIME_START <= hour < _DAYTIME_END
        natural_par = wx.solar_radiation * _GLASS_TRANSMITTANCE * _SOLAR_TO_PAR
        if is_daytime and natural_par < _LIGHT_PAR_THRESHOLD:
            state.lights_on = True
            state.light_watts = _SUPPLEMENTAL_WATTS
        else:
            state.lights_on = False
            state.light_watts = 0.0

        # --- CO₂ injection ---
        if state.current_co2 < crop.optimal_co2 and state.lights_on:
            co2_error = crop.optimal_co2 - state.current_co2
            state.co2_injection = min(100.0, co2_error * 0.3)
        elif state.current_co2 < crop.optimal_co2 and natural_par > 50:
            co2_error = crop.optimal_co2 - state.current_co2
            state.co2_injection = min(80.0, co2_error * 0.2)
        else:
            state.co2_injection = max(state.co2_injection - 5.0 * (dt / 30.0), 0.0)

        # --- Irrigation ---
        category = crop.category
        cycle_min, dur_min = _IRRIGATION_SCHEDULES.get(category, (240, 15))
        minute_of_day = ts.hour * 60 + ts.minute
        if minute_of_day % cycle_min < dur_min:
            state.irrigation_on = True
            state.irrigation_flow = _IRRIGATION_FLOW
            state.current_water_flow = _IRRIGATION_FLOW
        else:
            state.irrigation_on = False
            state.irrigation_flow = 0.0
            state.current_water_flow = 0.0

        state.recirc_on = True  # always on

    # ------------------------------------------------------------------
    # Temperature physics
    # ------------------------------------------------------------------

    def _update_temperature(
        self,
        state: ZoneState,
        crop: CropConfig,
        wx: WeatherReading,
        dt: float,
    ) -> None:
        indoor = state.current_temp
        outdoor = wx.outside_temperature

        # Solar gain offset (reduced by thermal curtain)
        curtain_factor = 1.0 - 0.6 * (state.curtain_deployed / 100.0)
        solar_offset = wx.solar_radiation * _GLASS_TRANSMITTANCE * curtain_factor * _SOLAR_GAIN_COEFF

        # Effective equilibrium temperature
        T_eq = outdoor + solar_offset
        T_eq += (state.heating_output / 100.0) * _HEATING_OFFSET
        T_eq -= (state.cooling_output / 100.0) * _COOLING_OFFSET

        # Vents increase the rate of exchange with outdoor air
        vent_factor = 1.0 + (state.vent_position / 100.0) * _VENT_TAU_FACTOR
        effective_tau = _THERMAL_MASS_TAU / vent_factor

        # Exponential approach to equilibrium
        alpha = 1.0 - math.exp(-dt / effective_tau)
        state.current_temp += (T_eq - indoor) * alpha

        state.current_temp = float(np.clip(state.current_temp, 5.0, 40.0))

    # ------------------------------------------------------------------
    # Humidity physics
    # ------------------------------------------------------------------

    def _update_humidity(
        self,
        state: ZoneState,
        crop: CropConfig,
        wx: WeatherReading,
        dt: float,
    ) -> None:
        dt_hr = dt / 3600.0

        # Transpiration — higher when lights on and temp in good range
        transpiration = _TRANSPIRATION_BASE
        if state.lights_on or state.current_par > 50:
            transpiration *= 1.5
        temp_factor = 1.0 - abs(state.current_temp - crop.optimal_temp) / 15.0
        temp_factor = max(0.3, temp_factor)
        transpiration *= temp_factor

        # Irrigation adds a small humidity bump
        if state.irrigation_on:
            transpiration += 1.0

        # Vent dilution toward outdoor humidity
        vent_exchange = (state.vent_position / 100.0) * 4.0 * (wx.outside_humidity - state.current_humidity) / 100.0

        # Heating dries air (RH drops as temp rises)
        heating_drying = (state.heating_output / 100.0) * 3.0

        delta_h = (transpiration + vent_exchange * 100.0 - heating_drying) * dt_hr
        state.current_humidity += delta_h

        state.current_humidity = float(np.clip(state.current_humidity, 30.0, 98.0))

    # ------------------------------------------------------------------
    # CO₂ physics
    # ------------------------------------------------------------------

    def _update_co2(
        self,
        state: ZoneState,
        crop: CropConfig,
        wx: WeatherReading,
        dt: float,
    ) -> None:
        dt_hr = dt / 3600.0

        # Photosynthetic consumption (when light is available)
        light_fraction = min(state.current_par / 400.0, 1.0) if state.current_par > 50 else 0.0
        consumption = _CO2_CONSUMPTION_RATE * light_fraction

        # Injection
        injection = _CO2_INJECTION_RATE * (state.co2_injection / 100.0)

        # Vent dilution toward outdoor CO₂
        vent_rate = (state.vent_position / 100.0) * 0.5  # fraction exchanged per hour
        vent_dilution = vent_rate * (state.current_co2 - _OUTDOOR_CO2)

        delta_co2 = (injection - consumption - vent_dilution) * dt_hr
        state.current_co2 += delta_co2

        state.current_co2 = float(np.clip(state.current_co2, 300.0, 2500.0))

    # ------------------------------------------------------------------
    # PAR light
    # ------------------------------------------------------------------

    def _update_par(
        self,
        state: ZoneState,
        wx: WeatherReading,
    ) -> None:
        curtain_factor = 1.0 - 0.4 * (state.curtain_deployed / 100.0)
        natural_par = wx.solar_radiation * _GLASS_TRANSMITTANCE * _SOLAR_TO_PAR * curtain_factor
        supplemental = _SUPPLEMENTAL_PAR if state.lights_on else 0.0
        state.current_par = max(0.0, natural_par + supplemental)

    # ------------------------------------------------------------------
    # Substrate model
    # ------------------------------------------------------------------

    def _update_substrate(
        self,
        state: ZoneState,
        crop: CropConfig,
        wx: WeatherReading,
        ts: datetime,
        dt: float,
    ) -> None:
        dt_hr = dt / 3600.0

        # Substrate temperature follows air temp with lag
        alpha_sub = 1.0 - math.exp(-dt / _SUBSTRATE_TEMP_TAU)
        state.current_substrate_temp += (state.current_temp - state.current_substrate_temp) * alpha_sub

        # Substrate moisture
        if state.irrigation_on:
            state.current_substrate_moisture += _MOISTURE_GAIN_RATE * dt_hr
        else:
            # Decay — faster when warm, when lights on (higher transpiration)
            decay_factor = 1.0
            if state.current_temp > crop.optimal_temp:
                decay_factor += 0.3
            if state.lights_on:
                decay_factor += 0.2
            state.current_substrate_moisture -= _MOISTURE_DECAY_RATE * decay_factor * dt_hr

        state.current_substrate_moisture = float(np.clip(
            state.current_substrate_moisture, 20.0, 95.0
        ))

        # EC — small random walk around crop optimal
        ec_drift = self._rng.normal(0, 0.002) * math.sqrt(dt_hr)
        # Gentle pull toward optimal
        ec_pull = (crop.optimal_ec - state.current_ec) * 0.01 * dt_hr
        state.current_ec += ec_drift + ec_pull
        state.current_ec = float(np.clip(state.current_ec, 0.5, 5.0))

        # pH — slow drift with tiny pull to optimal
        ph_drift = self._rng.normal(0, 0.003) * math.sqrt(dt_hr)
        ph_pull = (crop.optimal_ph - state.current_ph) * 0.008 * dt_hr
        state.current_ph += ph_drift + ph_pull
        state.current_ph = float(np.clip(state.current_ph, 4.0, 8.5))

    # ------------------------------------------------------------------
    # Noisy sensor reading
    # ------------------------------------------------------------------

    def _noisy_reading(
        self,
        state: ZoneState,
        ts: datetime,
    ) -> SensorReading:
        """Create a SensorReading with gaussian noise applied."""
        values: dict[str, float] = {}
        for sensor_type, sensor_cfg in SENSORS.items():
            field_suffix = _SENSOR_FIELD_MAP[sensor_type]
            clean_value = getattr(state, f"current_{field_suffix}")
            noisy = clean_value + float(self._rng.normal(0, sensor_cfg.noise_stddev))
            noisy = max(sensor_cfg.min_value, min(sensor_cfg.max_value, noisy))
            values[sensor_type] = noisy

        return SensorReading(
            timestamp=ts,
            greenhouse_id=state.greenhouse_id,
            zone_id=state.zone_id,
            air_temperature=values["air_temperature"],
            air_humidity=values["air_humidity"],
            co2_level=values["co2_level"],
            par_light=values["par_light"],
            substrate_temperature=values["substrate_temperature"],
            substrate_moisture=values["substrate_moisture"],
            substrate_ec=values["substrate_ec"],
            substrate_ph=values["substrate_ph"],
            water_flow_rate=values["water_flow_rate"],
            vpd=values["vpd"],
        )


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from src.models.weather import WeatherGenerator

    def _run_scenario(
        label: str,
        start: datetime,
        ticks: int = 10,
        seed: int = 42,
    ) -> None:
        print(f"\n{'=' * 90}")
        print(f"  {label}")
        print(f"  Start: {start.isoformat()}")
        print(f"{'=' * 90}")

        wx_bh = WeatherGenerator("brightharvest", seed=seed)
        wx_mv = WeatherGenerator("mucci-valley", seed=seed + 1)
        sensor_gen = SensorGenerator(seed=seed)

        ts = start
        delta = timedelta(seconds=30)

        for i in range(ticks):
            weather = {
                "brightharvest": wx_bh.generate(ts),
                "mucci-valley": wx_mv.generate(ts),
            }
            readings = sensor_gen.tick(ts, weather)

            if i == 0 or i == ticks - 1:
                tag = "FIRST" if i == 0 else "LAST"
                wx_sample = weather["brightharvest"]
                print(f"\n  [{tag} tick] {ts.isoformat()}")
                print(f"  Outside: {wx_sample.outside_temperature:.1f}°C  "
                      f"Solar: {wx_sample.solar_radiation:.0f} W/m²")
                print(
                    f"  {'Zone':<8} {'Temp °C':>8} {'Humid %':>8} "
                    f"{'CO₂ ppm':>8} {'PAR':>6} {'Heat%':>6} {'Cool%':>6} "
                    f"{'Vent%':>6} {'Lights':>6}"
                )
                print("  " + "-" * 76)
                for r in readings:
                    zs = sensor_gen.get_zone_state(r.zone_id)
                    print(
                        f"  {r.zone_id:<8} {r.air_temperature:>8.1f} "
                        f"{r.air_humidity:>8.1f} {r.co2_level:>8.0f} "
                        f"{r.par_light:>6.0f} {zs.heating_output:>6.1f} "
                        f"{zs.cooling_output:>6.1f} {zs.vent_position:>6.1f} "
                        f"{'ON' if zs.lights_on else 'off':>6}"
                    )

            ts += delta

    # Winter afternoon — expect heating active, lights on
    _run_scenario(
        "WINTER AFTERNOON — Jan 15, 2:00 PM UTC (8 AM Chicago / 9 AM Toronto)",
        datetime(2025, 1, 15, 14, 0, tzinfo=timezone.utc),
    )

    # Summer afternoon — expect cooling active, vents open, natural light
    _run_scenario(
        "SUMMER AFTERNOON — Jul 15, 7:00 PM UTC (2 PM Chicago / 3 PM Toronto)",
        datetime(2025, 7, 15, 19, 0, tzinfo=timezone.utc),
    )
