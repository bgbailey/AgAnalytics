"""Anomaly scenario definitions — 4 scenarios for demo.

Each scenario defines an apply_fn that mutates sensor readings based on
the anomaly phase (onset/peak/recovery) and progress within that phase.
"""
from __future__ import annotations
from src.anomalies.engine import AnomalyProfile, AnomalyPhase
import numpy as np


def _hvac_failure_apply(readings: dict, phase: AnomalyPhase, progress: float, 
                         severity: float, rng: np.random.Generator) -> dict:
    """HVAC Failure: Boiler goes offline, temperature drops.
    
    Onset (0-20%): Temp starts dropping ~0.5°C per progress step
    Peak (20-70%): Temp 4-8°C below setpoint, humidity spikes to 90%+
    Recovery (70-100%): Backup heating ramps, temp slowly returns
    """
    r = readings.copy()
    max_drop = 8.0 * severity  # up to 8°C drop at full severity
    
    if phase == AnomalyPhase.ONSET:
        drop = max_drop * 0.5 * progress  # ramping down
        r["air_temperature"] -= drop
        r["air_humidity"] += 5 * progress * severity
        r["heating_output"] = 0.0  # heater is offline
        
    elif phase == AnomalyPhase.PEAK:
        drop = max_drop * (0.5 + 0.5 * min(progress, 1.0))
        r["air_temperature"] -= drop + rng.normal(0, 0.3)
        r["air_humidity"] = min(98, r["air_humidity"] + 15 * severity)
        r["heating_output"] = 0.0
        # VPD collapses when temp drops and humidity rises
        r["vpd"] *= max(0.1, 1 - 0.7 * severity)
        
    elif phase == AnomalyPhase.RECOVERY:
        remaining = 1.0 - progress  # goes from 1→0 during recovery
        drop = max_drop * remaining * 0.5
        r["air_temperature"] -= drop
        r["air_humidity"] += 5 * remaining * severity
        r["heating_output"] = progress * 80  # backup ramps up
    
    return r


HVAC_FAILURE = AnomalyProfile(
    scenario_name="hvac-failure",
    display_name="HVAC Failure",
    description="Boiler/heating system goes offline. Temperature drops rapidly, humidity spikes.",
    affected_sensors=["air_temperature", "air_humidity", "vpd", "heating_output"],
    onset_fraction=0.15,
    peak_fraction=0.45,
    apply_fn=_hvac_failure_apply,
)


def _nutrient_drift_apply(readings: dict, phase: AnomalyPhase, progress: float,
                           severity: float, rng: np.random.Generator) -> dict:
    """Nutrient Drift: pH slowly rises, EC drops — contaminated water source.
    
    Onset (0-30%): pH starts creeping up slowly
    Peak (30-70%): pH at 7.2-7.8 (normal ~6.0), EC dropping
    Recovery (70-100%): Gradual return after flushing (or manual resolve)
    """
    r = readings.copy()
    max_ph_rise = 1.8 * severity  # up to pH 7.8 from 6.0
    max_ec_drop = 1.2 * severity   # EC drops as nutrients precipitate
    
    if phase == AnomalyPhase.ONSET:
        r["substrate_ph"] += max_ph_rise * 0.3 * progress
        r["substrate_ec"] -= max_ec_drop * 0.2 * progress
        
    elif phase == AnomalyPhase.PEAK:
        r["substrate_ph"] += max_ph_rise * (0.3 + 0.7 * min(progress, 1.0))
        r["substrate_ec"] -= max_ec_drop * (0.2 + 0.8 * progress)
        r["substrate_ph"] += rng.normal(0, 0.05)  # jitter
        
    elif phase == AnomalyPhase.RECOVERY:
        remaining = 1.0 - progress
        r["substrate_ph"] += max_ph_rise * remaining * 0.5
        r["substrate_ec"] -= max_ec_drop * remaining * 0.3
    
    return r


NUTRIENT_DRIFT = AnomalyProfile(
    scenario_name="nutrient-drift",
    display_name="Nutrient System Drift",
    description="pH rises slowly from contaminated water source. EC drops as nutrients precipitate.",
    affected_sensors=["substrate_ph", "substrate_ec"],
    onset_fraction=0.30,
    peak_fraction=0.40,
    apply_fn=_nutrient_drift_apply,
)


def _irrigation_failure_apply(readings: dict, phase: AnomalyPhase, progress: float,
                               severity: float, rng: np.random.Generator) -> dict:
    """Irrigation Failure: Pump fails, water flow drops to zero.
    
    Onset (0-10%): Flow drops rapidly
    Peak (10-60%): Zero flow, moisture drops, EC spikes (concentrating)
    Recovery (60-100%): Backup pump activates, slow moisture recovery
    """
    r = readings.copy()
    
    if phase == AnomalyPhase.ONSET:
        r["water_flow_rate"] *= (1 - progress)  # drops to 0
        r["irrigation_pump"] = progress < 0.5  # turns off
        
    elif phase == AnomalyPhase.PEAK:
        r["water_flow_rate"] = 0.0
        r["irrigation_pump"] = False
        moisture_drop = 20 * severity * progress  # moisture drops over time
        r["substrate_moisture"] = max(15, r["substrate_moisture"] - moisture_drop)
        ec_spike = 1.0 * severity * progress  # EC concentrates
        r["substrate_ec"] += ec_spike
        
    elif phase == AnomalyPhase.RECOVERY:
        r["water_flow_rate"] = progress * 25  # backup pump ramps
        r["irrigation_pump"] = True
        remaining = 1.0 - progress
        r["substrate_moisture"] = max(15, r["substrate_moisture"] - 10 * remaining * severity)
        r["substrate_ec"] += 0.5 * remaining * severity
    
    return r


IRRIGATION_FAILURE = AnomalyProfile(
    scenario_name="irrigation-failure",
    display_name="Irrigation Pump Failure",
    description="Main irrigation pump fails. Water flow drops to zero, substrate dries, nutrients concentrate.",
    affected_sensors=["water_flow_rate", "substrate_moisture", "substrate_ec"],
    onset_fraction=0.10,
    peak_fraction=0.50,
    apply_fn=_irrigation_failure_apply,
)


def _cold_chain_break_apply(readings: dict, phase: AnomalyPhase, progress: float,
                             severity: float, rng: np.random.Generator) -> dict:
    """Cold Chain Break: Truck refrigeration fails during transit.
    
    Note: This affects ShipmentEvent data, not zone sensors.
    The apply_fn here modifies a cold_chain_temp reading.
    
    Onset (0-20%): Temp starts rising from 2°C
    Peak (20-80%): Temp at 10-15°C (critical for produce)
    Recovery (80-100%): Truck arrives, temp logged as final
    """
    r = readings.copy()
    max_temp_rise = 12.0 * severity  # up to 14°C total (2+12)
    
    if phase == AnomalyPhase.ONSET:
        r["cold_chain_temp"] = 2.0 + max_temp_rise * 0.3 * progress
        
    elif phase == AnomalyPhase.PEAK:
        r["cold_chain_temp"] = 2.0 + max_temp_rise * (0.3 + 0.7 * progress) + rng.normal(0, 0.5)
        
    elif phase == AnomalyPhase.RECOVERY:
        r["cold_chain_temp"] = 2.0 + max_temp_rise * 0.8  # stays high, damage done
    
    r["cold_chain_compliant"] = r.get("cold_chain_temp", 2.0) <= 5.0
    return r


COLD_CHAIN_BREAK = AnomalyProfile(
    scenario_name="cold-chain-break",
    display_name="Cold Chain Break",
    description="Truck refrigeration fails during delivery. Produce temperature rises above safe limits.",
    affected_sensors=["cold_chain_temp"],
    onset_fraction=0.20,
    peak_fraction=0.60,
    apply_fn=_cold_chain_break_apply,
)


# Registry of all scenarios
ALL_SCENARIOS: dict[str, AnomalyProfile] = {
    "hvac-failure": HVAC_FAILURE,
    "nutrient-drift": NUTRIENT_DRIFT,
    "irrigation-failure": IRRIGATION_FAILURE,
    "cold-chain-break": COLD_CHAIN_BREAK,
}


def register_all(engine) -> None:
    """Register all scenario profiles with an AnomalyEngine."""
    for profile in ALL_SCENARIOS.values():
        engine.register_profile(profile)
