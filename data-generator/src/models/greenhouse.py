"""Runtime data models for greenhouse zones and telemetry.

These are mutable dataclasses representing point-in-time snapshots of
sensor readings, equipment state, weather, harvest, energy, shipments,
and crop health.  Distinct from the frozen config dataclasses in config.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from src.config import (
    CropConfig,
    GreenhouseConfig,
    ZoneConfig,
    get_crop_for_zone,
)


# ---------------------------------------------------------------------------
# Telemetry snapshots
# ---------------------------------------------------------------------------


@dataclass
class SensorReading:
    """A single sensor reading from one zone at one point in time."""

    timestamp: datetime
    greenhouse_id: str
    zone_id: str
    air_temperature: float  # °C
    air_humidity: float  # %RH
    co2_level: float  # ppm
    par_light: float  # µmol/m²/s
    substrate_temperature: float  # °C
    substrate_moisture: float  # %VWC
    substrate_ec: float  # mS/cm
    substrate_ph: float  # pH
    water_flow_rate: float  # L/min
    vpd: float  # kPa

    def to_dict(self) -> dict:
        """Convert to flat dict for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "greenhouse_id": self.greenhouse_id,
            "zone_id": self.zone_id,
            "air_temperature": round(self.air_temperature, 2),
            "air_humidity": round(self.air_humidity, 1),
            "co2_level": round(self.co2_level, 0),
            "par_light": round(self.par_light, 1),
            "substrate_temperature": round(self.substrate_temperature, 2),
            "substrate_moisture": round(self.substrate_moisture, 1),
            "substrate_ec": round(self.substrate_ec, 3),
            "substrate_ph": round(self.substrate_ph, 2),
            "water_flow_rate": round(self.water_flow_rate, 1),
            "vpd": round(self.vpd, 3),
        }


@dataclass
class EquipmentState:
    """Equipment status for one zone at one point in time."""

    timestamp: datetime
    greenhouse_id: str
    zone_id: str
    heating_output: float  # 0-100 %
    cooling_output: float  # 0-100 %
    vent_position: float  # 0-100 % open
    supplemental_light: bool
    supplemental_light_watts: float
    co2_injection_rate: float  # kg/hr
    thermal_curtain: float  # 0-100 % deployed
    irrigation_pump: bool
    irrigation_flow: float  # L/min
    recirc_pump: bool

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "greenhouse_id": self.greenhouse_id,
            "zone_id": self.zone_id,
            "heating_output": round(self.heating_output, 1),
            "cooling_output": round(self.cooling_output, 1),
            "vent_position": round(self.vent_position, 1),
            "supplemental_light": self.supplemental_light,
            "supplemental_light_watts": round(self.supplemental_light_watts, 0),
            "co2_injection_rate": round(self.co2_injection_rate, 2),
            "thermal_curtain": round(self.thermal_curtain, 1),
            "irrigation_pump": self.irrigation_pump,
            "irrigation_flow": round(self.irrigation_flow, 1),
            "recirc_pump": self.recirc_pump,
        }


@dataclass
class WeatherReading:
    """External weather conditions at one greenhouse location."""

    timestamp: datetime
    greenhouse_id: str
    outside_temperature: float  # °C
    outside_humidity: float  # %RH
    wind_speed: float  # km/h
    wind_direction: float  # degrees (0-360)
    precipitation: float  # mm/hr
    solar_radiation: float  # W/m²
    barometric_pressure: float  # hPa
    cloud_cover: float  # 0-100 %

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "greenhouse_id": self.greenhouse_id,
            "outside_temperature": round(self.outside_temperature, 1),
            "outside_humidity": round(self.outside_humidity, 1),
            "wind_speed": round(self.wind_speed, 1),
            "wind_direction": round(self.wind_direction, 0),
            "precipitation": round(self.precipitation, 2),
            "solar_radiation": round(self.solar_radiation, 1),
            "barometric_pressure": round(self.barometric_pressure, 1),
            "cloud_cover": round(self.cloud_cover, 0),
        }


# ---------------------------------------------------------------------------
# Operations snapshots
# ---------------------------------------------------------------------------


@dataclass
class DailyHarvest:
    """Daily harvest report for one zone."""

    date: date
    greenhouse_id: str
    zone_id: str
    crop_id: str
    harvest_weight_kg: float
    harvest_units: int
    grade_a_pct: float  # 0-100
    grade_b_pct: float
    grade_c_pct: float
    waste_kg: float
    waste_pct: float  # derived
    days_to_harvest: int  # days since planting
    revenue_estimate_usd: float  # harvest_weight_kg * market_price

    def to_dict(self) -> dict:
        return {
            "date": self.date.isoformat(),
            "greenhouse_id": self.greenhouse_id,
            "zone_id": self.zone_id,
            "crop_id": self.crop_id,
            "harvest_weight_kg": round(self.harvest_weight_kg, 1),
            "harvest_units": self.harvest_units,
            "grade_a_pct": round(self.grade_a_pct, 1),
            "grade_b_pct": round(self.grade_b_pct, 1),
            "grade_c_pct": round(self.grade_c_pct, 1),
            "waste_kg": round(self.waste_kg, 1),
            "waste_pct": round(self.waste_pct, 1),
            "days_to_harvest": self.days_to_harvest,
            "revenue_estimate_usd": round(self.revenue_estimate_usd, 2),
        }


@dataclass
class EnergyReading:
    """Hourly energy consumption for one greenhouse."""

    timestamp: datetime
    greenhouse_id: str
    electricity_kwh: float
    natural_gas_m3: float
    water_liters: float
    water_recycled_liters: float
    co2_purchased_kg: float

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "greenhouse_id": self.greenhouse_id,
            "electricity_kwh": round(self.electricity_kwh, 2),
            "natural_gas_m3": round(self.natural_gas_m3, 2),
            "water_liters": round(self.water_liters, 1),
            "water_recycled_liters": round(self.water_recycled_liters, 1),
            "co2_purchased_kg": round(self.co2_purchased_kg, 2),
        }


@dataclass
class ShipmentEvent:
    """A shipment/delivery event."""

    order_id: str
    greenhouse_id: str
    customer_id: str
    product_sku: str
    crop_id: str
    quantity_cases: int
    ship_date: datetime
    expected_delivery: datetime
    actual_delivery: datetime | None
    cold_chain_temp: float  # °C — current/final reading
    cold_chain_compliant: bool
    delivery_status: str  # "pending", "in_transit", "delivered", "delayed"
    shelf_life_remaining_days: int

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "greenhouse_id": self.greenhouse_id,
            "customer_id": self.customer_id,
            "product_sku": self.product_sku,
            "crop_id": self.crop_id,
            "quantity_cases": self.quantity_cases,
            "ship_date": self.ship_date.isoformat(),
            "expected_delivery": self.expected_delivery.isoformat(),
            "actual_delivery": (
                self.actual_delivery.isoformat() if self.actual_delivery else None
            ),
            "cold_chain_temp": round(self.cold_chain_temp, 1),
            "cold_chain_compliant": self.cold_chain_compliant,
            "delivery_status": self.delivery_status,
            "shelf_life_remaining_days": self.shelf_life_remaining_days,
        }


@dataclass
class WeeklyCropHealth:
    """Weekly crop health measurements for one zone."""

    week_start: date
    greenhouse_id: str
    zone_id: str
    crop_id: str
    avg_plant_height_cm: float
    avg_leaf_count: float
    predicted_quality_grade: str  # "A", "B", "C"
    predicted_yield_kg: float

    def to_dict(self) -> dict:
        return {
            "week_start": self.week_start.isoformat(),
            "greenhouse_id": self.greenhouse_id,
            "zone_id": self.zone_id,
            "crop_id": self.crop_id,
            "avg_plant_height_cm": round(self.avg_plant_height_cm, 1),
            "avg_leaf_count": round(self.avg_leaf_count, 1),
            "predicted_quality_grade": self.predicted_quality_grade,
            "predicted_yield_kg": round(self.predicted_yield_kg, 1),
        }


# ---------------------------------------------------------------------------
# Zone runtime state
# ---------------------------------------------------------------------------


@dataclass
class ZoneState:
    """Mutable runtime state for a single greenhouse zone.

    This tracks the current environmental conditions, equipment state,
    and crop progress. Updated every simulation tick.
    """

    zone_id: str
    greenhouse_id: str
    crop_id: str

    # Current environmental conditions (updated from sensor generation)
    current_temp: float = 20.0
    current_humidity: float = 70.0
    current_co2: float = 800.0
    current_par: float = 200.0
    current_substrate_temp: float = 20.0
    current_substrate_moisture: float = 60.0
    current_ec: float = 2.0
    current_ph: float = 6.0
    current_water_flow: float = 0.0
    current_vpd: float = 0.8

    # Equipment state
    heating_output: float = 0.0
    cooling_output: float = 0.0
    vent_position: float = 0.0
    lights_on: bool = False
    light_watts: float = 0.0
    co2_injection: float = 0.0
    curtain_deployed: float = 0.0
    irrigation_on: bool = False
    irrigation_flow: float = 0.0
    recirc_on: bool = True

    # Crop tracking
    planting_date: date | None = None
    days_since_planting: int = 0
    cumulative_dli: float = 0.0  # Daily Light Integral accumulator

    @classmethod
    def from_config(cls, zone_config: ZoneConfig) -> ZoneState:
        """Create initial zone state from config."""
        crop = get_crop_for_zone(zone_config.zone_id)
        return cls(
            zone_id=zone_config.zone_id,
            greenhouse_id=zone_config.greenhouse_id,
            crop_id=crop.crop_id,
            current_temp=crop.optimal_temp,
            current_humidity=crop.optimal_humidity,
            current_co2=float(crop.optimal_co2),
            current_ec=crop.optimal_ec,
            current_ph=crop.optimal_ph,
        )

    def to_sensor_reading(self, timestamp: datetime) -> SensorReading:
        """Snapshot current state as a SensorReading."""
        return SensorReading(
            timestamp=timestamp,
            greenhouse_id=self.greenhouse_id,
            zone_id=self.zone_id,
            air_temperature=self.current_temp,
            air_humidity=self.current_humidity,
            co2_level=self.current_co2,
            par_light=self.current_par,
            substrate_temperature=self.current_substrate_temp,
            substrate_moisture=self.current_substrate_moisture,
            substrate_ec=self.current_ec,
            substrate_ph=self.current_ph,
            water_flow_rate=self.current_water_flow,
            vpd=self.current_vpd,
        )

    def to_equipment_state(self, timestamp: datetime) -> EquipmentState:
        """Snapshot current state as an EquipmentState."""
        return EquipmentState(
            timestamp=timestamp,
            greenhouse_id=self.greenhouse_id,
            zone_id=self.zone_id,
            heating_output=self.heating_output,
            cooling_output=self.cooling_output,
            vent_position=self.vent_position,
            supplemental_light=self.lights_on,
            supplemental_light_watts=self.light_watts,
            co2_injection_rate=self.co2_injection,
            thermal_curtain=self.curtain_deployed,
            irrigation_pump=self.irrigation_on,
            irrigation_flow=self.irrigation_flow,
            recirc_pump=self.recirc_on,
        )
