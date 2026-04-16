"""Configuration constants for the AgriTech Analytics greenhouse data generator.

Single source of truth for ALL constants: greenhouses, crops, zones, sensors,
equipment, customers, weather parameters, and timing. All brand names are
fictional (AgriTech Analytics, BrightHarvest Greens, Mucci Valley Farms).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Dataclass definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GreenhouseConfig:
    """Immutable configuration for a single greenhouse facility."""

    greenhouse_id: str
    name: str
    location_city: str
    location_state: str
    country: str
    latitude: float
    longitude: float
    size_sqft: int
    zone_count: int
    growing_method: str
    timezone: str


@dataclass(frozen=True)
class CropConfig:
    """Immutable configuration for a crop type."""

    crop_id: str
    name: str
    display_name: str
    category: str  # "leafy_green", "vine_crop", or "berry"
    growth_cycle_days: int
    optimal_temp: float  # °C
    optimal_humidity: float  # %
    optimal_co2: int  # ppm
    optimal_ph: float
    optimal_ec: float  # mS/cm
    market_price_per_kg: float  # USD
    shelf_life_days: int
    greenhouse_id: str


@dataclass(frozen=True)
class ZoneConfig:
    """Immutable configuration for a grow zone inside a greenhouse."""

    zone_id: str  # e.g. "BH-Z01"
    greenhouse_id: str
    zone_name: str  # e.g. "Zone 1"
    size_sqft: int
    primary_crop_id: str
    adjacent_zones: tuple[str, ...]


@dataclass(frozen=True)
class SensorConfig:
    """Immutable configuration for a sensor type."""

    sensor_type: str
    unit: str
    display_name: str
    min_value: float
    max_value: float
    noise_stddev: float
    reading_interval_seconds: int


@dataclass(frozen=True)
class EquipmentConfig:
    """Immutable configuration for an equipment type."""

    equipment_type: str
    display_name: str
    unit: str
    has_backup: bool


@dataclass(frozen=True)
class CustomerConfig:
    """Immutable configuration for a retail customer."""

    customer_id: str
    name: str
    region: str
    country: str
    delivery_frequency: str  # "daily", "3x_weekly", "weekly"


@dataclass(frozen=True)
class WeatherParams:
    """Immutable parameters driving the external weather simulation."""

    greenhouse_id: str
    temp_annual_mean: float  # °C
    temp_annual_amplitude: float  # °C (half of annual range)
    temp_daily_amplitude: float  # °C (day-night swing)
    humidity_summer_mean: float
    humidity_winter_mean: float
    peak_month: float  # 0 = Jan, 6 = Jul


# ---------------------------------------------------------------------------
# 1. Greenhouses
# ---------------------------------------------------------------------------

GREENHOUSES: dict[str, GreenhouseConfig] = {
    "brightharvest": GreenhouseConfig(
        greenhouse_id="brightharvest",
        name="BrightHarvest Greens",
        location_city="Rochelle",
        location_state="Illinois",
        country="US",
        latitude=41.92,
        longitude=-89.07,
        size_sqft=200_000,
        zone_count=8,
        growing_method="Hydroponic NFT",
        timezone="America/Chicago",
    ),
    "mucci-valley": GreenhouseConfig(
        greenhouse_id="mucci-valley",
        name="Mucci Valley Farms",
        location_city="Kingsville",
        location_state="Ontario",
        country="Canada",
        latitude=42.04,
        longitude=-82.74,
        size_sqft=250_000,
        zone_count=8,
        growing_method="Hydroponic Drip Irrigation",
        timezone="America/Toronto",
    ),
}

# ---------------------------------------------------------------------------
# 2. Crops
# ---------------------------------------------------------------------------

CROPS: dict[str, CropConfig] = {
    "baby_spinach": CropConfig(
        crop_id="baby_spinach",
        name="baby_spinach",
        display_name="Baby Spinach",
        category="leafy_green",
        growth_cycle_days=30,
        optimal_temp=18.0,
        optimal_humidity=70.0,
        optimal_co2=800,
        optimal_ph=6.0,
        optimal_ec=1.8,
        market_price_per_kg=4.40,
        shelf_life_days=10,
        greenhouse_id="brightharvest",
    ),
    "romaine": CropConfig(
        crop_id="romaine",
        name="romaine",
        display_name="Romaine Lettuce",
        category="leafy_green",
        growth_cycle_days=35,
        optimal_temp=20.0,
        optimal_humidity=65.0,
        optimal_co2=900,
        optimal_ph=6.2,
        optimal_ec=2.0,
        market_price_per_kg=3.80,
        shelf_life_days=12,
        greenhouse_id="brightharvest",
    ),
    "arugula": CropConfig(
        crop_id="arugula",
        name="arugula",
        display_name="Arugula",
        category="leafy_green",
        growth_cycle_days=25,
        optimal_temp=19.0,
        optimal_humidity=68.0,
        optimal_co2=850,
        optimal_ph=6.0,
        optimal_ec=1.6,
        market_price_per_kg=5.20,
        shelf_life_days=8,
        greenhouse_id="brightharvest",
    ),
    "basil": CropConfig(
        crop_id="basil",
        name="basil",
        display_name="Basil",
        category="leafy_green",
        growth_cycle_days=28,
        optimal_temp=24.0,
        optimal_humidity=60.0,
        optimal_co2=1000,
        optimal_ph=5.8,
        optimal_ec=1.4,
        market_price_per_kg=8.80,
        shelf_life_days=7,
        greenhouse_id="brightharvest",
    ),
    "cocktail_tomato": CropConfig(
        crop_id="cocktail_tomato",
        name="cocktail_tomato",
        display_name="Cocktail Tomato",
        category="vine_crop",
        growth_cycle_days=270,
        optimal_temp=23.0,
        optimal_humidity=70.0,
        optimal_co2=1000,
        optimal_ph=5.8,
        optimal_ec=2.8,
        market_price_per_kg=5.50,
        shelf_life_days=14,
        greenhouse_id="mucci-valley",
    ),
    "bell_pepper": CropConfig(
        crop_id="bell_pepper",
        name="bell_pepper",
        display_name="Bell Pepper",
        category="vine_crop",
        growth_cycle_days=240,
        optimal_temp=22.0,
        optimal_humidity=65.0,
        optimal_co2=900,
        optimal_ph=6.0,
        optimal_ec=2.5,
        market_price_per_kg=4.20,
        shelf_life_days=14,
        greenhouse_id="mucci-valley",
    ),
    "mini_cucumber": CropConfig(
        crop_id="mini_cucumber",
        name="mini_cucumber",
        display_name="Mini Cucumber",
        category="vine_crop",
        growth_cycle_days=180,
        optimal_temp=24.0,
        optimal_humidity=75.0,
        optimal_co2=900,
        optimal_ph=5.8,
        optimal_ec=2.2,
        market_price_per_kg=3.60,
        shelf_life_days=10,
        greenhouse_id="mucci-valley",
    ),
    "strawberry": CropConfig(
        crop_id="strawberry",
        name="strawberry",
        display_name="Strawberry",
        category="berry",
        growth_cycle_days=300,
        optimal_temp=20.0,
        optimal_humidity=65.0,
        optimal_co2=800,
        optimal_ph=5.8,
        optimal_ec=1.4,
        market_price_per_kg=9.00,
        shelf_life_days=5,
        greenhouse_id="mucci-valley",
    ),
}

# ---------------------------------------------------------------------------
# 3. Zones
# ---------------------------------------------------------------------------

_BH_ZONE_CROPS = [
    "baby_spinach", "baby_spinach",
    "romaine", "romaine",
    "arugula", "arugula",
    "basil", "basil",
]

_MV_ZONE_CROPS = [
    "cocktail_tomato", "cocktail_tomato",
    "bell_pepper", "bell_pepper",
    "mini_cucumber", "mini_cucumber",
    "strawberry", "strawberry",
]


def _build_zones() -> dict[str, ZoneConfig]:
    """Generate the 16 zone configs with adjacency information."""
    zones: dict[str, ZoneConfig] = {}

    for prefix, gh_id, crop_list, sqft in [
        ("BH", "brightharvest", _BH_ZONE_CROPS, 25_000),
        ("MV", "mucci-valley", _MV_ZONE_CROPS, 31_250),
    ]:
        zone_ids = [f"{prefix}-Z{i:02d}" for i in range(1, 9)]
        for idx, zid in enumerate(zone_ids):
            adj: list[str] = []
            if idx > 0:
                adj.append(zone_ids[idx - 1])
            if idx < len(zone_ids) - 1:
                adj.append(zone_ids[idx + 1])
            zones[zid] = ZoneConfig(
                zone_id=zid,
                greenhouse_id=gh_id,
                zone_name=f"Zone {idx + 1}",
                size_sqft=sqft,
                primary_crop_id=crop_list[idx],
                adjacent_zones=tuple(adj),
            )
    return zones


ZONES: dict[str, ZoneConfig] = _build_zones()

# ---------------------------------------------------------------------------
# 4. Sensors
# ---------------------------------------------------------------------------

SENSORS: dict[str, SensorConfig] = {
    "air_temperature": SensorConfig(
        sensor_type="air_temperature",
        unit="°C",
        display_name="Air Temperature",
        min_value=5.0,
        max_value=40.0,
        noise_stddev=0.3,
        reading_interval_seconds=30,
    ),
    "air_humidity": SensorConfig(
        sensor_type="air_humidity",
        unit="%RH",
        display_name="Air Humidity",
        min_value=20.0,
        max_value=100.0,
        noise_stddev=1.5,
        reading_interval_seconds=30,
    ),
    "co2_level": SensorConfig(
        sensor_type="co2_level",
        unit="ppm",
        display_name="CO₂ Level",
        min_value=300.0,
        max_value=2500.0,
        noise_stddev=15.0,
        reading_interval_seconds=30,
    ),
    "par_light": SensorConfig(
        sensor_type="par_light",
        unit="µmol/m²/s",
        display_name="PAR Light",
        min_value=0.0,
        max_value=1200.0,
        noise_stddev=10.0,
        reading_interval_seconds=30,
    ),
    "substrate_temperature": SensorConfig(
        sensor_type="substrate_temperature",
        unit="°C",
        display_name="Substrate Temperature",
        min_value=10.0,
        max_value=30.0,
        noise_stddev=0.2,
        reading_interval_seconds=30,
    ),
    "substrate_moisture": SensorConfig(
        sensor_type="substrate_moisture",
        unit="%VWC",
        display_name="Substrate Moisture",
        min_value=20.0,
        max_value=95.0,
        noise_stddev=1.0,
        reading_interval_seconds=30,
    ),
    "substrate_ec": SensorConfig(
        sensor_type="substrate_ec",
        unit="mS/cm",
        display_name="Substrate EC",
        min_value=0.5,
        max_value=5.0,
        noise_stddev=0.05,
        reading_interval_seconds=30,
    ),
    "substrate_ph": SensorConfig(
        sensor_type="substrate_ph",
        unit="pH",
        display_name="Substrate pH",
        min_value=4.0,
        max_value=8.5,
        noise_stddev=0.03,
        reading_interval_seconds=30,
    ),
    "water_flow_rate": SensorConfig(
        sensor_type="water_flow_rate",
        unit="L/min",
        display_name="Water Flow Rate",
        min_value=0.0,
        max_value=60.0,
        noise_stddev=0.5,
        reading_interval_seconds=30,
    ),
    "vpd": SensorConfig(
        sensor_type="vpd",
        unit="kPa",
        display_name="VPD",
        min_value=0.0,
        max_value=3.0,
        noise_stddev=0.02,
        reading_interval_seconds=30,
    ),
}

# ---------------------------------------------------------------------------
# 5. Equipment
# ---------------------------------------------------------------------------

EQUIPMENT_TYPES: dict[str, EquipmentConfig] = {
    "heater": EquipmentConfig(
        equipment_type="heater",
        display_name="Heating System",
        unit="%output",
        has_backup=True,
    ),
    "cooler": EquipmentConfig(
        equipment_type="cooler",
        display_name="Cooling System",
        unit="%output",
        has_backup=False,
    ),
    "vent": EquipmentConfig(
        equipment_type="vent",
        display_name="Roof Vent",
        unit="%open",
        has_backup=False,
    ),
    "supplemental_light": EquipmentConfig(
        equipment_type="supplemental_light",
        display_name="Grow Lights",
        unit="watts",
        has_backup=False,
    ),
    "co2_injector": EquipmentConfig(
        equipment_type="co2_injector",
        display_name="CO\u2082 Injection",
        unit="kg/hr",
        has_backup=False,
    ),
    "thermal_curtain": EquipmentConfig(
        equipment_type="thermal_curtain",
        display_name="Thermal Curtain",
        unit="%deployed",
        has_backup=False,
    ),
    "irrigation_pump": EquipmentConfig(
        equipment_type="irrigation_pump",
        display_name="Irrigation Pump",
        unit="L/min",
        has_backup=True,
    ),
    "recirc_pump": EquipmentConfig(
        equipment_type="recirc_pump",
        display_name="Recirculation Pump",
        unit="on/off",
        has_backup=False,
    ),
}

# ---------------------------------------------------------------------------
# 6. Customers
# ---------------------------------------------------------------------------

CUSTOMERS: dict[str, CustomerConfig] = {
    "freshmart": CustomerConfig(
        customer_id="freshmart",
        name="FreshMart Groceries",
        region="Midwest US",
        country="US",
        delivery_frequency="daily",
    ),
    "greenleaf": CustomerConfig(
        customer_id="greenleaf",
        name="GreenLeaf Markets",
        region="Northeast US",
        country="US",
        delivery_frequency="3x_weekly",
    ),
    "harvest-co": CustomerConfig(
        customer_id="harvest-co",
        name="Harvest Co. Foods",
        region="Southeast US",
        country="US",
        delivery_frequency="3x_weekly",
    ),
    "maple-fresh": CustomerConfig(
        customer_id="maple-fresh",
        name="Maple Fresh",
        region="Ontario",
        country="Canada",
        delivery_frequency="daily",
    ),
    "northern-harvest": CustomerConfig(
        customer_id="northern-harvest",
        name="Northern Harvest",
        region="Quebec",
        country="Canada",
        delivery_frequency="3x_weekly",
    ),
    "pacific-organics": CustomerConfig(
        customer_id="pacific-organics",
        name="Pacific Organics",
        region="West Coast US",
        country="US",
        delivery_frequency="weekly",
    ),
}

# ---------------------------------------------------------------------------
# 7. Weather Parameters
# ---------------------------------------------------------------------------

WEATHER_PARAMS: dict[str, WeatherParams] = {
    "brightharvest": WeatherParams(
        greenhouse_id="brightharvest",
        temp_annual_mean=10.0,
        temp_annual_amplitude=22.0,
        temp_daily_amplitude=8.0,
        humidity_summer_mean=72.0,
        humidity_winter_mean=65.0,
        peak_month=6.5,
    ),
    "mucci-valley": WeatherParams(
        greenhouse_id="mucci-valley",
        temp_annual_mean=10.0,
        temp_annual_amplitude=18.0,
        temp_daily_amplitude=6.0,
        humidity_summer_mean=78.0,
        humidity_winter_mean=70.0,
        peak_month=6.5,
    ),
}

# ---------------------------------------------------------------------------
# 8. Timing Constants
# ---------------------------------------------------------------------------

HISTORICAL_START: datetime = datetime(2024, 4, 1, tzinfo=timezone.utc)
HISTORICAL_END: datetime = datetime(2026, 4, 15, tzinfo=timezone.utc)

SENSOR_INTERVAL_SECONDS: int = 30
EQUIPMENT_INTERVAL_SECONDS: int = 60
WEATHER_INTERVAL_SECONDS: int = 300  # 5 minutes
ENERGY_INTERVAL_SECONDS: int = 3600  # hourly
CROP_DAILY_HOUR: int = 6  # daily harvest reported at 6 AM local
CROP_WEEKLY_DOW: int = 2  # weekly measurements on Wednesday

ANOMALIES_PER_YEAR: int = 10


# ---------------------------------------------------------------------------
# 9. Helper Functions
# ---------------------------------------------------------------------------


def get_greenhouse(greenhouse_id: str) -> GreenhouseConfig:
    """Return a greenhouse config by its id, or raise ``KeyError``."""
    return GREENHOUSES[greenhouse_id]


def get_zone(zone_id: str) -> ZoneConfig:
    """Return a zone config by its id, or raise ``KeyError``."""
    return ZONES[zone_id]


def get_crop_for_zone(zone_id: str) -> CropConfig:
    """Return the primary crop config for a given zone."""
    return CROPS[ZONES[zone_id].primary_crop_id]


def get_zones_for_greenhouse(greenhouse_id: str) -> list[ZoneConfig]:
    """Return all zones belonging to *greenhouse_id*, ordered by zone_id."""
    return sorted(
        (z for z in ZONES.values() if z.greenhouse_id == greenhouse_id),
        key=lambda z: z.zone_id,
    )


def get_adjacent_zones(zone_id: str) -> list[ZoneConfig]:
    """Return resolved ``ZoneConfig`` objects for zones adjacent to *zone_id*."""
    return [ZONES[adj] for adj in ZONES[zone_id].adjacent_zones]
