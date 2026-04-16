"""Supply chain and logistics simulation.

Generates :class:`ShipmentEvent` records representing orders placed by
retail customers and their shipment through cold-chain logistics.

BrightHarvest Greens (US) serves 4 US customers; Mucci Valley Farms
(Canada) serves 2 Canadian customers.  Cross-border shipments happen
occasionally (BrightHarvest → Maple Fresh).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import numpy as np

from src.config import (
    CROPS,
    CUSTOMERS,
    CustomerConfig,
    get_crop_for_zone,
    get_zones_for_greenhouse,
)
from src.models.greenhouse import ShipmentEvent

# ---------------------------------------------------------------------------
# Customer ↔ greenhouse mapping
# ---------------------------------------------------------------------------

# Primary customer assignments
_GREENHOUSE_CUSTOMERS: dict[str, list[str]] = {
    "brightharvest": ["freshmart", "greenleaf", "harvest-co", "pacific-organics"],
    "mucci-valley": ["maple-fresh", "northern-harvest"],
}

# Cross-border: BrightHarvest occasionally ships to Maple Fresh
_CROSS_BORDER: dict[str, list[str]] = {
    "brightharvest": ["maple-fresh"],
}

# Transit hours: (greenhouse, customer) → (min_hours, max_hours)
_TRANSIT_HOURS: dict[tuple[str, str], tuple[float, float]] = {
    # BrightHarvest (Rochelle, IL) — local US customers
    ("brightharvest", "freshmart"): (6, 10),       # Midwest
    ("brightharvest", "greenleaf"): (12, 20),      # Northeast
    ("brightharvest", "harvest-co"): (14, 22),     # Southeast
    ("brightharvest", "pacific-organics"): (24, 30),  # West Coast
    ("brightharvest", "maple-fresh"): (8, 14),     # Cross-border Ontario
    # Mucci Valley (Kingsville, ON)
    ("mucci-valley", "maple-fresh"): (6, 10),      # Ontario
    ("mucci-valley", "northern-harvest"): (10, 18),  # Quebec
}

# Product packaging types by crop category
_PACKAGE_TYPES: dict[str, list[str]] = {
    "leafy_green": ["clamshell", "bag", "bulk"],
    "vine_crop": ["case", "clamshell"],
    "berry": ["clamshell", "pint"],
}

# Case weights (kg)
_CASE_KG: dict[str, float] = {
    "leafy_green": 5.0,
    "vine_crop": 10.0,
    "berry": 4.0,
}


# ---------------------------------------------------------------------------
# SupplyChainSimulator
# ---------------------------------------------------------------------------


class SupplyChainSimulator:
    """Simulates orders from retail customers and shipment logistics."""

    def __init__(self, seed: int | None = None) -> None:
        self._rng = np.random.default_rng(seed)
        self._order_counter = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_daily_shipments(
        self,
        current_date: date,
        greenhouse_id: str,
        available_harvest: dict[str, float],
    ) -> list[ShipmentEvent]:
        """Generate shipment events for a day based on available harvest.

        Args:
            current_date:      The shipping date.
            greenhouse_id:     Which greenhouse is shipping.
            available_harvest:  crop_id → kg available to ship today.
        """
        shipments: list[ShipmentEvent] = []

        # Determine which customers are ordering today
        ordering_customers = self._get_ordering_customers(
            current_date, greenhouse_id
        )

        if not ordering_customers or not available_harvest:
            return shipments

        # Available crops from this greenhouse
        crop_ids = [cid for cid, kg in available_harvest.items() if kg > 0]
        if not crop_ids:
            return shipments

        for customer in ordering_customers:
            # 1-3 product orders per customer per day
            n_products = int(self._rng.integers(1, 4))
            n_products = min(n_products, len(crop_ids))

            chosen_crops = self._rng.choice(
                crop_ids, size=n_products, replace=False
            ).tolist()

            order_id = self._generate_order_id(current_date)

            for crop_id in chosen_crops:
                crop = CROPS[crop_id]
                case_kg = _CASE_KG.get(crop.category, 5.0)

                # 5-30 cases per order
                quantity_cases = int(self._rng.integers(5, 31))

                order_kg = quantity_cases * case_kg
                # Don't ship more than available
                avail = available_harvest.get(crop_id, 0.0)
                if order_kg > avail:
                    quantity_cases = max(1, int(avail / case_kg))
                    order_kg = quantity_cases * case_kg
                available_harvest[crop_id] = max(
                    0.0, avail - order_kg
                )

                # Package type
                pkg_types = _PACKAGE_TYPES.get(crop.category, ["case"])
                pkg = self._rng.choice(pkg_types)
                product_sku = f"{crop_id}-{pkg}"

                # Transit time
                transit_key = (greenhouse_id, customer.customer_id)
                min_hrs, max_hrs = _TRANSIT_HOURS.get(
                    transit_key, (8, 18)
                )
                transit_hours = self._rng.uniform(min_hrs, max_hrs)

                ship_dt = datetime(
                    current_date.year,
                    current_date.month,
                    current_date.day,
                    6,  # ship at 6 AM
                    tzinfo=timezone.utc,
                )
                expected_delivery = ship_dt + timedelta(hours=transit_hours)

                # Cold chain temperature: normally 2-4 °C
                cold_chain_temp = self._rng.normal(3.0, 0.4)
                cold_chain_temp = float(np.clip(cold_chain_temp, 1.0, 6.0))
                cold_chain_compliant = cold_chain_temp <= 5.0

                # Shelf life remaining at delivery
                transit_days = transit_hours / 24.0
                shelf_remaining = max(
                    0, int(crop.shelf_life_days - transit_days)
                )

                shipments.append(ShipmentEvent(
                    order_id=order_id,
                    greenhouse_id=greenhouse_id,
                    customer_id=customer.customer_id,
                    product_sku=product_sku,
                    crop_id=crop_id,
                    quantity_cases=quantity_cases,
                    ship_date=ship_dt,
                    expected_delivery=expected_delivery,
                    actual_delivery=None,
                    cold_chain_temp=round(cold_chain_temp, 1),
                    cold_chain_compliant=cold_chain_compliant,
                    delivery_status="in_transit",
                    shelf_life_remaining_days=shelf_remaining,
                ))

        return shipments

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_ordering_customers(
        self,
        current_date: date,
        greenhouse_id: str,
    ) -> list[CustomerConfig]:
        """Determine which customers order from this greenhouse today."""
        dow = current_date.weekday()  # 0 = Monday

        primary_ids = _GREENHOUSE_CUSTOMERS.get(greenhouse_id, [])
        cross_ids = _CROSS_BORDER.get(greenhouse_id, [])

        result: list[CustomerConfig] = []

        for cid in primary_ids:
            cust = CUSTOMERS.get(cid)
            if cust is None:
                continue
            if self._should_order(cust, dow):
                result.append(cust)

        # Cross-border shipments: ~30 % probability on eligible days
        for cid in cross_ids:
            cust = CUSTOMERS.get(cid)
            if cust is None:
                continue
            if self._should_order(cust, dow) and self._rng.random() < 0.30:
                result.append(cust)

        return result

    @staticmethod
    def _should_order(customer: CustomerConfig, dow: int) -> bool:
        """Check if customer orders on this day-of-week."""
        freq = customer.delivery_frequency
        if freq == "daily":
            return True
        if freq == "3x_weekly":
            return dow in (0, 2, 4)  # Mon, Wed, Fri
        if freq == "weekly":
            return dow == 0  # Monday only
        return False

    def _generate_order_id(self, current_date: date) -> str:
        """Generate sequential order ID like 'ORD-20250115-001'."""
        self._order_counter += 1
        return f"ORD-{current_date.strftime('%Y%m%d')}-{self._order_counter:04d}"


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from datetime import date

    print("=" * 72)
    print("  SupplyChainSimulator — quick smoke test")
    print("=" * 72)

    sim = SupplyChainSimulator(seed=42)

    # Simulate a Monday (all frequencies active)
    test_date = date(2025, 7, 14)  # Monday
    print(f"\n  Date: {test_date} ({test_date.strftime('%A')})")

    for gh_id, label in [
        ("brightharvest", "BrightHarvest Greens"),
        ("mucci-valley", "Mucci Valley Farms"),
    ]:
        # Build mock harvest availability
        zones = get_zones_for_greenhouse(gh_id)
        avail: dict[str, float] = {}
        for z in zones:
            crop = get_crop_for_zone(z.zone_id)
            avail[crop.crop_id] = avail.get(crop.crop_id, 0.0) + 500.0

        shipments = sim.generate_daily_shipments(test_date, gh_id, avail)
        print(f"\n  {label}: {len(shipments)} shipment(s)")
        for s in shipments:
            transit_h = (s.expected_delivery - s.ship_date).total_seconds() / 3600
            print(
                f"    {s.order_id}  → {s.customer_id:<18s}"
                f"  {s.product_sku:<28s}"
                f"  {s.quantity_cases:>3d} cases"
                f"  transit={transit_h:.0f}h"
                f"  temp={s.cold_chain_temp:.1f}°C"
                f"  shelf={s.shelf_life_remaining_days}d"
            )

    # Also test a Wednesday and Saturday
    for d in [date(2025, 7, 16), date(2025, 7, 19)]:
        avail = {"baby_spinach": 300.0, "romaine": 300.0}
        shipments = sim.generate_daily_shipments(d, "brightharvest", avail)
        print(f"\n  {d} ({d.strftime('%A')}): {len(shipments)} BrightHarvest shipment(s)")
