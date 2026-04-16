"""CLI entry point for aganalytics commands.

Provides the ``aganalytics`` command group with sub-commands for historical
data generation, real-time streaming, and live anomaly triggering.

Usage examples::

    # Generate 2 years of historical Parquet data
    aganalytics generate --output ./sample-data

    # Stream real-time telemetry to Event Hub
    aganalytics stream --eventhub-conn "$EVENTHUB_CONNECTION_STRING"

    # Stream in dry-run mode (no Event Hub) at 60× speed
    aganalytics stream --speed 60

    # Trigger an anomaly (picked up by a running stream)
    aganalytics trigger hvac-failure --zone BH-Z05 --severity 0.8

    # Resolve active anomalies
    aganalytics resolve hvac-failure
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click

from src.anomalies.scenarios import ALL_SCENARIOS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

# File-based IPC paths (shared with RealtimeGenerator)
TRIGGER_FILE = Path(".aganalytics-trigger.json")
RESOLVE_FILE = Path(".aganalytics-resolve.json")

SCENARIO_NAMES = list(ALL_SCENARIOS.keys())


@click.group()
@click.version_option(version="0.1.0", prog_name="aganalytics")
def main():
    """AgriTech Analytics — Greenhouse data generator for Microsoft Fabric demos."""


# ------------------------------------------------------------------
# generate — historical backfill
# ------------------------------------------------------------------


@main.command()
@click.option("--output", "-o", default="./sample-data", help="Output directory for Parquet files")
@click.option("--start", default="2024-04-01", help="Start date (YYYY-MM-DD)")
@click.option("--end", default="2026-04-15", help="End date (YYYY-MM-DD)")
@click.option("--seed", default=42, type=int, help="Random seed for reproducibility")
def generate(output: str, start: str, end: str, seed: int) -> None:
    """Generate historical data (2 years of Parquet files)."""
    from src.generators.historical import HistoricalGenerator

    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)

    click.echo(f"Generating historical data: {start} → {end}")
    click.echo(f"Output: {output}")
    click.echo(f"Seed:   {seed}")

    generator = HistoricalGenerator(output_dir=output, seed=seed)
    stats = generator.generate(start=start_dt, end=end_dt)

    click.echo("\n✅ Generation complete!")
    click.echo(json.dumps(stats, indent=2))


# ------------------------------------------------------------------
# stream — real-time Event Hub publisher
# ------------------------------------------------------------------


@main.command()
@click.option(
    "--eventhub-conn",
    envvar="EVENTHUB_CONNECTION_STRING",
    default=None,
    help="Event Hub connection string (or set EVENTHUB_CONNECTION_STRING env var)",
)
@click.option("--eventhub-name", default="greenhouse-telemetry", help="Event Hub name")
@click.option(
    "--speed", default=1.0, type=float,
    help="Speed multiplier (1.0 = real-time, 60.0 = 1 hr per minute)",
)
@click.option("--seed", default=42, type=int, help="Random seed")
def stream(eventhub_conn: str | None, eventhub_name: str, speed: float, seed: int) -> None:
    """Start real-time streaming to Event Hub.

    Continuously generates sensor, equipment, and weather data for all 16
    grow zones and publishes to Event Hub.  Anomalies can be triggered by
    running ``aganalytics trigger`` in a separate terminal.

    Without an Event Hub connection string the generator runs in **dry-run
    mode** — data is generated but only logged, not published.
    """
    from src.generators.realtime import RealtimeGenerator

    click.echo(f"🚀 Starting real-time stream (speed: {speed}×)")
    click.echo(f"   Event Hub: {eventhub_name}")
    if not eventhub_conn:
        click.echo("   ⚠  No connection string — running in dry-run mode (data logged, not sent)")
    click.echo("   Press Ctrl+C to stop.\n")

    gen = RealtimeGenerator(
        connection_string=eventhub_conn,
        eventhub_name=eventhub_name,
        seed=seed,
        speed=speed,
    )

    try:
        gen.start(blocking=True)
    except KeyboardInterrupt:
        click.echo("\n⏹  Stopping stream…")
        gen.stop()
        status = gen.status()
        click.echo(f"   Total ticks:          {status['stats']['ticks']}")
        click.echo(f"   Sensor events sent:   {status['stats']['sensor_events']}")
        click.echo(f"   Equipment events:     {status['stats']['equipment_events']}")
        click.echo(f"   Weather events:       {status['stats']['weather_events']}")
        click.echo("   Stream stopped.")


# ------------------------------------------------------------------
# trigger — inject a live anomaly (file-based IPC)
# ------------------------------------------------------------------


@main.command()
@click.argument("scenario", type=click.Choice(SCENARIO_NAMES))
@click.option(
    "--greenhouse", "-g", default="brightharvest",
    type=click.Choice(["brightharvest", "mucci-valley"]),
    help="Target greenhouse",
)
@click.option(
    "--zone", "-z", default=None,
    help="Target zone (e.g. BH-Z05). Auto-selected if omitted.",
)
@click.option("--severity", "-s", default=0.8, type=float, help="Severity 0.0–1.0")
@click.option("--duration", "-d", default=10, type=int, help="Duration in minutes")
def trigger(
    scenario: str,
    greenhouse: str,
    zone: str | None,
    severity: float,
    duration: int,
) -> None:
    """Trigger a live anomaly scenario.

    Writes a trigger file that the running ``stream`` process picks up
    on its next tick (~30 s or less).

    \b
    Scenarios:
      hvac-failure        Boiler offline, temperature drops       (~90 s detection)
      nutrient-drift      pH slowly rises, EC drops               (~3-4 min)
      irrigation-failure  Pump fails, moisture drops               (~2 min)
      cold-chain-break    Truck temp rises above safe limits       (~5 min)
    """
    # Default zone per greenhouse
    if zone is None:
        zone = "BH-Z05" if greenhouse == "brightharvest" else "MV-Z03"

    trigger_data = {
        "scenario": scenario,
        "greenhouse_id": greenhouse,
        "zone_id": zone,
        "severity": severity,
        "duration_minutes": duration,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    TRIGGER_FILE.write_text(json.dumps(trigger_data, indent=2), encoding="utf-8")

    click.echo(f"🔴 Triggered: {scenario}")
    click.echo(f"   Greenhouse: {greenhouse}")
    click.echo(f"   Zone:       {zone}")
    click.echo(f"   Severity:   {severity}")
    click.echo(f"   Duration:   {duration} min")
    click.echo(f"\n   Trigger file written → {TRIGGER_FILE}")
    click.echo("   (The running stream will pick this up on the next tick)")


# ------------------------------------------------------------------
# resolve — manually end an active anomaly
# ------------------------------------------------------------------


@main.command()
@click.argument("scenario", required=False, default=None)
@click.option("--anomaly-id", "-a", default=None, help="Resolve a specific anomaly by ID")
@click.option("--zone", "-z", default=None, help="(informational) Zone to resolve")
def resolve(scenario: str | None, anomaly_id: str | None, zone: str | None) -> None:
    """Manually resolve an active anomaly.

    Provide a SCENARIO name to resolve all anomalies of that type, or use
    --anomaly-id to resolve a specific instance.  With no arguments, signals
    the stream to resolve all active anomalies.
    """
    resolve_data: dict[str, str | None] = {
        "scenario": scenario,
        "anomaly_id": anomaly_id,
        "zone_id": zone,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    RESOLVE_FILE.write_text(json.dumps(resolve_data, indent=2), encoding="utf-8")

    target = scenario or anomaly_id or "all active anomalies"
    click.echo(f"✅ Resolve signal sent for: {target}")
    click.echo(f"   Resolve file written → {RESOLVE_FILE}")


# ------------------------------------------------------------------
# status — show current state
# ------------------------------------------------------------------


@main.command("status")
def show_status() -> None:
    """Show current generator status and active anomalies."""
    click.echo("─── AgriTech Analytics — Generator Status ───\n")

    if TRIGGER_FILE.exists():
        try:
            data = json.loads(TRIGGER_FILE.read_text(encoding="utf-8"))
            click.echo(f"  Pending trigger: {data['scenario']} ({data.get('zone_id', '?')})")
            click.echo(f"  Submitted:       {data.get('timestamp', '?')}")
        except Exception:
            click.echo("  Pending trigger: (corrupt file)")
    else:
        click.echo("  No pending triggers.")

    if RESOLVE_FILE.exists():
        try:
            data = json.loads(RESOLVE_FILE.read_text(encoding="utf-8"))
            click.echo(f"  Pending resolve: {data.get('scenario') or data.get('anomaly_id') or 'all'}")
        except Exception:
            click.echo("  Pending resolve: (corrupt file)")
    else:
        click.echo("  No pending resolves.")

    click.echo(f"\n  Available scenarios: {', '.join(SCENARIO_NAMES)}")
    click.echo("\n  (For live stats, the stream command logs periodic updates)")


# ------------------------------------------------------------------
# scenarios — list available anomaly scenarios
# ------------------------------------------------------------------


@main.command("scenarios")
def list_scenarios() -> None:
    """List available anomaly scenarios with descriptions."""
    click.echo("─── Available Anomaly Scenarios ───\n")
    for name, profile in ALL_SCENARIOS.items():
        click.echo(f"  {name}")
        click.echo(f"    {profile.display_name}")
        click.echo(f"    {profile.description}")
        click.echo(f"    Sensors: {', '.join(profile.affected_sensors)}")
        click.echo()


if __name__ == "__main__":
    main()
