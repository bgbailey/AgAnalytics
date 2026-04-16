"""Write data to Parquet files for OneLake upload."""
from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetWriter:
    """Writes simulation data to partitioned Parquet files.
    
    Directory structure mirrors OneLake Bronze layer:
        output_dir/
        ├── iot_telemetry/
        │   └── greenhouse_id=brightharvest/
        │       └── date=2025-01-15/
        │           └── data.parquet
        ├── weather/
        │   └── greenhouse_id=brightharvest/
        │       └── date=2025-01-15/
        │           └── data.parquet
        ├── equipment/
        │   └── ...
        ├── daily_harvest/
        │   └── ...
        ├── energy/
        │   └── ...
        └── shipments/
            └── ...
    """
    
    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self._buffers: dict[str, list[dict]] = {
            "iot_telemetry": [],
            "weather": [],
            "equipment": [],
            "daily_harvest": [],
            "energy": [],
            "shipments": [],
        }
        self._flush_threshold = 5000  # rows before auto-flush
    
    def write_sensor_readings(self, readings: list) -> None:
        """Buffer sensor readings. Auto-flushes when threshold reached."""
        for r in readings:
            self._buffers["iot_telemetry"].append(r.to_dict())
        if len(self._buffers["iot_telemetry"]) >= self._flush_threshold:
            self.flush("iot_telemetry")
    
    def write_weather(self, readings: list) -> None:
        for r in readings:
            self._buffers["weather"].append(r.to_dict())
        if len(self._buffers["weather"]) >= self._flush_threshold:
            self.flush("weather")
    
    def write_equipment(self, states: list) -> None:
        for s in states:
            self._buffers["equipment"].append(s.to_dict())
        if len(self._buffers["equipment"]) >= self._flush_threshold:
            self.flush("equipment")
    
    def write_harvests(self, harvests: list) -> None:
        for h in harvests:
            self._buffers["daily_harvest"].append(h.to_dict())
        # Harvests are daily — no auto-flush needed, but support it
        if len(self._buffers["daily_harvest"]) >= self._flush_threshold:
            self.flush("daily_harvest")
    
    def write_energy(self, readings: list) -> None:
        for r in readings:
            self._buffers["energy"].append(r.to_dict())
        if len(self._buffers["energy"]) >= self._flush_threshold:
            self.flush("energy")
    
    def write_shipments(self, events: list) -> None:
        for e in events:
            self._buffers["shipments"].append(e.to_dict())
        if len(self._buffers["shipments"]) >= self._flush_threshold:
            self.flush("shipments")
    
    def flush(self, data_type: str | None = None) -> None:
        """Flush buffered data to Parquet files.
        
        If data_type is None, flush all buffers.
        Partitions by greenhouse_id and date (extracted from timestamp or date field).
        """
        types_to_flush = [data_type] if data_type else list(self._buffers.keys())
        for dt in types_to_flush:
            if not self._buffers[dt]:
                continue
            df = pd.DataFrame(self._buffers[dt])
            # Extract partition columns
            if "timestamp" in df.columns:
                df["date"] = pd.to_datetime(df["timestamp"]).dt.date.astype(str)
            elif "date" in df.columns:
                df["date"] = df["date"].astype(str)
            elif "ship_date" in df.columns:
                df["date"] = pd.to_datetime(df["ship_date"]).dt.date.astype(str)
            
            if "greenhouse_id" in df.columns:
                # Partition by greenhouse and date
                for (gh, d), group in df.groupby(["greenhouse_id", "date"]):
                    out_path = self.output_dir / dt / f"greenhouse_id={gh}" / f"date={d}"
                    out_path.mkdir(parents=True, exist_ok=True)
                    file_path = out_path / "data.parquet"
                    # Append if file exists
                    write_df = group.drop(columns=["date"], errors="ignore")
                    if file_path.exists():
                        existing = pd.read_parquet(file_path)
                        write_df = pd.concat([existing, write_df], ignore_index=True)
                    write_df.to_parquet(file_path, index=False, engine="pyarrow")
            else:
                # No greenhouse partition (e.g., shipments span greenhouses)
                for d, group in df.groupby("date"):
                    out_path = self.output_dir / dt / f"date={d}"
                    out_path.mkdir(parents=True, exist_ok=True)
                    file_path = out_path / "data.parquet"
                    write_df = group.drop(columns=["date"], errors="ignore")
                    if file_path.exists():
                        existing = pd.read_parquet(file_path)
                        write_df = pd.concat([existing, write_df], ignore_index=True)
                    write_df.to_parquet(file_path, index=False, engine="pyarrow")
            
            self._buffers[dt] = []
    
    def flush_all(self) -> None:
        """Flush all buffers."""
        self.flush()
    
    def get_stats(self) -> dict[str, int]:
        """Return count of buffered rows per data type."""
        return {k: len(v) for k, v in self._buffers.items()}
