"""CSV export for debugging and inspection."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


class CSVWriter:
    """Writes simulation data to CSV files for easy inspection."""
    
    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._files: dict[str, Any] = {}
        self._writers: dict[str, csv.DictWriter] = {}
    
    def _get_writer(self, data_type: str, fieldnames: list[str]) -> csv.DictWriter:
        if data_type not in self._writers:
            filepath = self.output_dir / f"{data_type}.csv"
            f = open(filepath, "w", newline="", encoding="utf-8")
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            self._files[data_type] = f
            self._writers[data_type] = writer
        return self._writers[data_type]
    
    def write_rows(self, data_type: str, rows: list[dict]) -> None:
        if not rows:
            return
        writer = self._get_writer(data_type, list(rows[0].keys()))
        for row in rows:
            writer.writerow(row)
    
    def write_sensor_readings(self, readings: list) -> None:
        self.write_rows("iot_telemetry", [r.to_dict() for r in readings])
    
    def write_weather(self, readings: list) -> None:
        self.write_rows("weather", [r.to_dict() for r in readings])
    
    def write_equipment(self, states: list) -> None:
        self.write_rows("equipment", [s.to_dict() for s in states])
    
    def write_harvests(self, harvests: list) -> None:
        self.write_rows("daily_harvest", [h.to_dict() for h in harvests])
    
    def write_energy(self, readings: list) -> None:
        self.write_rows("energy", [r.to_dict() for r in readings])
    
    def write_shipments(self, events: list) -> None:
        self.write_rows("shipments", [e.to_dict() for e in events])
    
    def close(self) -> None:
        for f in self._files.values():
            f.close()
        self._files.clear()
        self._writers.clear()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
