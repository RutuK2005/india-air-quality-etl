"""Re-record the small API responses used by the tests (needs network).

Usage:  python scripts/record_fixtures.py
"""

import json
from datetime import date
from pathlib import Path

from aq_etl.config import load_settings
from aq_etl.extract import SOURCES, OpenMeteoClient

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
CITIES = ("Delhi", "Mumbai")
START, END = date(2026, 11, 1), date(2026, 11, 2)


def main() -> None:
    settings = load_settings()
    client = OpenMeteoClient(settings)
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    for city in (c for c in settings.cities if c.name in CITIES):
        for source in SOURCES:
            payload = client.fetch(source, city, START, END)
            payload.pop("generationtime_ms", None)  # varies per call, not useful
            path = FIXTURE_DIR / f"{source}_{city.slug}.json"
            path.write_text(json.dumps(payload, indent=1), encoding="utf-8")
            print(f"wrote {path.name}: {len(payload['hourly']['time'])} hours")


if __name__ == "__main__":
    main()
