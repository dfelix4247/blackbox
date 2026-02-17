from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from .models import CSV_FIELDS, Lead


DATA_PATH = Path("data/leads.csv")


def ensure_csv(path: Path = DATA_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
    return path


def load_leads(path: Path = DATA_PATH) -> list[Lead]:
    ensure_csv(path)
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [Lead.model_validate(row) for row in reader]


def save_leads(leads: Iterable[Lead], path: Path = DATA_PATH) -> None:
    ensure_csv(path)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead.model_dump())
