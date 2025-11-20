from __future__ import annotations

from pathlib import Path
from typing import List

from src.edu_cti.core.models import BaseIncident

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
RAW_CURATED_DIR = RAW_DIR / "curated"
RAW_NEWS_DIR = RAW_DIR / "news"
RAW_RSS_DIR = RAW_DIR / "rss"
PROC_DIR = DATA_DIR / "processed"


def ensure_dirs() -> None:
    for path in (
        DATA_DIR,
        RAW_DIR,
        RAW_CURATED_DIR,
        RAW_NEWS_DIR,
        RAW_RSS_DIR,
        PROC_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def write_base_csv(path: Path, incidents: List[BaseIncident]) -> int:
    """
    Write the supplied incidents to CSV, overwriting any previous snapshot.
    Returns number of rows written.
    """
    if not incidents:
        print(f"[warn] No incidents to write for {path.name}")
        return 0

    rows = [i.to_dict() for i in incidents]
    fieldnames = list(rows[0].keys())

    path.parent.mkdir(parents=True, exist_ok=True)
    import csv

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[ok] Wrote {len(rows)} rows to {path}")
    return len(rows)

