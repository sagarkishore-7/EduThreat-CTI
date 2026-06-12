"""Export the EduThreat-CTI dataset as a threat-intel feed for Wazuh (and MISP/STIX).

The pipeline *produces* CTI — threat actors, ransomware families, CVEs, victim
institutions, countries. A SOC consumes a threat feed by loading its IOCs into a
lookup and writing detection rules that fire when monitored logs match. This script
turns the published EduThreat dataset into:

  * **Wazuh CDB lists** (``key:value`` text, one per IOC type) — drop into
    ``/var/ossec/etc/lists/`` and reference from a rule's ``<list>`` field.
  * **STIX 2.1 bundle** (``--stix``) — indicators + threat-actor SDOs, importable
    into MISP / OpenCTI / any TAXII consumer.

Source of truth is the **public export API** (no DB credentials needed), so Wazuh
stays fully decoupled from the pipeline:

    GET {BASE}/api/v2/export/{dataset}.json   for dataset in (incidents, cves, iocs)

Usage:
    python wazuh/export_threat_feed.py --out ./feeds
    python wazuh/export_threat_feed.py --out ./feeds --stix
    python wazuh/export_threat_feed.py --base https://v2-api-production-e3d1.up.railway.app
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import os
import re
import urllib.request
from pathlib import Path

DEFAULT_BASE = os.environ.get(
    "EDUTHREAT_API_BASE", "https://v2-api-production-e3d1.up.railway.app"
)
_CVE_RE = re.compile(r"CVE-\d{4}-\d{4,7}", re.IGNORECASE)


def fetch_dataset(base: str, dataset: str) -> list[dict]:
    """Fetch an export dataset as a list of dict rows.

    Prefers CSV (the most reliable export format); falls back to JSON.
    """
    root = base.rstrip("/")
    # CSV first — robust, streamable, and the format the export endpoint serves best.
    try:
        with urllib.request.urlopen(f"{root}/api/v2/export/{dataset}.csv", timeout=120) as resp:
            text = resp.read().decode("utf-8")
        return list(csv.DictReader(io.StringIO(text)))
    except Exception:
        pass
    with urllib.request.urlopen(f"{root}/api/v2/export/{dataset}.json", timeout=120) as resp:
        payload = json.load(resp)
    if isinstance(payload, dict):
        for key in ("rows", "data", "items", dataset):
            if isinstance(payload.get(key), list):
                return payload[key]
        return []
    return payload if isinstance(payload, list) else []


def _split(value, sep="|") -> list[str]:
    if not value:
        return []
    return [v.strip() for v in str(value).split(sep) if v and v.strip()]


def build_cdb_lists(incidents: list[dict], cves: list[dict]) -> dict[str, dict[str, str]]:
    """Build Wazuh CDB lists (key -> value). Value carries a short context string."""
    actors: dict[str, str] = {}
    families: dict[str, str] = {}
    cve_list: dict[str, str] = {}
    countries: dict[str, str] = {}

    for row in incidents:
        actor = (row.get("threat_actor") or "").strip()
        if actor:
            actors[actor.lower()] = "edu_threat_actor"
        fam = (row.get("ransomware_family") or "").strip()
        if fam:
            families[fam.lower()] = "edu_ransomware_family"
        country = (row.get("country") or "").strip()
        if country:
            countries[country.lower()] = "edu_victim_country"
        for cve in _split(row.get("cves")):
            if _CVE_RE.fullmatch(cve):
                cve_list[cve.upper()] = "edu_exploited_cve"

    for row in cves:
        cve = (row.get("cve_id") or "").strip()
        if _CVE_RE.fullmatch(cve or ""):
            cve_list[cve.upper()] = "edu_exploited_cve"

    return {
        "eduthreat_actors": actors,
        "eduthreat_ransomware_families": families,
        "eduthreat_cves": cve_list,
        "eduthreat_victim_countries": countries,
    }


def write_cdb_lists(lists: dict[str, dict[str, str]], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    for name, entries in lists.items():
        path = out / name
        lines = [f"{key}:{val}" for key, val in sorted(entries.items())]
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        print(f"  wrote {path}  ({len(entries)} entries)")


def build_stix_bundle(lists: dict[str, dict[str, str]]) -> dict:
    """Minimal STIX 2.1 bundle: threat-actor SDOs + CVE/vulnerability indicators."""
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    objects: list[dict] = []

    def _id(prefix: str, seed: str) -> str:
        import hashlib
        import uuid

        h = hashlib.sha1(seed.encode()).digest()[:16]
        return f"{prefix}--{uuid.UUID(bytes=h)}"

    for actor in lists.get("eduthreat_actors", {}):
        objects.append({
            "type": "threat-actor", "spec_version": "2.1",
            "id": _id("threat-actor", f"actor:{actor}"),
            "created": now, "modified": now, "name": actor,
            "labels": ["education-sector-threat"],
        })
    for cve in lists.get("eduthreat_cves", {}):
        objects.append({
            "type": "indicator", "spec_version": "2.1",
            "id": _id("indicator", f"cve:{cve}"),
            "created": now, "modified": now,
            "name": cve, "pattern_type": "stix",
            "pattern": f"[vulnerability:name = '{cve}']",
            "valid_from": now, "labels": ["malicious-activity"],
        })
    return {"type": "bundle", "id": _id("bundle", f"eduthreat:{now}"), "objects": objects}


def main() -> None:
    ap = argparse.ArgumentParser(description="Export EduThreat-CTI as a Wazuh/STIX threat feed")
    ap.add_argument("--base", default=DEFAULT_BASE, help="EduThreat API base URL")
    ap.add_argument("--out", default="./feeds", help="output directory")
    ap.add_argument("--stix", action="store_true", help="also emit a STIX 2.1 bundle")
    args = ap.parse_args()

    out = Path(args.out)
    print(f"[feed] pulling datasets from {args.base} ...")
    incidents = fetch_dataset(args.base, "incidents")
    try:
        cves = fetch_dataset(args.base, "cves")
    except Exception:
        cves = []
    print(f"[feed] {len(incidents)} incidents, {len(cves)} cve rows")

    lists = build_cdb_lists(incidents, cves)
    write_cdb_lists(lists, out)

    if args.stix:
        bundle = build_stix_bundle(lists)
        stix_path = out / "eduthreat_stix_bundle.json"
        stix_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
        print(f"  wrote {stix_path}  ({len(bundle['objects'])} STIX objects)")

    total = sum(len(v) for v in lists.values())
    print(f"[feed] done — {total} IOC entries across {len(lists)} CDB lists in {out}/")


if __name__ == "__main__":
    main()
