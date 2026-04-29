"""
Live integration test for the Instructor correction layer.

Fetches real incidents from the Railway production API that have null/unknown
critical fields, then runs apply_instructor_corrections() against Ollama Cloud
(same credentials the pipeline uses) and prints a before/after comparison.

Usage:
    .venv/bin/python3 scripts/test_instructor_live.py [--limit N]

Requirements:
    - OLLAMA_API_KEY / OLLAMA_HOST / OLLAMA_MODEL in .env
    - instructor installed in .venv (pip install instructor)
    - Production API reachable
"""

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

# ── Path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests

PROD_API = "https://eduthreat-cti-production.up.railway.app"
ADMIN_CREDS = {"username": "admin", "password": "Kassat0071998<3?"}

# Critical fields the corrector targets
_NULL_SENTINELS = {"unknown", "other", "", "n/a", "none", "null"}

def _is_null(v: Any) -> bool:
    return not v or str(v).lower() in _NULL_SENTINELS


def _extract_attack_vector(d: Dict[str, Any]) -> Optional[str]:
    flat = d.get("attack_vector")
    if flat and not _is_null(flat):
        return str(flat)
    nested = d.get("attack_dynamics") or {}
    if isinstance(nested, dict):
        val = nested.get("attack_vector")
        if val and not _is_null(val):
            return str(val)
    return None


def count_null_critical(d: Dict[str, Any]):
    fields = []
    for f in ("attack_category", "institution_type", "country"):
        if _is_null(d.get(f)):
            fields.append(f)
    if not _extract_attack_vector(d):
        fields.append("attack_vector")
    return len(fields), fields


# ── API helpers ───────────────────────────────────────────────────────────────

def get_session_token() -> str:
    r = requests.post(f"{PROD_API}/api/admin/login", json=ADMIN_CREDS, timeout=15)
    r.raise_for_status()
    return r.json()["session_token"]


def fetch_enriched_incidents(token: str, limit: int = 200) -> List[Dict]:
    """Pull enriched incidents from the list endpoint."""
    r = requests.get(
        f"{PROD_API}/api/incidents",
        headers={"X-Session-Token": token},
        params={"limit": limit},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    incidents = data.get("incidents", data) if isinstance(data, dict) else data
    return [i for i in incidents if i.get("llm_enriched")]


def fetch_incident_detail(token: str, incident_id: str) -> Dict:
    r = requests.get(
        f"{PROD_API}/api/incidents/{incident_id}",
        headers={"X-Session-Token": token},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


# ── Reconstruction helper ─────────────────────────────────────────────────────

def incident_to_json_data(inc: Dict) -> Dict[str, Any]:
    """Reconstruct the json_data dict that apply_instructor_corrections() expects."""
    ad = inc.get("attack_dynamics") or {}
    di = inc.get("data_impact") or {}
    d = {
        "institution_name": inc.get("institution_name"),
        "institution_type": inc.get("institution_type"),
        "country": inc.get("country"),
        "attack_category": inc.get("attack_category"),
        "attack_vector": inc.get("attack_vector"),
        "attack_dynamics": {
            "attack_vector": ad.get("attack_vector") if isinstance(ad, dict) else None,
            "ransomware_family": ad.get("ransomware_family") if isinstance(ad, dict) else None,
        },
        "records_affected_exact": di.get("records_affected_exact") if isinstance(di, dict) else None,
    }
    return d


# ── Pretty print helpers ──────────────────────────────────────────────────────

BOLD  = "\033[1m"
GREEN = "\033[92m"
RED   = "\033[91m"
CYAN  = "\033[96m"
DIM   = "\033[2m"
RST   = "\033[0m"

CRITICAL_FIELDS = [
    "institution_type", "attack_category", "country",
    "attack_vector",    "ransomware_family",
]

def field_row(name: str, before: Any, after: Any) -> str:
    changed = str(before) != str(after) and not (_is_null(before) and _is_null(after))
    if changed and not _is_null(after):
        marker = f"{GREEN}FILLED{RST}"
    elif changed:
        marker = f"{RED}CHANGED{RST}"
    else:
        marker = f"{DIM}same{RST}"
    bv = f"{RED}NULL{RST}" if _is_null(before) else str(before)
    av = f"{GREEN}{after}{RST}" if (changed and not _is_null(after)) else (f"{RED}NULL{RST}" if _is_null(after) else str(after))
    return f"  {name:<28} {bv:<40} → {av}  [{marker}]"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5,
                        help="Number of incidents to test (default 5)")
    parser.add_argument("--fetch-limit", type=int, default=300,
                        help="How many enriched incidents to scan to find candidates")
    parser.add_argument("--min-nulls", type=int, default=1,
                        help="Min null critical fields to be a candidate (default 1; pipeline uses 2)")
    args = parser.parse_args()

    # ── Import corrector (needs Python 3.10+ for instructor) ──────────────────
    try:
        from src.edu_cti.pipeline.phase2.extraction.instructor_corrector import (
            apply_instructor_corrections,
            should_trigger_correction,
            count_null_critical_fields,
            INSTRUCTOR_AVAILABLE,
        )
    except Exception as e:
        print(f"{RED}Failed to import instructor_corrector: {e}{RST}")
        sys.exit(1)

    if not INSTRUCTOR_AVAILABLE:
        print(f"{RED}instructor package not available — run with Python 3.10+{RST}")
        print("Use: .venv/bin/python3 scripts/test_instructor_live.py")
        sys.exit(1)

    print(f"{BOLD}Instructor live integration test{RST}  (Python {sys.version.split()[0]})")
    print(f"instructor available: {GREEN}YES{RST}")

    # ── Build Ollama client ───────────────────────────────────────────────────
    from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
    ollama_client = OllamaLLMClient(
        api_key=os.environ["OLLAMA_API_KEY"],
        host=os.environ.get("OLLAMA_HOST", "https://ollama.com"),
        model=os.environ.get("OLLAMA_MODEL", "deepseek-v3.1:671b-cloud"),
    )
    print(f"Ollama model: {CYAN}{ollama_client.model}{RST}\n")

    # ── Fetch candidates from Railway ─────────────────────────────────────────
    print(f"Fetching up to {args.fetch_limit} enriched incidents from Railway...")
    token = get_session_token()
    incidents = fetch_enriched_incidents(token, limit=args.fetch_limit)
    print(f"Retrieved {len(incidents)} incidents\n")

    # Fetch full detail for all enriched incidents so we see attack_vector too
    print(f"Fetching full detail for {len(incidents)} enriched incidents...")
    detailed = []
    for inc in incidents:
        try:
            detail = fetch_incident_detail(token, inc["incident_id"])
            detailed.append(detail)
        except Exception as e:
            print(f"  Warning: could not fetch {inc['incident_id']}: {e}")

    # Filter to candidates with ≥min_nulls null critical fields
    candidates = []
    for inc in detailed:
        jd = incident_to_json_data(inc)
        count, null_fields = count_null_critical_fields(jd)
        if count >= args.min_nulls:
            candidates.append((inc, jd, null_fields))

    print(f"Candidates with ≥{args.min_nulls} null critical fields: {BOLD}{len(candidates)}{RST}")
    if not candidates:
        print(f"No candidates found. All {len(detailed)} enriched incidents have all critical fields populated.")
        print("The corrector would not fire on any of them — pipeline extraction quality is high.")
        sys.exit(0)

    selected = candidates[: args.limit]
    print(f"Testing correction on {len(selected)} incidents\n")
    print("=" * 80)

    results = {"corrected": 0, "no_change": 0, "failed": 0}

    for idx, (inc, json_data_before, null_fields) in enumerate(selected, 1):
        inc_id   = inc.get("id", "?")
        inst     = inc.get("institution_name", "Unknown")
        summary  = (inc.get("enriched_summary") or "")[:120]

        print(f"\n{BOLD}[{idx}/{len(selected)}] {inc_id}{RST}")
        print(f"  Institution : {inst}")
        print(f"  Summary     : {DIM}{summary}...{RST}")
        print(f"  Null fields : {RED}{', '.join(null_fields)}{RST}")

        # Article text — prefer full article_text, fall back to enriched_summary as context
        article_text = inc.get("article_text") or inc.get("enriched_summary") or ""

        import copy
        json_data = copy.deepcopy(json_data_before)

        t0 = time.time()
        try:
            json_data_after, was_corrected = apply_instructor_corrections(
                json_data=json_data,
                article_text=article_text,
                institution_name=inst,
                ollama_client=ollama_client,
                max_retries=3,
            )
            elapsed = time.time() - t0
        except Exception as exc:
            print(f"  {RED}FAILED: {exc}{RST}")
            results["failed"] += 1
            continue

        if was_corrected:
            results["corrected"] += 1
        else:
            results["no_change"] += 1

        status_label = f"{GREEN}CORRECTED{RST}" if was_corrected else f"{DIM}no change{RST}"
        print(f"  Result      : {status_label}  ({elapsed:.1f}s)")

        # Extract after values for comparison
        ad_after = json_data_after.get("attack_dynamics") or {}

        def _get_before(field):
            if field == "attack_vector":
                return _extract_attack_vector(json_data_before)
            if field == "ransomware_family":
                return (json_data_before.get("attack_dynamics") or {}).get("ransomware_family")
            return json_data_before.get(field)

        def _get_after(field):
            if field == "attack_vector":
                return _extract_attack_vector(json_data_after)
            if field == "ransomware_family":
                return ad_after.get("ransomware_family") or json_data_after.get("ransomware_family")
            return json_data_after.get(field)

        print(f"\n  {'Field':<28} {'Before':<40}   After")
        print(f"  {'-'*28} {'-'*40}   {'-'*30}")
        for field in CRITICAL_FIELDS:
            print(field_row(field, _get_before(field), _get_after(field)))

        print()

    # ── Summary ───────────────────────────────────────────────────────────────
    print("=" * 80)
    print(f"\n{BOLD}Summary{RST}")
    total = len(selected)
    print(f"  Tested    : {total}")
    print(f"  Corrected : {GREEN}{results['corrected']}{RST}  ({results['corrected']/total*100:.0f}%)")
    print(f"  No change : {results['no_change']}")
    print(f"  Failed    : {RED}{results['failed']}{RST}")


if __name__ == "__main__":
    main()
