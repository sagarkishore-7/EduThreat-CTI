"""
CISA Known Exploited Vulnerabilities (KEV) source for EduThreat-CTI.

CISA maintains a catalog of vulnerabilities that are known to be actively
exploited in the wild. While not education-specific, these CVEs are critical
for understanding which vulnerabilities are being used against education
institutions when cross-referenced with our incident data.

API: https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json
Cost: FREE (US government open data)
Coverage: Actively exploited vulnerabilities since 2021
"""

import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional

import requests

from src.edu_cti.core.models import BaseIncident, make_incident_id

logger = logging.getLogger(__name__)

CISA_KEV_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
SOURCE_NAME = "cisa_kev"


def fetch_kev_catalog() -> List[Dict]:
    """
    Fetch the full CISA KEV catalog.

    Returns:
        List of vulnerability dicts from CISA
    """
    try:
        resp = requests.get(CISA_KEV_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("vulnerabilities", [])
    except Exception as e:
        logger.error(f"Failed to fetch CISA KEV catalog: {e}")
        return []


def build_cisa_kev_incidents(
    *,
    max_pages: Optional[int] = None,
    client=None,
    save_callback: Optional[Callable] = None,
    incremental: bool = True,
    start_year: int = 2019,
) -> List[BaseIncident]:
    """
    Fetch CISA KEV entries that are relevant to education sector.

    CISA KEV entries don't directly map to incidents — they map to CVEs
    that are actively exploited. We create pseudo-incidents for CVEs that
    specifically mention education or have been used against education targets.

    For the primary use case, this data is better used as IOC enrichment
    (cross-referencing CVE IDs found in incidents). But we also create
    incidents for KEV entries with education-specific notes.

    Returns:
        List of BaseIncident objects (typically empty — main value is CVE data)
    """
    logger.info("Fetching CISA KEV catalog...")

    vulns = fetch_kev_catalog()
    if not vulns:
        return []

    logger.info(f"CISA KEV: {len(vulns)} vulnerabilities in catalog")

    # Education-related keywords for filtering
    edu_keywords = [
        "education", "university", "school", "college", "academic",
        "student", "campus", "k-12",
    ]

    incidents: List[BaseIncident] = []

    for vuln in vulns:
        cve_id = vuln.get("cveID", "")
        vendor = vuln.get("vendorProject", "")
        product = vuln.get("product", "")
        vuln_name = vuln.get("vulnerabilityName", "")
        description = vuln.get("shortDescription", "")
        date_added = vuln.get("dateAdded", "")
        known_ransomware = vuln.get("knownRansomwareCampaignUse", "Unknown")
        notes = vuln.get("notes", "")

        if not cve_id:
            continue

        # Date filter
        if date_added:
            try:
                year = int(date_added[:4])
                if year < start_year:
                    continue
            except (ValueError, IndexError):
                pass

        # Check if education-related or commonly used against education
        combined_text = f"{description} {notes} {vuln_name}".lower()
        is_edu_related = any(kw in combined_text for kw in edu_keywords)

        # Also include all ransomware-associated CVEs (common in education attacks)
        is_ransomware = known_ransomware.lower() == "known"

        # Common education-targeted products
        edu_products = [
            "accellion", "moveit", "solarwinds", "citrix", "vpn",
            "exchange", "fortinet", "pulse", "zoom", "blackboard",
            "moodle", "canvas", "powerschool", "ellucian", "banner",
        ]
        is_edu_product = any(p in f"{vendor} {product}".lower() for p in edu_products)

        if not (is_edu_related or is_ransomware or is_edu_product):
            continue

        source_event_id = cve_id
        incident_id = make_incident_id(SOURCE_NAME, source_event_id)

        # Build a CISA reference URL
        cisa_url = f"https://www.cisa.gov/known-exploited-vulnerabilities-catalog"
        nvd_url = f"https://nvd.nist.gov/vuln/detail/{cve_id}"

        incident = BaseIncident(
            incident_id=incident_id,
            source=SOURCE_NAME,
            source_event_id=source_event_id,
            institution_name=f"{vendor} {product} - {cve_id}",
            victim_raw_name=None,
            institution_type=None,
            country="United States",  # CISA is US-focused
            region=None,
            city=None,
            incident_date=date_added,
            date_precision="day" if date_added else "unknown",
            source_published_date=date_added,
            ingested_at=datetime.utcnow().isoformat(),
            title=f"CISA KEV: {cve_id} - {vuln_name}",
            subtitle=description[:200] if description else None,
            primary_url=None,
            all_urls=[nvd_url, cisa_url],
            attack_type_hint="vulnerability_exploit",
            status="confirmed",
            source_confidence="high",
            notes=f"ransomware_use={known_ransomware};vendor={vendor};product={product}",
        )

        incidents.append(incident)

        if save_callback:
            save_callback([incident])

    logger.info(f"CISA KEV: Found {len(incidents)} education/ransomware-relevant CVEs")
    return incidents


def get_kev_cve_set() -> set:
    """
    Get the full set of CVE IDs in the KEV catalog.

    Useful for cross-referencing: if an incident mentions a CVE that's
    in the KEV, it's actively exploited and higher priority.

    Returns:
        Set of CVE ID strings
    """
    vulns = fetch_kev_catalog()
    return {v.get("cveID", "") for v in vulns if v.get("cveID")}
