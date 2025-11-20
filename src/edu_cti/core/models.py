from dataclasses import dataclass, asdict
from typing import List, Optional
import hashlib


@dataclass
class BaseIncident:
    # Core identity
    incident_id: str
    source: str                      # e.g. "konbriefing", "ransomwarelive"
    source_event_id: Optional[str]   # source-native ID (e.g. ransomware.live slug)

    # Victim naming
    university_name: str             # best normalized label (for now = raw victim name)
    victim_raw_name: Optional[str]   # untouched original label from source

    # Location / type
    institution_type: Optional[str]  # "University" | "School" | "Research Institute" | "Unknown"
    country: Optional[str]           # ISO-2 (e.g. "DE", "US")
    region: Optional[str]
    city: Optional[str]

    # Dates
    incident_date: Optional[str]     # YYYY-MM-DD or None
    date_precision: str              # "day" | "month" | "year" | "unknown"
    source_published_date: Optional[str]   # when source published/discovered
    ingested_at: Optional[str]       # UTC timestamp when you ingested

    # Text
    title: Optional[str]
    subtitle: Optional[str]

    # URLs used in ENRICHMENT (news / statements ONLY)
    primary_url: Optional[str]       # main article to enrich from (selected by LLM in Phase 2 from all_urls)
    all_urls: List[str]              # all enrichment URLs (news articles, official statements) for Phase 2 LLM enrichment

    # CTI / infra URLs (NOT used by enrich pipeline - for reference/tracking only)
    leak_site_url: Optional[str] = None       # e.g. .onion claim_url (ransomware leak site)
    source_detail_url: Optional[str] = None   # e.g. ransomware.live detail page (CTI platform detail page, NOT news article)
    screenshot_url: Optional[str] = None      # image of leak page

    # Basic classification
    attack_type_hint: Optional[str] = None    # e.g. "ransomware"
    status: str = "suspected"                 # "suspected" | "confirmed"
    source_confidence: str = "medium"         # "low" | "medium" | "high"

    # Free-form notes (short)
    notes: Optional[str] = None

    def to_dict(self) -> dict:
        """
        Flatten to dict for CSV writing.
        We keep all fields, but serialize all_urls as a semicolon-separated string.
        """
        d = asdict(self)
        d["all_urls"] = ";".join(self.all_urls) if self.all_urls else ""
        return d


def make_incident_id(source: str, unique_string: str) -> str:
    """
    Stable, cross-source incident id based on source + some uniqueness context.
    """
    h = hashlib.sha256(unique_string.encode("utf-8")).hexdigest()[:16]
    return f"{source}_{h}"
