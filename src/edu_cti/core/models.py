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
    institution_name: str             # best normalized label (for now = raw victim name)
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

    # Discovery / disclosure date — when the incident/claim was publicly disclosed
    # For ransomware.live: the date the group posted on their leak site ("discovered" field)
    # Distinct from incident_date (when attack happened) and source_published_date (article date)
    discovery_date: Optional[str] = None

    # Threat actor name (ransomware group, APT, etc.) — populated by API sources directly
    # from the group's own infrastructure; more reliable than LLM-extracted names
    threat_actor: Optional[str] = None

    # Data-quality re-enrichment metadata. When present, the enricher can build
    # its retry hint directly from the incident row without opening a fresh DB
    # connection inside the hot path.
    re_enrich_attempts: Optional[int] = None
    re_enrich_reason: Optional[str] = None

    # Full source-native payload when the collector receives structured API data.
    # This keeps future fields available in v2 raw_payload without requiring a
    # schema change every time an upstream source adds a key.
    raw_source_payload: Optional[dict] = None

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
