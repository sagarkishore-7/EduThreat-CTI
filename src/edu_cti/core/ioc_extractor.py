"""
Regex-based IOC (Indicator of Compromise) extraction for EduThreat-CTI.

Extracts IOCs from article text before LLM enrichment:
- IPv4/IPv6 addresses
- Domain names
- URLs
- File hashes (MD5, SHA1, SHA256)
- CVE IDs
- Email addresses
- Bitcoin/cryptocurrency addresses
- MITRE ATT&CK technique IDs

This runs as a pre-enrichment step and is completely free (no API calls).
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional

logger = logging.getLogger(__name__)

# --- Regex patterns for IOC extraction ---

# IPv4 address (with word boundaries to avoid partial matches)
IPV4_PATTERN = re.compile(
    r'\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b'
)

# IPv6 address (simplified - catches common formats)
IPV6_PATTERN = re.compile(
    r'\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b'
    r'|\b(?:[0-9a-fA-F]{1,4}:){1,7}:\b'
    r'|\b::(?:[0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}\b'
)

# Domain name (must have at least one dot, exclude common false positives)
DOMAIN_PATTERN = re.compile(
    r'\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+(?:[a-zA-Z]{2,})\b'
)

# URL pattern
URL_PATTERN = re.compile(
    r'https?://[^\s<>"\')\]]+',
    re.IGNORECASE
)

# File hashes
MD5_PATTERN = re.compile(r'\b[a-fA-F0-9]{32}\b')
SHA1_PATTERN = re.compile(r'\b[a-fA-F0-9]{40}\b')
SHA256_PATTERN = re.compile(r'\b[a-fA-F0-9]{64}\b')

# CVE IDs
CVE_PATTERN = re.compile(r'\bCVE-\d{4}-\d{4,}\b', re.IGNORECASE)

# Email addresses
EMAIL_PATTERN = re.compile(
    r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
)

# Bitcoin addresses (P2PKH, P2SH, Bech32)
BTC_PATTERN = re.compile(
    r'\b(?:[13][a-km-zA-HJ-NP-Z1-9]{25,34}|bc1[a-zA-HJ-NP-Z0-9]{25,90})\b'
)

# MITRE ATT&CK technique IDs
MITRE_PATTERN = re.compile(r'\bT\d{4}(?:\.\d{3})?\b')

# Common false-positive domains to exclude
FALSE_POSITIVE_DOMAINS = {
    "example.com", "example.org", "example.net",
    "google.com", "facebook.com", "twitter.com", "youtube.com",
    "github.com", "linkedin.com", "instagram.com",
    "w3.org", "schema.org", "jquery.com", "googleapis.com",
    "cloudflare.com", "amazonaws.com", "gstatic.com",
    "wordpress.org", "wordpress.com", "wp.com",
    "gravatar.com", "creativecommons.org",
}

# Common false-positive IPs (private, loopback, etc.)
FALSE_POSITIVE_IPS = {
    "127.0.0.1", "0.0.0.0", "255.255.255.255",
    "192.168.0.1", "192.168.1.1", "10.0.0.1",
}


@dataclass
class ExtractedIOCs:
    """Container for extracted IOCs from text."""
    ipv4_addresses: List[str] = field(default_factory=list)
    ipv6_addresses: List[str] = field(default_factory=list)
    domains: List[str] = field(default_factory=list)
    urls: List[str] = field(default_factory=list)
    md5_hashes: List[str] = field(default_factory=list)
    sha1_hashes: List[str] = field(default_factory=list)
    sha256_hashes: List[str] = field(default_factory=list)
    cve_ids: List[str] = field(default_factory=list)
    email_addresses: List[str] = field(default_factory=list)
    btc_addresses: List[str] = field(default_factory=list)
    mitre_technique_ids: List[str] = field(default_factory=list)

    @property
    def total_count(self) -> int:
        return (
            len(self.ipv4_addresses) + len(self.ipv6_addresses) +
            len(self.domains) + len(self.urls) +
            len(self.md5_hashes) + len(self.sha1_hashes) + len(self.sha256_hashes) +
            len(self.cve_ids) + len(self.email_addresses) +
            len(self.btc_addresses) + len(self.mitre_technique_ids)
        )

    @property
    def has_iocs(self) -> bool:
        return self.total_count > 0

    def to_dict(self) -> Dict[str, List[str]]:
        """Convert to dictionary for JSON serialization."""
        result = {}
        if self.ipv4_addresses:
            result["ipv4"] = self.ipv4_addresses
        if self.ipv6_addresses:
            result["ipv6"] = self.ipv6_addresses
        if self.domains:
            result["domains"] = self.domains
        if self.urls:
            result["urls"] = self.urls
        if self.md5_hashes:
            result["md5"] = self.md5_hashes
        if self.sha1_hashes:
            result["sha1"] = self.sha1_hashes
        if self.sha256_hashes:
            result["sha256"] = self.sha256_hashes
        if self.cve_ids:
            result["cve"] = self.cve_ids
        if self.email_addresses:
            result["email"] = self.email_addresses
        if self.btc_addresses:
            result["btc"] = self.btc_addresses
        if self.mitre_technique_ids:
            result["mitre"] = self.mitre_technique_ids
        return result

    def to_flat_list(self) -> List[Dict[str, str]]:
        """Convert to flat list of {type, value} dicts for DB storage."""
        items = []
        for ioc_type, values in [
            ("ipv4", self.ipv4_addresses),
            ("ipv6", self.ipv6_addresses),
            ("domain", self.domains),
            ("url", self.urls),
            ("md5", self.md5_hashes),
            ("sha1", self.sha1_hashes),
            ("sha256", self.sha256_hashes),
            ("cve", self.cve_ids),
            ("email", self.email_addresses),
            ("btc", self.btc_addresses),
            ("mitre_technique", self.mitre_technique_ids),
        ]:
            for value in values:
                items.append({"type": ioc_type, "value": value})
        return items


def _is_private_ip(ip: str) -> bool:
    """Check if an IP address is private/reserved."""
    parts = ip.split(".")
    if len(parts) != 4:
        return False
    try:
        first = int(parts[0])
        second = int(parts[1])
    except ValueError:
        return False

    # 10.x.x.x, 172.16-31.x.x, 192.168.x.x, 127.x.x.x
    if first == 10:
        return True
    if first == 172 and 16 <= second <= 31:
        return True
    if first == 192 and second == 168:
        return True
    if first == 127:
        return True
    return False


def _is_likely_hash(text: str, length: int) -> bool:
    """Check if a hex string is likely a hash (not a color code, etc.)."""
    # All hex chars
    if not all(c in "0123456789abcdefABCDEF" for c in text):
        return False
    # Check it's not all same char (e.g., "0000...0000")
    if len(set(text.lower())) <= 2:
        return False
    return True


def extract_iocs(text: str, defang: bool = True) -> ExtractedIOCs:
    """
    Extract IOCs from text content.

    Args:
        text: Article text or any string content
        defang: If True, also handle defanged IOCs (e.g., hxxp://, [.], etc.)

    Returns:
        ExtractedIOCs container with all found indicators
    """
    if not text or len(text) < 10:
        return ExtractedIOCs()

    # Optionally refang defanged IOCs for extraction
    work_text = text
    if defang:
        work_text = work_text.replace("hxxp", "http")
        work_text = work_text.replace("hXXp", "http")
        work_text = work_text.replace("[.]", ".")
        work_text = work_text.replace("(.)", ".")
        work_text = work_text.replace("[:]", ":")
        work_text = work_text.replace("[at]", "@")
        work_text = work_text.replace(" dot ", ".")

    result = ExtractedIOCs()

    # Extract CVE IDs (high confidence, few false positives)
    cves: Set[str] = set()
    for match in CVE_PATTERN.finditer(work_text):
        cves.add(match.group().upper())
    result.cve_ids = sorted(cves)

    # Extract MITRE ATT&CK technique IDs
    mitre: Set[str] = set()
    for match in MITRE_PATTERN.finditer(work_text):
        mitre.add(match.group())
    result.mitre_technique_ids = sorted(mitre)

    # Extract SHA256 hashes (before SHA1 and MD5 to avoid substring matches)
    sha256: Set[str] = set()
    for match in SHA256_PATTERN.finditer(work_text):
        h = match.group().lower()
        if _is_likely_hash(h, 64):
            sha256.add(h)
    result.sha256_hashes = sorted(sha256)

    # Extract SHA1 hashes (exclude substrings of SHA256)
    sha1: Set[str] = set()
    for match in SHA1_PATTERN.finditer(work_text):
        h = match.group().lower()
        if _is_likely_hash(h, 40) and not any(h in s for s in sha256):
            sha1.add(h)
    result.sha1_hashes = sorted(sha1)

    # Extract MD5 hashes (exclude substrings of SHA1/SHA256)
    md5: Set[str] = set()
    all_longer_hashes = sha256 | sha1
    for match in MD5_PATTERN.finditer(work_text):
        h = match.group().lower()
        if _is_likely_hash(h, 32) and not any(h in s for s in all_longer_hashes):
            md5.add(h)
    result.md5_hashes = sorted(md5)

    # Extract IPv4 addresses
    ipv4: Set[str] = set()
    for match in IPV4_PATTERN.finditer(work_text):
        ip = match.group()
        if ip not in FALSE_POSITIVE_IPS and not _is_private_ip(ip):
            ipv4.add(ip)
    result.ipv4_addresses = sorted(ipv4)

    # Extract domains (filter out false positives)
    domains: Set[str] = set()
    for match in DOMAIN_PATTERN.finditer(work_text):
        domain = match.group().lower()
        # Filter out common false positives and very short domains
        if (
            domain not in FALSE_POSITIVE_DOMAINS
            and len(domain) > 5
            and not domain.endswith((".png", ".jpg", ".gif", ".css", ".js", ".svg"))
            and "." in domain
        ):
            # Only include if it looks like a potentially malicious/relevant domain
            # (has unusual TLD or appears in IOC context)
            domains.add(domain)
    # Only include domains that appear near IOC-related words
    ioc_context_words = {"malware", "c2", "command", "control", "ioc", "indicator",
                         "compromise", "threat", "attack", "phishing", "ransomware",
                         "exploit", "backdoor", "trojan", "botnet"}
    text_lower = work_text.lower()
    has_ioc_context = any(word in text_lower for word in ioc_context_words)
    if has_ioc_context:
        result.domains = sorted(domains)[:50]  # Cap at 50 to avoid noise

    # Extract email addresses
    emails: Set[str] = set()
    for match in EMAIL_PATTERN.finditer(work_text):
        email = match.group().lower()
        # Filter out common non-IOC emails
        domain = email.split("@")[1] if "@" in email else ""
        if domain not in FALSE_POSITIVE_DOMAINS:
            emails.add(email)
    result.email_addresses = sorted(emails)

    # Extract Bitcoin addresses
    btc: Set[str] = set()
    for match in BTC_PATTERN.finditer(work_text):
        btc.add(match.group())
    result.btc_addresses = sorted(btc)

    if result.has_iocs:
        logger.debug(f"Extracted {result.total_count} IOCs from text ({len(text)} chars)")

    return result
