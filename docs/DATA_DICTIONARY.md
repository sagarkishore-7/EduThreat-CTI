# Data Dictionary

Column contract for the normalized dataset exports produced by
`eduthreat-v2-export` and `GET /api/v2/export/{dataset}.{csv,json}`. Every export
is a flat join over the star-schema analytical layer (`src/edu_cti_v2/models/star.py`),
so categorical values are single controlled-vocabulary tokens and multi-valued
CTI is delivered as delimited columns (`incidents`) or long tables (`mitre`,
`cves`, `iocs`). No client-side preprocessing is required.

## `incidents` (one row per open canonical incident)

| Column | Source | Notes |
| --- | --- | --- |
| `incident_id` | `fact_incident.canonical_incident_id` | stable UUID |
| `institution_name` | `fact_incident.institution_name` | |
| `institution_type` | `dim_institution_type.slug` | controlled vocab, normalized |
| `vendor_name` | `fact_incident.vendor_name` | set for vendor-mediated breaches |
| `country_code` / `country` | `dim_country.country_code` / `.name` | ISO 3166-1 alpha-2 |
| `region`, `city` | `fact_incident` | free text where disclosed |
| `attack_category` | `dim_attack_category.slug` | controlled vocab |
| `attack_family` | `dim_attack_category.family` | ransomware / phishing / data_breach / ... |
| `attack_vector` | `dim_attack_vector.slug` | controlled vocab |
| `severity` | `dim_severity.slug` | critical..informational |
| `threat_actor` | `dim_threat_actor.label` | canonical actor name |
| `ransomware_family` | `dim_ransomware_family.label` | canonical family name |
| `incident_date`, `detection_date`, `disclosure_date` | `fact_incident` | ISO dates |
| `incident_year`, `incident_quarter` | `fact_incident` | derived |
| `dwell_time_days` | `fact_incident` | detection minus incident, where known |
| `disclosure_lag_days` | `fact_incident` | disclosure minus incident |
| `recovery_days`, `downtime_days` | `fact_incident` | operational impact |
| `records_affected_exact/min/max` | `fact_incident` | interval-valued; nulls preserved |
| `ransom_demanded_usd`, `ransom_paid_usd` | `fact_incident` | numeric where disclosed |
| `data_exfiltrated`, `data_encrypted`, `is_vendor_breach` | `fact_incident` | booleans |
| `teaching_disrupted`, `research_disrupted` | `fact_incident` | education-specific impact |
| `attribution_confidence` | `fact_incident` | confirmed..speculative (reprocess) |
| `source_reliability` | `fact_incident` | Admiralty A-F (reprocess) |
| `enrichment_confidence`, `completeness_score`, `source_count` | `fact_incident` | quality measures |
| `mitre_techniques` | `bridge_incident_mitre_technique` | `\|`-delimited technique IDs |
| `data_categories` | `bridge_incident_data_category` | `\|`-delimited slugs |
| `cves` | `bridge_incident_cve` | `\|`-delimited CVE IDs |

## `mitre` (one row per incident-technique)

| Column | Source |
| --- | --- |
| `incident_id` | `bridge_incident_mitre_technique.canonical_incident_id` |
| `institution_name` | `fact_incident` |
| `technique_id` | `dim_mitre_technique.technique_id` (Txxxx[.yyy]) |
| `technique_name` | `dim_mitre_technique.name` |
| `tactic` | `bridge_incident_mitre_technique.tactic_slug` |

## `cves` (one row per incident-CVE)

| Column | Source |
| --- | --- |
| `incident_id` | `bridge_incident_cve.canonical_incident_id` |
| `institution_name` | `fact_incident` |
| `cve_id` | `dim_cve.cve_id` |
| `cve_year` | `dim_cve.year` |

## `iocs` (one row per indicator)

| Column | Source |
| --- | --- |
| `incident_id` | `incident_ioc.canonical_incident_id` |
| `institution_name` | `fact_incident` |
| `ioc_type` | `incident_ioc.ioc_type` (ipv4/ipv6/domain/url/email/md5/sha1/sha256/sha512) |
| `value` | `incident_ioc.value` |
| `confidence` | `incident_ioc.confidence` |

## `campaigns` (one row per campaign)

| Column | Source |
| --- | --- |
| `campaign_id`, `campaign_name`, `campaign_type`, `status`, `confidence` | `campaigns` |
| `first_seen_date`, `last_seen_date` | `campaigns` |
| `member_count`, `confirmed_member_count` | `campaigns` |
| `actors`, `vendors`, `cves`, `attack_categories` | `campaigns` (JSON arrays) |

Suppressed campaigns are excluded from the export.
