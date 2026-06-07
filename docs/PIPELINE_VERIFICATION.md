# Pipeline Stage Verification

This document records an end-to-end verification of the v2 pipeline against the
live Railway corpus (public proxy), sampling one or more real scenarios per
stage. It is a point-in-time check; counts reflect the snapshot at the time of
writing (June 2026, ~534 open canonical incidents from ~35,258 raw source
incidents).

## Stage 1: Collection and deduplication

**What it does.** Sources write raw observations to `source_incidents` keyed by
`source_event_key`; the ingest hash and event key prevent the same observation
from being stored twice by the same source.

**Verification.** The funnel holds: ~35,258 raw source incidents collapse to 534
open canonical incidents, so collection is broad and downstream canonicalization
is doing the consolidation rather than collection storing duplicates. No action
required.

## Stage 2: Article fetch and selection

**What it does.** Each candidate URL is fetched through the Scrapling-first tier
chain; the best fetched article per incident is selected for enrichment by score.

**Finding and fix.** Selection used `max()` on score alone, which returns the
first maximal element, so equal-scoring candidates were selected by list order
and enrichment was not reproducible. A deterministic tiebreaker was added
(score, then longer body, then URL) in `services/fetching.py`.

## Stage 3: Enrichment

**What it does.** The relevance gate plus schema-constrained extraction populate
`source_enrichments.typed_enrichment` and the canonical projection; long articles
use the three-call thematic split; vendor breaches fan out named victims.

**Verification.** 1,498 source enrichments back 534 canonicals. The canonical
projection carries the structured sub-objects (`attack_dynamics`, `data_impact`,
`system_impact`, `mitre_attack_techniques`, `timeline`, `recovery_metrics`), and
the star backfill reads them without re-querying the model.

**Finding (deferred to reprocess).** IOC indicator arrays and the structured
CVE field are usually null in the current corpus; CVEs occur in free text and are
recovered by regex in the star builder. The new `attribution_confidence`,
`source_reliability`, and per-vulnerability `cwe_id` fields populate at the next
reprocess.

## Stage 4: Canonicalization

**What it does.** Source observations of the same real-world event merge into one
canonical incident, recording the match type and score per membership.

**Verification.** Match-type distribution over the live corpus: `seed` 534,
`name_date` 35, `url_exact` 24, `vendor_date` 1. The `name_date` fuzzy matches
confirm the token-sort name matcher (threshold 85, 14-day window) is merging
genuine cross-source duplicates, for example Hartford Public Schools (4 sources),
Lewis and Clark College (4 sources), and Columbia University (3 sources), without
collapsing distinct institutions. Thresholds live in `services/canonicalization.py`
(`_identity_match_quality` >= 85, `institution_names_match` 85/92, 14-day window).

## Stage 5: Campaign correlation

**What it does.** Canonical incidents that share a vendor, CVE, or actor signature
are grouped into campaigns with a confidence score and member roles.

**Finding and fix.** Correlation had not been re-run after the corpus rebuild, so
the `campaigns` table was empty. Running `V2CampaignService.run_correlation`
produced 35 candidate campaigns, 149 memberships, 441 edges, and 23 signatures
from 594 rows. Correlation should be scheduled as a standing orchestration step
so it stays current as new canonicals land (WS-E).

## Stage 6: Analytics serving

**What it does.** The dashboard reads breakdowns and metrics from the serving
layer.

**Finding and fix.** Breakdowns were aggregated in Python over mixed-case
canonical columns and merged again on the frontend. The unfiltered dashboard path
now serves normalized breakdowns from the star schema via SQL `GROUP BY`, so
`"university"` and `"University"` collapse to one row server-side (243 vs the raw
split of 243 = 110 + 133). Parity with the legacy path was confirmed on the
shared dimensions; the legacy path remains as a fallback.

## Coverage status and increase plan

Measured at the snapshot: 33,735 of 35,258 source incidents (95.7%) are not yet
enriched, 16,146 of 27,402 article fetch attempts (59%) succeed, and of enriched
sources the education relevance gate keeps 594 and drops 913 (about 60% dropped).

The dominant coverage lever is therefore the reprocess backlog draining, which is
bounded by LLM throughput rather than by collection or by the gate. The increase
plan, in priority order:

1. Let the reprocess drain the 33,735-source backlog; this is the single largest
   source of additional canonicals and is already in progress.
2. After the reprocess settles, run a false-negative eval on a labelled sample of
   the 913 gate-dropped enrichments to decide whether to relax borderline
   edtech-vendor and school-district cases. Do not change the gate mid-reprocess.
3. Recover part of the 41% fetch-failure rate by tuning the retry tiers
   (archive.org and browser rescue) for the failing domains.
4. Keep `campaign_correlate` scheduled so new canonicals join campaigns as they
   land (the manual run produced 35 campaigns / 149 memberships).

## Summary

The pipeline is structurally sound. The verification produced three concrete
fixes already applied (deterministic article selection, lease-heartbeat logging,
SQL `GROUP BY` breakdowns) and one operational action (re-run and schedule
campaign correlation). New CTI fields are added to the schema and populate at the
next reprocess.
