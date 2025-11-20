# URL Schema Design: Understanding all_urls, primary_url, and source_detail_url

## Overview

The BaseIncident schema has three different URL fields with distinct purposes:

1. **`all_urls`** - List of enrichment URLs (news articles, official statements)
2. **`primary_url`** - Best URL selected by LLM from `all_urls` (Phase 2)
3. **`source_detail_url`** - CTI/infrastructure detail page (NOT for enrichment)

## URL Field Purposes

### `all_urls: List[str]` - Enrichment URLs
**Purpose**: URLs that will be used by LLM enrichment in Phase 2 to extract incident details.

**Contains**:
- News article URLs (e.g., `https://krebsonsecurity.com/2024/...`)
- Official statements/press releases
- Any URLs with useful content for LLM extraction

**Phase 1**: Collect ALL enrichment URLs here
**Phase 2**: LLM will read articles from these URLs and select the best one as `primary_url`

### `primary_url: Optional[str]` - Best URL for Enrichment
**Purpose**: The single best URL selected by LLM from `all_urls` for detailed enrichment.

**Phase 1**: Always `None` (all URLs are in `all_urls`)
**Phase 2**: LLM selects the best URL from `all_urls` and sets it here

**Why**: LLM enrichment only reads ONE article (the best one), not all URLs.

### `source_detail_url: Optional[str]` - CTI/Infrastructure Detail Page
**Purpose**: Links to the source platform's detail page (for reference, NOT enrichment).

**Contains**:
- Ransomware.live detail page: `https://api.ransomware.live/v2/victim/...`
- Leak site detail pages
- Source platform internal pages

**NOT for enrichment**: These URLs are NOT in `all_urls` because:
- They're from CTI/infrastructure platforms, not news sources
- They may require authentication or be unstable
- They're for reference/tracking, not content extraction

**Example from ransomware.live**:
```python
# Enrichment URLs (articles) go in all_urls
all_urls = ["https://bbc.com/article", "https://reuters.com/article"]

# CTI detail page goes in source_detail_url (NOT in all_urls)
source_detail_url = "https://api.ransomware.live/v2/victim/xyz123"
```

## Current Issues

### Issue 1: News Sources Setting Redundant `source_detail_url`

**Problem**: News sources are setting `source_detail_url=article_url`, which is redundant since `article_url` is already in `all_urls`.

**Current (WRONG)**:
```python
# News sources (thehackernews, krebsonsecurity, etc.)
all_urls=[article_url],
source_detail_url=article_url,  # ❌ REDUNDANT - already in all_urls
```

**Should be**:
```python
# News sources
all_urls=[article_url],
source_detail_url=None,  # ✅ No CTI detail page for news articles
```

### Issue 2: Confusion About Purpose

**Question**: Is `source_detail_url` necessary when we have `primary_url`?

**Answer**: YES, but only for CTI/infrastructure sources:
- `primary_url` = best news article for enrichment (from `all_urls`)
- `source_detail_url` = CTI platform detail page (NOT in `all_urls`, NOT for enrichment)

They serve different purposes:
- `primary_url` is for LLM to read and extract details
- `source_detail_url` is for tracking/reference on the source platform

## Recommendations

### For News Sources (thehackernews, krebsonsecurity, etc.)
```python
BaseIncident(
    ...
    primary_url=None,  # Phase 1: always None
    all_urls=[article_url],  # Article URL for enrichment
    source_detail_url=None,  # No CTI detail page
    ...
)
```

### For CTI Sources (ransomware.live, etc.)
```python
BaseIncident(
    ...
    primary_url=None,  # Phase 1: always None
    all_urls=press_article_urls,  # News articles for enrichment
    source_detail_url=detail_page_url,  # CTI platform detail page
    leak_site_url=claim_url,  # Leak site URL (also CTI/infra)
    ...
)
```

### For Archive Sources (databreach, konbriefing)
```python
BaseIncident(
    ...
    primary_url=None,  # Phase 1: always None
    all_urls=[article_url],  # Article URL for enrichment
    source_detail_url=None,  # Archive sources don't have detail pages
    ...
)
```

## Summary

| Field | Purpose | Phase 1 | Phase 2 | Example |
|-------|---------|---------|---------|---------|
| `all_urls` | Enrichment URLs (news articles) | Collect all | LLM selects best | `["https://bbc.com/article", "https://reuters.com/article"]` |
| `primary_url` | Best URL for enrichment | `None` | Set by LLM | `"https://bbc.com/article"` |
| `source_detail_url` | CTI platform detail page | Set if available | Unchanged | `"https://api.ransomware.live/v2/victim/xyz"` |

**Key Points**:
1. `all_urls` contains enrichment URLs (news articles)
2. `primary_url` is selected by LLM from `all_urls` in Phase 2
3. `source_detail_url` is ONLY for CTI/infrastructure detail pages, NOT news article URLs
4. News sources should set `source_detail_url=None` (not redundant with `all_urls`)

