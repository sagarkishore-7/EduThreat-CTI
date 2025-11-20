# Adding New Sources to EduThreat-CTI

This guide explains how to add new data sources to EduThreat-CTI. Adding sources is straightforward and follows a consistent pattern.

## Overview

EduThreat-CTI organizes sources into three categories:

1. **Curated Sources**: Sources with dedicated education sector sections/endpoints
   - Example: KonBriefing (university cyber attacks database)
   - Example: Ransomware.live (education sector filter)

2. **News Sources**: Keyword-based search sources
   - Example: The Hacker News (search for "university", "school", etc.)
   - Example: SecurityWeek (keyword search)

3. **RSS Sources**: RSS feed sources
   - Example: DataBreaches.net RSS feed

## Step-by-Step Guide

### Step 1: Determine Source Type

Identify which category your source fits into:

- **Curated**: Does the source have a dedicated education section or filter?
- **News**: Is it a general security news site that requires keyword searching?
- **RSS**: Does it provide an RSS feed?

### Step 2: Create Source Builder File

Create a new file in the appropriate directory:

- **Curated**: `edu_cti/sources/curated/<source_name>.py`
- **News**: `edu_cti/sources/news/<source_name>.py`
- **RSS**: `edu_cti/sources/rss/<source_name>.py`

### Step 3: Implement Builder Function

Create a builder function following this pattern:

```python
"""
<Source Name> source implementation for EduThreat-CTI.

Description of the source and how it works.
"""

import logging
from typing import List, Optional

from edu_cti.core.models import BaseIncident
from edu_cti.core.http import HttpClient, build_http_client

logger = logging.getLogger(__name__)

SOURCE_NAME = "<source_name>"


def build_<source_name>_incidents(
    *,
    max_pages: Optional[int] = None,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Build incidents from <Source Name>.
    
    Args:
        max_pages: Maximum pages to fetch (None = all)
        client: HTTP client (creates default if not provided)
        save_callback: Optional callback for incremental saving
        
    Returns:
        List of BaseIncident objects
    """
    if client is None:
        client = build_http_client()
    
    incidents = []
    
    # Your implementation here
    # 1. Fetch data from source
    # 2. Parse and extract incident information
    # 3. Create BaseIncident objects
    # 4. Use save_callback if provided for incremental saving
    
    # Example structure:
    # for item in source_items:
    #     incident = BaseIncident(
    #         incident_id=make_incident_id(SOURCE_NAME, unique_string),
    #         source=SOURCE_NAME,
    #         source_event_id=item.id,
    #         university_name=item.university,
    #         # ... other fields
    #         all_urls=[item.url],
    #         # ...
    #     )
    #     incidents.append(incident)
    
    return incidents
```

### Step 4: Register Source

Add your source to the registry in `edu_cti/core/sources.py`:

```python
# Import your builder
from edu_cti.sources.curated import build_<source_name>_incidents

# Add to registry
CURATED_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    # ... existing sources
    "<source_name>": build_<source_name>_incidents,
}
```

### Step 5: Test Your Source

```bash
# Test your source builder
python -c "from edu_cti.sources.curated import build_<source_name>_incidents; incidents = build_<source_name>_incidents(max_pages=1); print(f'Found {len(incidents)} incidents')"

# Run full pipeline with your source
python -m edu_cti.cli.pipeline --groups curated --curated-sources <source_name>
```

### Step 6: Document Your Source

Add your source to `docs/SOURCES.md`:

```markdown
### <Source Name>

- **Type**: Curated/News/RSS
- **URL**: https://...
- **Description**: Brief description
- **Implementation**: `edu_cti/sources/curated/<source_name>.py`
```

## Examples

### Example 1: Curated Source (KonBriefing)

See `edu_cti/sources/curated/konbriefing.py` for a complete example of a curated source.

### Example 2: News Source (The Hacker News)

See `edu_cti/sources/news/thehackernews.py` for a complete example of a news source.

### Example 3: RSS Source (DataBreaches RSS)

See `edu_cti/sources/rss/databreaches_rss.py` for a complete example of an RSS source.

## Best Practices

1. **Follow Naming Conventions**:
   - Function: `build_<source_name>_incidents`
   - Constant: `SOURCE_NAME = "<source_name>"`
   - File: `<source_name>.py`

2. **Error Handling**:
   - Use try/except for network errors
   - Log errors with context
   - Continue processing other items on individual failures

3. **Incremental Saving**:
   - Use `save_callback` if provided
   - Saves progress during long-running fetches

4. **Rate Limiting**:
   - HTTP client handles delays automatically
   - Add additional delays if needed for specific sources

5. **Deduplication**:
   - Use `make_incident_id()` for consistent IDs
   - Include all relevant URLs in `all_urls`

6. **Date Handling**:
   - Normalize dates to YYYY-MM-DD format
   - Set `date_precision` appropriately
   - Handle missing dates gracefully

## Common Patterns

### Pattern 1: Paginated Source

```python
page = 1
while True:
    if max_pages and page > max_pages:
        break
    
    url = f"https://source.com/page/{page}"
    soup = client.get_soup(url)
    
    if not soup or not has_more_pages(soup):
        break
    
    # Process page
    items = extract_items(soup)
    for item in items:
        # Create incidents
        pass
    
    page += 1
```

### Pattern 2: RSS Feed

```python
from edu_cti.core.rss import parse_rss_feed

items = parse_rss_feed(rss_url, max_age_days=max_age_days)
for item in items:
    # Create incidents from RSS items
    pass
```

### Pattern 3: Search-Based Source

```python
keywords = ["university", "school", "education"]
for keyword in keywords:
    url = f"https://source.com/search?q={keyword}"
    # Process search results
    pass
```

## Testing

Write tests for your source:

```python
# tests/test_sources/test_<source_name>.py

def test_build_<source_name>_incidents():
    incidents = build_<source_name>_incidents(max_pages=1)
    assert len(incidents) > 0
    assert all(isinstance(inc, BaseIncident) for inc in incidents)
```

## Troubleshooting

### Issue: Source not found in registry

**Solution**: Check that you've imported and registered the source in `edu_cti/core/sources.py`.

### Issue: No incidents found

**Solution**: 
- Check source URL is correct
- Verify parser logic extracts data correctly
- Add debug logging to see what's being fetched

### Issue: Deduplication not working

**Solution**:
- Ensure consistent incident IDs using `make_incident_id()`
- Include all relevant URLs in `all_urls`

## Questions?

- Check existing source implementations for examples
- Open an issue for questions
- Review `edu_cti/core/models.py` for BaseIncident structure

Thank you for adding sources to EduThreat-CTI!

