"""
Scheduler module for continuous data ingestion and enrichment.

Schedules:
- Weekly: Full historical ingestion (curated + news sources)
- Every 2 hours: RSS feeds for real-time news collection
- Continuous: LLM enrichment of newly ingested incidents
"""

from src.edu_cti.scheduler.scheduler import IngestionScheduler

__all__ = ["IngestionScheduler"]
