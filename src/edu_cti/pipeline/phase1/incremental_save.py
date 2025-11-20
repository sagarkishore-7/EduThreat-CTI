"""
Incremental saving utilities for pipeline sources.

This module provides utilities for saving incidents incrementally during collection,
preventing data loss if an error occurs mid-way through fetching.
"""

from __future__ import annotations

import logging
from typing import Callable, List, Optional

from src.edu_cti.core.models import BaseIncident

logger = logging.getLogger(__name__)


class IncrementalSaver:
    """
    Handles incremental saving of incidents during collection.
    Saves incidents in batches to prevent data loss on errors.
    """
    
    def __init__(
        self,
        save_callback: Callable[[List[BaseIncident]], int],
        batch_size: int = 50,
        source_name: Optional[str] = None,
    ):
        """
        Initialize incremental saver.
        
        Args:
            save_callback: Function that takes a list of incidents and returns count saved
            batch_size: Number of incidents to accumulate before saving (default: 50)
            source_name: Optional source name for logging (defaults to extracting from incidents)
        """
        self.save_callback = save_callback
        self.batch_size = batch_size
        self.source_name = source_name
        self.buffer: List[BaseIncident] = []
        self.total_saved = 0
        self.total_processed = 0
    
    def _get_source_name(self) -> str:
        """Get source name from incidents or fallback to default."""
        if self.source_name:
            return self.source_name
        
        # Try to extract source name from incidents in buffer
        if self.buffer:
            # Get the most common source in the buffer
            sources = [inc.source for inc in self.buffer if inc.source]
            if sources:
                # Return the first source (they should all be the same for a batch)
                return sources[0]
        
        return "unknown"
    
    def add(self, incident: BaseIncident) -> None:
        """
        Add an incident to the buffer. Saves automatically when buffer reaches batch_size.
        """
        self.buffer.append(incident)
        self.total_processed += 1
        
        if len(self.buffer) >= self.batch_size:
            self.flush()
    
    def add_batch(self, incidents: List[BaseIncident]) -> None:
        """
        Add multiple incidents. Saves in batches as needed.
        """
        for incident in incidents:
            self.add(incident)
    
    def flush(self) -> int:
        """
        Force save all buffered incidents.
        
        Returns:
            Number of incidents saved
        """
        if not self.buffer:
            return 0
        
        try:
            saved = self.save_callback(self.buffer)
            self.total_saved += saved
            source_name = self._get_source_name()
            logger.debug(
                f"{source_name}: Saved batch of {len(self.buffer)} incidents "
                f"({saved} new, {self.total_saved} total saved)"
            )
            self.buffer.clear()
            return saved
        except Exception as e:
            source_name = self._get_source_name()
            logger.error(
                f"{source_name}: Error saving batch: {e}",
                exc_info=True
            )
            # Keep buffer in case of error - don't lose data
            raise
    
    def finish(self) -> int:
        """
        Save any remaining buffered incidents and return total saved.
        
        Returns:
            Total number of incidents saved
        """
        if self.buffer:
            self.flush()
        
        source_name = self._get_source_name()
        logger.info(
            f"{source_name}: Collection complete. "
            f"Processed {self.total_processed} incidents, saved {self.total_saved} new"
        )
        return self.total_saved


def create_db_saver(conn, is_rss: bool = False, source_name: Optional[str] = None) -> IncrementalSaver:
    """
    Create an IncrementalSaver that saves to database.
    
    Args:
        conn: Database connection
        is_rss: Whether this is for RSS feeds (affects deduplication logic)
        source_name: Optional source name for logging (will be extracted from incidents if not provided)
    
    Returns:
        IncrementalSaver configured for database saving
    """
    from src.edu_cti.pipeline.phase1.__main__ import _ingest_batch
    
    def save_to_db(incidents: List[BaseIncident]) -> int:
        """Save incidents to database and return count of new incidents."""
        return _ingest_batch(conn, incidents, is_rss=is_rss)
    
    return IncrementalSaver(save_to_db, batch_size=50, source_name=source_name)

