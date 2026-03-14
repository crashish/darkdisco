"""Base connector interface for dark web sources."""

from __future__ import annotations

import abc
import logging
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class RawMention:
    """A single mention found by a connector, before matching against watch terms.

    Metadata may include file attachments as raw bytes under the 'file_data' key.
    The pipeline strips file_data before JSON serialization and processes
    archives (ZIP/RAR with password support) in _process_file_mentions().
    """

    source_name: str
    source_url: str | None = None
    title: str = ""
    content: str = ""
    author: str | None = None
    discovered_at: datetime = field(default_factory=datetime.utcnow)
    metadata: dict = field(default_factory=dict)


class BaseConnector(abc.ABC):
    """Abstract base for all dark web source connectors.

    Each connector knows how to:
    1. Connect to its source (Tor, API, etc.)
    2. Poll for new content since last check
    3. Return normalized RawMention objects
    """

    name: str = "base"
    source_type: str = "other"

    def __init__(self, config: dict | None = None):
        self.config = config or {}

    @abc.abstractmethod
    async def poll(self, since: datetime | None = None) -> list[RawMention]:
        """Fetch new content from this source.

        Args:
            since: Only return content newer than this timestamp.

        Returns:
            List of raw mentions to be matched against watch terms.
        """
        ...

    @abc.abstractmethod
    async def health_check(self) -> dict:
        """Check if the source is reachable.

        Returns:
            Dict with at least {"healthy": bool, "message": str}.
        """
        ...

    async def setup(self) -> None:
        """One-time initialization (e.g., authenticate, establish session)."""
        pass

    async def teardown(self) -> None:
        """Cleanup resources."""
        pass
