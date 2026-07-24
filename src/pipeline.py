from __future__ import annotations

import asyncio
import logging

from src.models import ChainEvent

logger = logging.getLogger(__name__)


async def consume_events(events: asyncio.Queue[ChainEvent]) -> None:
    """Placeholder for parsing, signal generation, risk checks, and execution."""
    while True:
        event = await events.get()
        try:
            logger.debug("received on-chain event at slot=%s", event.slot)
        finally:
            events.task_done()
