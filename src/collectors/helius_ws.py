from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable

from websockets.asyncio.client import connect

from src.models import ChainEvent

logger = logging.getLogger(__name__)


async def stream_logs(
    ws_url: str,
    output: asyncio.Queue[ChainEvent],
    program_ids: Iterable[str] = (),
) -> None:
    """Subscribe to logs and reconnect with capped exponential backoff."""
    mentions = list(program_ids)
    log_filter: str | dict[str, list[str]] = {"mentions": mentions} if mentions else "all"
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [log_filter, {"commitment": "confirmed"}],
    }
    delay = 1
    while True:
        try:
            async with connect(ws_url, ping_interval=20, ping_timeout=20) as socket:
                await socket.send(json.dumps(request))
                acknowledgement = json.loads(await socket.recv())
                if "error" in acknowledgement:
                    raise RuntimeError(f"subscription rejected: {acknowledgement['error']}")
                delay = 1
                async for message in socket:
                    payload = json.loads(message)
                    params = payload.get("params") or {}
                    context = (params.get("result") or {}).get("context") or {}
                    await output.put(ChainEvent(slot=context.get("slot"), payload=payload))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Helius WebSocket disconnected; retrying in %ss", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

