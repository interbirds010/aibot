from __future__ import annotations

import asyncio
import logging
import os

from src.collectors.helius_ws import stream_logs
from src.config import Settings
from src.models import ChainEvent
from src.logging_utils import configure_safe_logging
from src.monitor import DEX_PROGRAMS
from src.pipeline import consume_events
from src.risk_manager import run_risk_loop


# Master safety switch. Keep True until paper results have been reviewed.
PAPER_TRADING = True


async def heartbeat() -> None:
    while True:
        logging.getLogger("service").info("heartbeat: paper_trading=%s", PAPER_TRADING)
        await asyncio.sleep(60)


async def run() -> None:
    os.environ["TRADING_MODE"] = "paper" if PAPER_TRADING else "live"
    settings = Settings.from_env()
    events: asyncio.Queue[ChainEvent] = asyncio.Queue(settings.event_queue_size)
    async with asyncio.TaskGroup() as tasks:
        for dex_name, program_id in DEX_PROGRAMS.items():
            tasks.create_task(
                stream_logs(settings.helius_ws_url, events, [program_id]),
                name=f"helius-ws-{dex_name}",
            )
        tasks.create_task(consume_events(events), name="event-consumer")
        tasks.create_task(run_risk_loop(PAPER_TRADING), name="risk-manager")
        tasks.create_task(heartbeat(), name="service-heartbeat")


def main() -> None:
    configure_safe_logging()
    asyncio.run(run())


if __name__ == "__main__":
    main()
