from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True, slots=True)
class Settings:
    helius_ws_url: str
    helius_http_url: str
    trading_mode: str
    event_queue_size: int = 2_000

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        api_key = os.getenv("HELIUS_API_KEY", "").strip()
        ws_url = os.getenv("HELIUS_RPC_WS_URL", "").replace("${HELIUS_API_KEY}", api_key)
        http_url = os.getenv("HELIUS_RPC_HTTP_URL", "").replace("${HELIUS_API_KEY}", api_key)
        if not api_key or not ws_url or not http_url:
            raise RuntimeError("HELIUS_API_KEY and Helius RPC URLs must be configured")

        mode = os.getenv("TRADING_MODE", "paper").strip().lower()
        if mode not in {"paper", "live"}:
            raise RuntimeError("TRADING_MODE must be 'paper' or 'live'")
        return cls(ws_url, http_url, mode)

