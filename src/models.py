from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ChainEvent:
    slot: int | None
    payload: dict[str, Any]

