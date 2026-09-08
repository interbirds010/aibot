"""미래 Research용 signal 이전 projection을 작고 결정적으로 보존한다."""

from __future__ import annotations

import math
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


FEATURE_COLLECTION_SCHEMA_VERSION = 1
MOMENTUM_COLLECTOR_VERSION = "momentum_pre_signal_v1"
MAX_PRE_SIGNAL_SNAPSHOTS = 6
SNAPSHOT_BUCKET_SECONDS = 60.0
SNAPSHOT_TTL_SECONDS = 900.0
MAX_TRACKED_MINT_PAIRS = 256
SNAPSHOT_FIELDS = frozenset({
    "snapshot_at_epoch",
    "volume_m5_usd",
    "buys_m5",
    "sells_m5",
    "liquidity_usd",
    "price_usd",
})


def _finite_number(value: Any, *, minimum: float = 0.0) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < minimum:
        return None
    return number


def _timestamp(value: Any) -> float | None:
    number = _finite_number(value)
    if number is not None:
        return number
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    epoch = parsed.timestamp()
    return epoch if math.isfinite(epoch) and epoch >= 0 else None


def _iso_timestamp(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


@dataclass(frozen=True, slots=True)
class MomentumPreSignalSnapshot:
    snapshot_at_epoch: float
    volume_m5_usd: float
    buys_m5: int
    sells_m5: int
    liquidity_usd: float
    price_usd: float | None

    def projection(self) -> dict[str, int | float | None]:
        return asdict(self)


def _normalized_snapshot(value: Any) -> MomentumPreSignalSnapshot | None:
    if not isinstance(value, dict):
        return None
    timestamp = _timestamp(value.get("snapshot_at_epoch"))
    volume = _finite_number(value.get("volume_m5_usd"))
    buys = _finite_number(value.get("buys_m5"))
    sells = _finite_number(value.get("sells_m5"))
    liquidity = _finite_number(value.get("liquidity_usd"))
    price = _finite_number(value.get("price_usd"))
    if None in (timestamp, volume, buys, sells, liquidity):
        return None
    return MomentumPreSignalSnapshot(
        snapshot_at_epoch=float(timestamp),
        volume_m5_usd=float(volume),
        buys_m5=int(buys),
        sells_m5=int(sells),
        liquidity_usd=float(liquidity),
        price_usd=float(price) if price is not None else None,
    )


def normalize_prospective_feature_collection(
    value: Any,
    *,
    signal_timestamp: Any,
) -> dict[str, Any]:
    """허용 projection만 남기고 signal 이후 snapshot을 제거한다."""
    if not isinstance(value, dict):
        return {}
    if value.get("schema_version") != FEATURE_COLLECTION_SCHEMA_VERSION:
        return {}
    if value.get("collector_version") != MOMENTUM_COLLECTOR_VERSION:
        return {}
    signal_epoch = _timestamp(signal_timestamp)
    snapshots = value.get("pre_signal_snapshots")
    if signal_epoch is None or not isinstance(snapshots, list):
        return {}
    normalized: dict[int, MomentumPreSignalSnapshot] = {}
    for raw in snapshots:
        snapshot = _normalized_snapshot(raw)
        if snapshot is None:
            continue
        if not signal_epoch - SNAPSHOT_TTL_SECONDS <= snapshot.snapshot_at_epoch <= signal_epoch:
            continue
        bucket = int(snapshot.snapshot_at_epoch // SNAPSHOT_BUCKET_SECONDS)
        normalized[bucket] = snapshot
    selected = sorted(
        normalized.values(), key=lambda item: item.snapshot_at_epoch
    )[-MAX_PRE_SIGNAL_SNAPSHOTS:]
    if not selected:
        return {}
    return {
        "schema_version": FEATURE_COLLECTION_SCHEMA_VERSION,
        "collector_version": MOMENTUM_COLLECTOR_VERSION,
        "prospective_collection_start": _iso_timestamp(
            selected[0].snapshot_at_epoch
        ),
        "snapshot_count": len(selected),
        "pre_signal_snapshots": [item.projection() for item in selected],
    }


def prospective_feature_collection_eligible(
    row: Any,
    *,
    minimum_snapshots: int = 2,
) -> bool:
    """기존 row와 prospective row를 명시적으로 분리한다."""
    if not isinstance(row, dict):
        return False
    normalized = normalize_prospective_feature_collection(
        row.get("prospective_feature_collection"),
        signal_timestamp=row.get("signal_detected_at"),
    )
    return int(normalized.get("snapshot_count", 0) or 0) >= max(
        1, int(minimum_snapshots)
    )


class MomentumSnapshotStore:
    """mint/pair별 minute projection만 보존하는 bounded LRU store다."""

    def __init__(self) -> None:
        self._series: OrderedDict[
            tuple[str, str], deque[MomentumPreSignalSnapshot]
        ] = OrderedDict()

    @property
    def series_count(self) -> int:
        return len(self._series)

    @property
    def snapshot_count(self) -> int:
        return sum(len(values) for values in self._series.values())

    def _evict_expired(self, now_epoch: float) -> None:
        oldest_allowed = now_epoch - SNAPSHOT_TTL_SECONDS
        for key, values in list(self._series.items()):
            while values and values[0].snapshot_at_epoch < oldest_allowed:
                values.popleft()
            if not values:
                self._series.pop(key, None)

    def record(
        self,
        *,
        mint: str,
        pair_address: str,
        snapshot_at_epoch: Any,
        volume_m5_usd: Any,
        buys_m5: Any,
        sells_m5: Any,
        liquidity_usd: Any,
        price_usd: Any = None,
    ) -> bool:
        timestamp = _timestamp(snapshot_at_epoch)
        snapshot = _normalized_snapshot({
            "snapshot_at_epoch": timestamp,
            "volume_m5_usd": volume_m5_usd,
            "buys_m5": buys_m5,
            "sells_m5": sells_m5,
            "liquidity_usd": liquidity_usd,
            "price_usd": price_usd,
        })
        normalized_mint = str(mint).strip()
        normalized_pair = str(pair_address).strip()
        if (
            snapshot is None
            or not normalized_mint
            or not normalized_pair
            or len(normalized_mint) > 80
            or len(normalized_pair) > 160
        ):
            return False
        self._evict_expired(snapshot.snapshot_at_epoch)
        key = (normalized_mint, normalized_pair)
        values = self._series.get(key)
        if values is None:
            values = deque(maxlen=MAX_PRE_SIGNAL_SNAPSHOTS)
            self._series[key] = values
        elif values and snapshot.snapshot_at_epoch < values[-1].snapshot_at_epoch:
            return False
        bucket = int(snapshot.snapshot_at_epoch // SNAPSHOT_BUCKET_SECONDS)
        if values and int(values[-1].snapshot_at_epoch // SNAPSHOT_BUCKET_SECONDS) == bucket:
            values[-1] = snapshot
        else:
            values.append(snapshot)
        self._series.move_to_end(key)
        while len(self._series) > MAX_TRACKED_MINT_PAIRS:
            self._series.popitem(last=False)
        return True

    def collection(
        self,
        *,
        mint: str,
        pair_address: str,
        signal_timestamp: Any,
    ) -> dict[str, Any]:
        signal_epoch = _timestamp(signal_timestamp)
        if signal_epoch is None:
            return {}
        self._evict_expired(signal_epoch)
        values = self._series.get((str(mint).strip(), str(pair_address).strip()))
        if not values:
            return {}
        return normalize_prospective_feature_collection(
            {
                "schema_version": FEATURE_COLLECTION_SCHEMA_VERSION,
                "collector_version": MOMENTUM_COLLECTOR_VERSION,
                "pre_signal_snapshots": [item.projection() for item in values],
            },
            signal_timestamp=signal_epoch,
        )
