"""실제 거래 설정의 명시적 비밀 제외 projection. 환경 전체를 저장하지 않는다."""
from __future__ import annotations

from decimal import Decimal
import inspect
import math
import os

from src.research.n3_shadow import digest

# 각 이름은 실제 모듈의 상수다. URL, 키, 주소와 mutable 원장은 제외한다.
MODULE_FIELDS = {
    "monitor": (
        "PAPER_BUY_BASIS_POINTS", "SINGLE_STRENGTH_LAMPORTS", "MIN_ACCUMULATION_TRADE_LAMPORTS",
        "ACCUMULATION_TARGET_LAMPORTS", "ACCUMULATION_WINDOW_SECONDS", "DEX_SCREENER_POLL_SECONDS",
        "MOMENTUM_MIN_VOLUME_M5_USD", "MOMENTUM_MIN_NET_BUYS_M5", "MOMENTUM_MIN_BUY_SELL_RATIO",
        "MOMENTUM_MIN_LIQUIDITY_USD", "MOMENTUM_MIN_PAIR_AGE_SECONDS", "MOMENTUM_MAX_DISCOVERY_TOKENS",
        "MOMENTUM_MAX_CANDIDATES", "MOMENTUM_MAX_RAW_PAIRS", "MOMENTUM_MAX_SHADOW_CANDIDATES",
        "MOMENTUM_SHADOWS_PER_TICK", "MOMENTUM_SHADOW_CAPTURE_INTERVAL_SECONDS", "MOMENTUM_ENTRY_COOLDOWN_SECONDS",
        "UNKNOWN_WHALE_MIN_COUNT", "UNKNOWN_WHALE_SIGNATURE_LIMIT", "ROUTE_B_MIN_SAFETY_SCORE",
        "ROUTE_B_MIN_LIQUIDITY_USD", "ROUTE_B_MIN_LP_LOCKED_PERCENT", "TOKEN_TRADE_COOLDOWN_SECONDS",
        "STOP_LOSS_TOKEN_COOLDOWN_SECONDS", "STOP_LOSS_BLACKLIST_MAX_TOKENS", "ROUTE_A_LOSS_SIZE_REDUCTION_STREAK",
        "ROUTE_A_PAUSE_STREAK", "ROUTE_A_PAUSE_SECONDS", "WALLET_TARGET_COUNT", "WALLET_FEEDER_TRIGGER_COUNT",
        "WALLET_FEEDER_COOLDOWN_SECONDS", "MONITOR_MAINTENANCE_INTERVAL_SECONDS", "SUBSCRIPTION_REFRESH_SECONDS",
        "HEALTH_WRITE_INTERVAL_SECONDS", "ROUTE_B_CONFIRM_TIMEOUT_SECONDS", "MAX_PENDING_SHADOW_SIGNALS",
        "HELIUS_RECOVERY_PROBE_SECONDS", "MAX_SEEN_WALLET_SIGNATURES", "MONITOR_RSS_CEILING_BYTES",
    ),
    "executor": (
        "PAPER_BUY_BASIS_POINTS", "LIVE_BUY_PERCENT", "ROUTE_B_SIZE_MULTIPLIER", "JITO_FALLBACK_TIP_LAMPORTS",
        "JITO_MIN_TIP_LAMPORTS", "MAX_FEE_BASIS_POINTS", "MAX_ABSOLUTE_FEE_LAMPORTS", "MAX_COMPUTE_UNIT_LIMIT",
        "COMPUTE_UNIT_MARGIN", "MIN_FEE_RESERVE_LAMPORTS", "_QUOTE_INTERVAL_SECONDS", "_JUPITER_MAX_QUOTE_ATTEMPTS",
        "_JUPITER_MAX_RESET_WAIT_SECONDS", "MAX_ENTRY_PRICE_IMPACT_PCT", "MAX_EXIT_PRICE_IMPACT_PCT",
    ),
    "risk_manager": (
        "INITIAL_PAPER_LAMPORTS", "TAKE_PROFIT_RATIO", "SECOND_TAKE_PROFIT_RATIO", "STOP_LOSS_RATIO",
        "ROUTE_B_TAKE_PROFIT_RATIO", "ROUTE_B_STOP_LOSS_RATIO", "BREAK_EVEN_STOP_RATIO", "TAKE_PROFIT_SELL_PERCENT",
        "PRICE_POLL_SECONDS", "QUOTE_FAILURE_WARNING_COUNT", "DEGRADED_QUOTE_GAP_SECONDS",
    ),
    "analyzer": ("ROUTE_B_MINIMUM_LIQUIDITY_USD", "ANALYZER_CACHE_TTL_SECONDS", "ANALYZER_CACHE_MAX_ENTRIES"),
    "solana_rpc": (
        "RPC_PROVIDER_STATE_SCHEMA_VERSION", "RPC_CIRCUIT_FAILURE_THRESHOLD", "RPC_CIRCUIT_COOLDOWN_SECONDS",
        "RPC_HALF_OPEN_LEASE_SECONDS", "RPC_MAX_INLINE_BACKOFF_SECONDS",
    ),
    "wallet_performance": ("EVALUATION_DELAY_SECONDS", "MAX_RETURN_PERCENT", "COOLDOWN_SECONDS"),
    "prospective_features": ("FEATURE_COLLECTION_SCHEMA_VERSION", "MAX_PRE_SIGNAL_SNAPSHOTS", "SNAPSHOT_BUCKET_SECONDS",
                             "SNAPSHOT_TTL_SECONDS", "MAX_TRACKED_MINT_PAIRS"),
}
LEGACY_MONITOR_KEYS = frozenset({"paper_buy_basis_points", "single_strength_lamports", "momentum_min_volume_m5_usd",
    "momentum_min_net_buys_m5", "momentum_min_buy_sell_ratio", "momentum_min_liquidity_usd",
    "momentum_min_pair_age_seconds", "route_b_min_safety_score", "unknown_whale_min_count"})
NUMERIC_KEYS = frozenset({f"{module}.{name.lower().strip('_')}" for module, names in MODULE_FIELDS.items() for name in names}) | LEGACY_MONITOR_KEYS | frozenset({
    "config_contract_version", "wallet_reload_seconds", "approved_signal_max_open_positions", "rpc_overall_attempt_budget",
    "rpc_provider_local_attempts", "analyzer.minimum_safety_score", "analyzer.maximum_developer_percent",
    "analyzer.minimum_lp_locked_percent", "analyzer.minimum_liquidity_usd", "quote.default_slippage_bps",
    "telemetry.predictor_schema", "telemetry.receipt_schema", "telemetry.outcome_schema",
    "feeder.max_wallets", "feeder.refresh_seconds", "feeder.min_sol_balance", "feeder.max_daily_transactions",
    "feeder.signatures_per_program", "feeder.max_candidates", "feeder.rpc_min_interval_seconds",
    "feeder.elite_reserved_slots", "feeder.http_concurrency",
}) | frozenset(f"rpc.{provider}.{field}" for provider in ("alchemy", "chainstack", "ankr", "helius", "solana_public") for field in ("max_rps", "light_priority", "heavy_priority"))
BOOLEAN_KEYS = frozenset({"observation_mode", "approved_signal_paper_mode", "monitor.allow_unverified_wallets",
    "monitor.helius_ws_configured", "monitor.standard_ws_enabled"}) | frozenset(
    f"rpc.{provider}.enabled" for provider in ("alchemy", "chainstack", "ankr", "helius", "solana_public"))
ENUM_VALUES = {"trading_mode": frozenset({"paper", "live"}),
               "strategy_selection": frozenset({"wallet_route_a_and_dex_momentum_b"})}
CONFIG_KEYS = NUMERIC_KEYS | BOOLEAN_KEYS | frozenset(ENUM_VALUES)


def validate_config(config: dict) -> dict:
    """허용 키/타입만 정규화한다. 누락 키는 생략, 잘못된 값은 명시적으로 실패한다."""
    if not isinstance(config, dict) or set(config) - CONFIG_KEYS:
        raise RuntimeError("Telemetry config must use explicit non-secret keys")
    result = {}
    for key, value in sorted(config.items()):
        if key in NUMERIC_KEYS:
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise RuntimeError("Telemetry numeric config must be finite")
        elif key in BOOLEAN_KEYS:
            if not isinstance(value, bool):
                raise RuntimeError("Telemetry boolean config must be boolean")
        elif not isinstance(value, str) or value not in ENUM_VALUES[key]:
            raise RuntimeError("Telemetry enum config invalid")
        result[key] = value
    return result


def config_fingerprint(config: dict) -> str:
    return digest(validate_config(config))


def project_config(config: dict | None) -> dict:
    """임의 호출자 입력에서 비밀/unknown 키를 버린 뒤 동일한 타입 계약을 적용한다."""
    return validate_config({key: value for key, value in (config or {}).items() if key in CONFIG_KEYS})


def effective_config(environ=None) -> dict:
    """dotenv를 읽거나 네트워크 호출 없이 process의 적용 값과 기본값을 추출한다."""
    from src import analyzer, executor, monitor, risk_manager, solana_rpc, wallet_performance, wallet_feeder
    from src.research import prospective_features
    from src.research import entry_telemetry_epoch
    modules = {"monitor": monitor, "executor": executor, "risk_manager": risk_manager,
               "analyzer": analyzer, "solana_rpc": solana_rpc, "wallet_performance": wallet_performance,
               "prospective_features": prospective_features}
    environ = os.environ if environ is None else environ
    values = {"config_contract_version": 2}
    for module, names in MODULE_FIELDS.items():
        for name in names:
            value = getattr(modules[module], name)
            values[f"{module}.{name.lower().strip('_')}"] = float(value) if isinstance(value, Decimal) else value
    for key in LEGACY_MONITOR_KEYS:
        values[key] = getattr(monitor, key.upper())
    for name in ("minimum_safety_score", "maximum_developer_percent", "minimum_lp_locked_percent", "minimum_liquidity_usd"):
        value = analyzer.AnalyzerSettings.__dataclass_fields__[name].default
        values[f"analyzer.{name}"] = float(value) if isinstance(value, Decimal) else value
    values["quote.default_slippage_bps"] = inspect.signature(executor.jupiter_quote).parameters["slippage_bps"].default
    values["monitor.allow_unverified_wallets"] = monitor.TEST_ALLOW_UNVERIFIED_WALLETS
    values["strategy_selection"] = "wallet_route_a_and_dex_momentum_b"
    ws_template = str(environ.get("HELIUS_RPC_WS_URL", "")).strip()
    # 키 존재 여부는 resolver의 사용 가능 여부에만 반영하며 값 자체는 버린다.
    ws_resolved = ws_template.replace("${HELIUS_API_KEY}", str(environ.get("HELIUS_API_KEY", "")).strip())
    values["monitor.helius_ws_configured"] = bool(ws_template and "${" not in ws_resolved
        and ("${HELIUS_API_KEY}" not in ws_template or str(environ.get("HELIUS_API_KEY", "")).strip()))
    public_ws = str(environ.get("SOLANA_PUBLIC_WS_URL", "")).strip() or monitor.SOLANA_PUBLIC_WS_URL
    if not public_ws.startswith(("ws://", "wss://")):
        raise RuntimeError("Telemetry standard WSS configuration invalid")
    values["monitor.standard_ws_enabled"] = True
    for key in ("observation_mode", "approved_signal_paper_mode"):
        raw = str(environ.get(key.upper(), "false")).strip().lower()
        if raw not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
            raise RuntimeError("Telemetry effective mode invalid")
        values[key] = raw in {"1", "true", "yes", "on"}
    mode = str(environ.get("TRADING_MODE", "paper")).strip().lower()
    values["trading_mode"] = mode
    max_positions = int(str(environ.get("APPROVED_SIGNAL_MAX_OPEN_POSITIONS", "8")).strip())
    if not 1 <= max_positions <= 20:
        raise RuntimeError("Telemetry max positions must be between 1 and 20")
    values["approved_signal_max_open_positions"] = max_positions
    values["wallet_reload_seconds"] = max(1.0, float(environ.get("WALLET_RELOAD_SECONDS", "5")))
    max_wallets = max(1, int(environ.get("WALLET_MAX_WALLETS", "20")))
    values.update({
        "feeder.max_wallets": max_wallets,
        "feeder.refresh_seconds": max(1, int(environ.get("WALLET_REFRESH_HOURS", "1"))) * 3600,
        "feeder.min_sol_balance": max(0.0, float(environ.get("WALLET_MIN_SOL_BALANCE", "0.1"))),
        "feeder.max_daily_transactions": max(1, int(environ.get("WALLET_MAX_DAILY_TX", "300"))),
        "feeder.signatures_per_program": max(1, min(1000, int(environ.get("WALLET_SIGNATURES_PER_PROGRAM", "50")))),
        "feeder.max_candidates": max(1, int(environ.get("WALLET_MAX_CANDIDATES", "250"))),
        "feeder.rpc_min_interval_seconds": max(0.0, float(environ.get("WALLET_RPC_MIN_INTERVAL_SECONDS", "0.3"))),
        "feeder.elite_reserved_slots": min(max_wallets, max(1, int(environ.get("WALLET_ELITE_RESERVED_SLOTS", "7")))),
        "feeder.http_concurrency": wallet_feeder.FeederSettings.__dataclass_fields__["http_concurrency"].default,
    })
    # URL와 credential은 provider router에만 넘기고 출력/hash에는 포함하지 않는다.
    providers = {provider.name: provider for provider in solana_rpc.provider_configs_from_env(environ)}
    for name, (_, rps_setting, default_rps) in solana_rpc.PROVIDER_ENVIRONMENTS.items():
        values[f"rpc.{name}.enabled"] = name in providers
        values[f"rpc.{name}.max_rps"] = providers[name].max_rps if name in providers else default_rps
        values[f"rpc.{name}.light_priority"] = solana_rpc.LIGHT_PROVIDER_ORDER.index(name)
        values[f"rpc.{name}.heavy_priority"] = solana_rpc.HEAVY_PROVIDER_ORDER.index(name)
    for key, setting, default, maximum in (
        ("rpc_overall_attempt_budget", "SOLANA_RPC_OVERALL_ATTEMPT_BUDGET", solana_rpc.RPC_OVERALL_ATTEMPT_BUDGET, 20),
        ("rpc_provider_local_attempts", "SOLANA_RPC_PROVIDER_ATTEMPTS", solana_rpc.RPC_PROVIDER_LOCAL_ATTEMPTS, 3),
    ):
        values[key] = solana_rpc._positive_int(environ.get(setting, default), setting=setting, maximum=maximum)
    values.update({f"telemetry.{stream}_schema": version for stream, version in entry_telemetry_epoch.SCHEMAS.items()})
    return validate_config(values)
