"""Lightweight real-time operations dashboard for the local trading bot."""

from __future__ import annotations

import asyncio
import html
import hmac
import json
import os
import tempfile
from hashlib import sha256
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp
import streamlit as st
from dotenv import load_dotenv
from src.dashboard_auth import request_is_authenticated
from src.dashboard_clipboard import clipboard_button_document, clipboard_script
from src.dashboard_progress import (
    clear_manual_close_progress_document,
    manual_close_progress_document,
)
from src.logging_utils import install_redacting_formatters, redact_sensitive_text
from src.service_health import service_is_fresh as monitor_service_is_fresh
from src.wallet_performance import MAX_RETURN_PERCENT, capped_return_percent

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WALLETS_PATH = PROJECT_ROOT / "data" / "wallets.json"
PERFORMANCE_PATH = PROJECT_ROOT / "data" / "wallet_performance.json"
LEDGER_PATH = PROJECT_ROOT / "data" / "paper_trades.json"
SERVICE_LOG_PATH = PROJECT_ROOT / "logs" / "service.stderr.log"
STATS_PATH = PROJECT_ROOT / "data" / "dashboard_stats.json"
LAMPORTS_PER_SOL = 1_000_000_000
WSOL_MINT = "So11111111111111111111111111111111111111112"
DEFAULT_INITIAL_SOL = 10.0
KST = timezone(timedelta(hours=9), name="KST")
load_dotenv()
install_redacting_formatters()
DASHBOARD_AUTH_COOKIE = "solana_ai_bot_auth"


st.set_page_config(
    page_title="Solana AI Bot · Live Ops",
    page_icon="◎",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
      :root {
          --ops-bg: #07100f;
          --ops-surface: #0c1816;
          --ops-surface-raised: #10201d;
          --ops-border: #203a34;
          --ops-border-soft: #172c27;
          --ops-text: #edf8f4;
          --ops-muted: #91aaa2;
          --ops-accent: #42d392;
          --ops-accent-soft: rgba(66, 211, 146, .12);
          --ops-danger: #ff7a83;
          --ops-radius: 16px;
      }
      html {
          color-scheme: dark;
          background: var(--ops-bg);
      }
      body, .stApp {
          background:
              radial-gradient(circle at 88% -8%, rgba(66, 211, 146, .09), transparent 28rem),
              var(--ops-bg);
          color: var(--ops-text);
          font-family: "Pretendard Variable", Pretendard, -apple-system,
              BlinkMacSystemFont, "Segoe UI", sans-serif;
          text-rendering: optimizeLegibility;
          -webkit-font-smoothing: antialiased;
      }
      [data-testid="stHeader"] {
          background: rgba(7, 16, 15, .82);
          backdrop-filter: blur(14px);
          -webkit-backdrop-filter: blur(14px);
          border-bottom: 1px solid rgba(32, 58, 52, .55);
      }
      [data-testid="stToolbar"] {
          right: .75rem;
      }
      .block-container {
          width: min(100%, 1480px);
          max-width: 1480px;
          padding: 2rem 2rem 3rem;
      }
      h1, h2, h3 {
          color: var(--ops-text);
          letter-spacing: -.035em;
      }
      h2, h3 {
          margin-top: 1.75rem;
      }
      p, [data-testid="stCaptionContainer"] {
          line-height: 1.65;
      }
      .hero {
          position: relative;
          isolation: isolate;
          overflow: hidden;
          padding: 1.5rem 1.6rem;
          border: 1px solid var(--ops-border);
          border-radius: var(--ops-radius);
          background:
              linear-gradient(120deg, rgba(66, 211, 146, .08), transparent 48%),
              var(--ops-surface);
          box-shadow: 0 18px 50px rgba(1, 12, 9, .22);
      }
      .hero::after {
          content: "";
          position: absolute;
          inset: 0 0 auto;
          height: 1px;
          background: linear-gradient(90deg, transparent, rgba(118, 244, 185, .56), transparent);
          pointer-events: none;
      }
      .eyebrow {
          color: var(--ops-accent);
          font-size: .74rem;
          letter-spacing: .13em;
          font-weight: 750;
      }
      .hero h1 {
          margin: .32rem 0 .45rem;
          max-width: none;
          font-size: clamp(1.75rem, 4vw, 2.55rem);
          line-height: 1.08;
      }
      .muted {
          color: var(--ops-muted);
      }
      .hero-status {
          display: flex;
          flex-wrap: wrap;
          align-items: center;
          gap: .45rem;
          font-size: .9rem;
      }
      .status-dot {
          display: inline-block;
          flex: 0 0 auto;
          width: 9px;
          height: 9px;
          border-radius: 50%;
          background: var(--ops-accent);
          box-shadow: 0 0 0 4px rgba(66, 211, 146, .11);
      }
      .status-off {
          background: var(--ops-danger);
          box-shadow: 0 0 0 4px rgba(255, 122, 131, .12);
      }
      .ops-kpi-grid {
          display: grid;
          grid-template-columns: repeat(4, minmax(0, 1fr));
          gap: .8rem;
          margin: 1rem 0 1.4rem;
      }
      .ops-kpi-card {
          min-width: 0;
          padding: 1.05rem 1.1rem;
          border: 1px solid var(--ops-border-soft);
          border-radius: var(--ops-radius);
          background: linear-gradient(180deg, rgba(255,255,255,.018), transparent), var(--ops-surface);
          box-shadow: 0 12px 28px rgba(1, 12, 9, .16);
      }
      .ops-kpi-card[data-emphasis="true"] {
          border-color: rgba(66, 211, 146, .42);
          background: linear-gradient(145deg, var(--ops-accent-soft), transparent 64%), var(--ops-surface);
      }
      .ops-kpi-label {
          margin-bottom: .42rem;
          color: var(--ops-muted);
          font-size: .76rem;
          font-weight: 650;
          letter-spacing: .015em;
      }
      .ops-kpi-value {
          overflow-wrap: anywhere;
          color: var(--ops-text);
          font-size: clamp(1.28rem, 2.2vw, 1.72rem);
          font-weight: 780;
          letter-spacing: -.035em;
          line-height: 1.1;
      }
      .ops-kpi-value.positive {
          color: #74e6ad;
      }
      .ops-kpi-value.negative {
          color: #ff8f97;
      }
      .ops-kpi-detail {
          margin-top: .48rem;
          overflow-wrap: anywhere;
          color: var(--ops-muted);
          font-size: .75rem;
          line-height: 1.35;
      }
      .ops-kpi-live {
          display: inline-flex;
          align-items: center;
          gap: .5rem;
      }
      div[data-testid="stMetric"] {
          min-width: 0;
          padding: 1rem 1.05rem;
          border: 1px solid var(--ops-border-soft);
          border-radius: var(--ops-radius);
          background: var(--ops-surface);
      }
      div[data-testid="stMetricLabel"] {
          color: var(--ops-muted);
      }
      div[data-testid="stMetricValue"] {
          overflow-wrap: anywhere;
          font-weight: 760;
          letter-spacing: -.025em;
      }
      div[data-testid="stExpander"] {
          overflow: hidden;
          border-color: var(--ops-border-soft);
          border-radius: var(--ops-radius);
          background: rgba(12, 24, 22, .68);
      }
      div[data-testid="stExpander"] summary {
          min-width: 0;
      }
      div[data-testid="stExpander"] summary p {
          overflow-wrap: anywhere;
          word-break: break-word;
      }
      div[data-testid="stExpander"] div[data-testid="stMetric"] {
          padding: .72rem .9rem;
      }
      div[data-testid="stExpander"] div[data-testid="stMetricLabel"] {
          font-size: .8rem;
      }
      div[data-testid="stExpander"] div[data-testid="stMetricValue"] {
          font-size: 1.35rem;
          line-height: 1.25;
      }
      [data-testid="stTabs"] [data-baseweb="tab-list"] {
          gap: .35rem;
          overflow-x: auto;
          scrollbar-width: thin;
      }
      [data-testid="stTabs"] [data-baseweb="tab"] {
          flex: 0 0 auto;
          min-height: 2.7rem;
          white-space: nowrap;
      }
      [data-testid="stDataFrame"],
      [data-testid="stTable"] {
          max-width: 100%;
          overflow-x: auto;
          border-radius: var(--ops-radius);
      }
      [data-testid="stButton"] button {
          min-height: 2.6rem;
          border-radius: 12px;
          border-color: var(--ops-border);
          background: var(--ops-surface-raised);
          color: var(--ops-text);
          font-weight: 650;
          transition: transform .14s ease, border-color .14s ease, background .14s ease;
      }
      [data-testid="stButton"] button p,
      [data-testid="stFormSubmitButton"] button p {
          color: inherit;
      }
      [data-testid="stFormSubmitButton"] button {
          min-height: 2.7rem;
          border-radius: 12px;
          border-color: var(--ops-accent);
          background: var(--ops-accent);
          color: #052017;
          font-weight: 750;
      }
      [data-testid="stButton"] button:hover {
          border-color: rgba(66, 211, 146, .55);
          background: var(--ops-accent-soft);
      }
      [data-testid="stButton"] button:active {
          transform: translateY(1px);
      }
      @media (max-width: 1024px) {
          .block-container {
              padding-inline: 1.25rem;
          }
          .ops-kpi-grid {
              grid-template-columns: repeat(2, minmax(0, 1fr));
          }
      }
      @media (max-width: 767px) {
          [data-testid="stHeader"] {
              height: 3rem;
          }
          .block-container {
              width: 100%;
              padding: 1rem .82rem 2.25rem;
          }
          .hero {
              padding: 1.15rem 1rem;
          }
          .hero h1 {
              max-width: none;
              font-size: 1.72rem;
          }
          .hero-status {
              align-items: flex-start;
              font-size: .82rem;
          }
          .ops-kpi-grid {
              grid-template-columns: minmax(0, 1fr);
              gap: .65rem;
              margin: .8rem 0 1.15rem;
          }
          .ops-kpi-card {
              padding: .92rem 1rem;
          }
          .ops-kpi-value {
              font-size: 1.42rem;
          }
          div[data-testid="stHorizontalBlock"] {
              flex-direction: column !important;
              flex-wrap: nowrap !important;
              gap: .7rem !important;
          }
          div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] {
              width: 100% !important;
              min-width: 0 !important;
              flex: 1 1 auto !important;
          }
          div[data-testid="stMetric"] {
              width: 100%;
              padding: .9rem 1rem;
          }
          div[data-testid="stMetricValue"] {
              font-size: 1.35rem;
          }
          [data-testid="stButton"] button {
              width: 100%;
          }
          [data-testid="stTabs"] {
              min-width: 0;
          }
          [data-testid="stTabs"] [data-baseweb="tab-panel"] {
              padding-top: .75rem;
          }
          div[data-testid="stExpander"] summary {
              padding: .8rem .85rem;
          }
          div[data-testid="stCode"] {
              max-width: 100%;
          }
          div[data-testid="stCode"] code {
              white-space: pre-wrap !important;
              overflow-wrap: anywhere;
              word-break: break-word;
          }
      }
      @media (prefers-reduced-motion: reduce) {
          *, *::before, *::after {
              scroll-behavior: auto !important;
              transition-duration: .01ms !important;
              animation-duration: .01ms !important;
              animation-iteration-count: 1 !important;
          }
      }
      @media (prefers-reduced-transparency: reduce) {
          [data-testid="stHeader"] {
              background: var(--ops-bg);
              backdrop-filter: none;
              -webkit-backdrop-filter: none;
          }
      }
    </style>
    """,
    unsafe_allow_html=True,
)
def render_operations_overview(
    *,
    total_return: float,
    realized_pnl: int,
    book_value: int,
    cash_lamports: int,
    invested_lamports: int,
    wallet_count: int,
    position_count: int,
    running: bool,
) -> None:
    """운영자가 모바일 상단에서 핵심 상태를 즉시 스캔하도록 표시한다."""
    return_class = "positive" if total_return >= 0 else "negative"
    status_class = "" if running else " status-off"
    status_text = "프로세스 정상" if running else "점검 필요"
    heartbeat_detail = "하트비트 🟢 · 2초 자동 갱신" if running else "하트비트 🔴 · 로그 확인 필요"
    wallet_ratio = f"{wallet_count}/20"
    wallet_detail = f"보유 포지션 {position_count}개 · 공급 기준 17개"
    cards = (
        (
            "종합 수익률",
            f"{total_return:+.2f}%",
            f"실현 손익 {sol(realized_pnl):+.6f} SOL",
            return_class,
            "true",
        ),
        (
            "감시 지갑",
            wallet_ratio,
            wallet_detail,
            "",
            "true",
        ),
        (
            "프로세스 하트비트",
            (
                f'<span class="ops-kpi-live"><span class="status-dot{status_class}">'
                f"</span>{html.escape(status_text)}</span>"
            ),
            heartbeat_detail,
            "",
            "true",
        ),
        (
            "장부 기준 총자산",
            f"{sol(book_value):.6f} SOL",
            (
                f"가상 현금 {sol(cash_lamports):.6f} · "
                f"투입 {sol(invested_lamports):.6f}"
            ),
            "",
            "false",
        ),
    )
    markup = ['<section class="ops-kpi-grid" aria-label="핵심 운영 지표">']
    for label, value, detail, value_class, emphasis in cards:
        markup.append(
            f'<article class="ops-kpi-card" data-emphasis="{emphasis}">'
            f'<div class="ops-kpi-label">{html.escape(label)}</div>'
            f'<div class="ops-kpi-value {value_class}">{value}</div>'
            f'<div class="ops-kpi-detail">{html.escape(detail)}</div>'
            "</article>"
        )
    markup.append("</section>")
    st.markdown("".join(markup), unsafe_allow_html=True)


def _update_auth_cookie(value: str | None) -> None:
    """Update the browser cookie and reload in one client-side operation."""
    cookie_name = json.dumps(DASHBOARD_AUTH_COOKIE)
    cookie_value = json.dumps(value or "")
    secure_suffix = (
        "; Secure"
        if os.getenv("DASHBOARD_COOKIE_SECURE", "false").lower() == "true"
        else ""
    )
    max_age = 0 if value is None else 60 * 60 * 12
    st.html(
        f"""
        <script>
          document.cookie =
            {cookie_name} + "=" + encodeURIComponent({cookie_value}) +
            "; Path=/; Max-Age={max_age}; SameSite=Strict{secure_suffix}";
          window.parent.location.reload();
        </script>
        """,
        unsafe_allow_javascript=True,
    )


def require_dashboard_login() -> None:
    """Stop dashboard rendering until environment-backed credentials are verified."""
    expected_username = os.getenv("DASHBOARD_USERNAME", "admin")
    expected_password = os.getenv("DASHBOARD_PASSWORD", "1234")
    session_secret = os.getenv(
        "DASHBOARD_SESSION_SECRET", "local_dev_session_secret_change_before_vps"
    )
    expected_token = hmac.new(
        session_secret.encode("utf-8"),
        expected_username.encode("utf-8"),
        sha256,
    ).hexdigest()
    # Invalidate the previous URL bearer-token implementation immediately.
    if "_auth" in st.query_params:
        st.query_params.pop("_auth", None)
    authenticated = request_is_authenticated(
        st.context.cookies,
        DASHBOARD_AUTH_COOKIE,
        expected_token,
        session_authenticated=(
            st.session_state.get("dashboard_authenticated") is True
        ),
    )
    if authenticated:
        st.session_state["dashboard_authenticated"] = True
        identity, logout = st.columns([8, 1])
        identity.caption(f"로그인: {expected_username}")
        if logout.button("로그아웃", use_container_width=True):
            st.session_state.pop("dashboard_authenticated", None)
            st.info("로그아웃 처리 중입니다.")
            _update_auth_cookie(None)
            st.stop()
        return

    st.markdown(
        """
        <div class="hero" style="max-width:460px;margin:10vh auto 1.4rem">
          <div class="eyebrow">SOLANA AI BOT · SECURE ACCESS</div>
          <h1>대시보드 로그인</h1>
          <div class="muted">운영 대시보드에 접근하려면 로그인하세요.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    left, login_column, right = st.columns([1, 1.15, 1])
    with login_column:
        with st.form("dashboard_login", clear_on_submit=False):
            username = st.text_input("아이디", autocomplete="username")
            password = st.text_input(
                "비밀번호", type="password", autocomplete="current-password"
            )
            submitted = st.form_submit_button(
                "로그인", type="primary", use_container_width=True
            )
        if submitted:
            username_ok = hmac.compare_digest(username, expected_username)
            password_ok = hmac.compare_digest(password, expected_password)
            if username_ok and password_ok:
                st.session_state["dashboard_authenticated"] = True
                st.success("로그인되었습니다.")
                _update_auth_cookie(expected_token)
                st.stop()
            else:
                st.error("아이디 또는 비밀번호가 올바르지 않습니다.")
    st.stop()


def load_json(path: Path, fallback: Any, cache_key: str | None = None) -> Any:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        if cache_key:
            st.session_state[f"json_cache_{cache_key}"] = document
        return document
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        if cache_key:
            return st.session_state.get(f"json_cache_{cache_key}", fallback)
        return fallback


def atomic_json(path: Path, document: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as file:
            json.dump(document, file, ensure_ascii=False, indent=2)
            file.write("\n")
            temporary = file.name
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


async def fetch_dexscreener_prices(mints: list[str]) -> dict[str, dict[str, Any]]:
    """Fetch token prices and symbols in API-safe batches of 29 plus WSOL."""
    unique_mints = list(dict.fromkeys(mints))
    if not mints:
        return {}
    timeout = aiohttp.ClientTimeout(total=3)
    pairs: list[dict[str, Any]] = []
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for start in range(0, len(unique_mints), 29):
            addresses = [*unique_mints[start:start + 29], WSOL_MINT]
            url = f"https://api.dexscreener.com/tokens/v1/solana/{','.join(addresses)}"
            async with session.get(url, headers={"accept": "application/json"}) as response:
                response.raise_for_status()
                batch = await response.json()
            if isinstance(batch, list):
                pairs.extend(item for item in batch if isinstance(item, dict))
    sol_usd = next((
        float(pair["priceUsd"]) for pair in pairs
        if (pair.get("baseToken") or {}).get("address") == WSOL_MINT and pair.get("priceUsd")
    ), 0.0)
    prices: dict[str, dict[str, Any]] = {}
    for mint in unique_mints:
        choices = [pair for pair in pairs if (pair.get("baseToken") or {}).get("address") == mint]
        if not choices:
            continue
        choices.sort(key=lambda pair: float((pair.get("liquidity") or {}).get("usd") or 0), reverse=True)
        pair = choices[0]
        quote = (pair.get("quoteToken") or {}).get("address")
        price_sol = float(pair.get("priceNative") or 0) if quote == WSOL_MINT else 0.0
        if price_sol <= 0 and sol_usd > 0 and pair.get("priceUsd"):
            price_sol = float(pair["priceUsd"]) / sol_usd
        token = pair.get("baseToken") or {}
        name = token.get("symbol") or token.get("name")
        if price_sol > 0 or name:
            prices[mint] = {"price_sol": price_sol, "name": name}
    return prices


def live_dexscreener_prices(mints: list[str]) -> dict[str, dict[str, Any]]:
    try:
        return asyncio.run(fetch_dexscreener_prices(mints))
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError):
        return {}


async def fetch_helius_symbols(mints: tuple[str, ...]) -> dict[str, str]:
    """Resolve symbols missing from DexScreener through Helius DAS metadata."""
    if not mints:
        return {}
    load_dotenv()
    api_key = os.getenv("HELIUS_API_KEY", "").strip()
    rpc_url = os.getenv("HELIUS_RPC_HTTP_URL", "").strip()
    rpc_url = rpc_url.replace("${HELIUS_API_KEY}", api_key)
    if not api_key or not rpc_url:
        return {}
    symbols: dict[str, str] = {}
    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for start in range(0, len(mints), 100):
            ids = list(mints[start:start + 100])
            request = {
                "jsonrpc": "2.0", "id": "dashboard-symbols",
                "method": "getAssetBatch", "params": {"ids": ids},
            }
            async with session.post(rpc_url, json=request) as response:
                response.raise_for_status()
                payload = await response.json()
            results = payload.get("result") or []
            for mint, asset in zip(ids, results):
                if not isinstance(asset, dict):
                    continue
                metadata = ((asset.get("content") or {}).get("metadata") or {})
                token_info = asset.get("token_info") or {}
                symbol = metadata.get("symbol") or token_info.get("symbol")
                if symbol:
                    symbols[mint] = str(symbol)
    return symbols


@st.cache_data(ttl=3600, show_spinner=False)
def cached_helius_symbols(mints: tuple[str, ...]) -> dict[str, str]:
    try:
        return asyncio.run(fetch_helius_symbols(mints))
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError):
        return {}


def sol(lamports: int | float) -> float:
    return float(lamports) / LAMPORTS_PER_SOL


def format_token_price(value: int | float) -> str:
    """Display small SOL prices as readable decimals instead of e-notation."""
    formatted = f"{float(value):.12f}".rstrip("0").rstrip(".")
    return formatted if formatted and formatted != "-0" else "0"


def service_is_fresh() -> bool:
    return monitor_service_is_fresh(SERVICE_LOG_PATH)


def format_kst(value: Any) -> str:
    """Convert an ISO-8601 UTC timestamp to a compact KST display value."""
    if value in (None, ""):
        return "-"
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return str(value)


def parse_timestamp(value: Any) -> datetime | None:
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except (TypeError, ValueError):
        return None


def format_duration(start: Any, end: Any) -> str:
    opened, closed = parse_timestamp(start), parse_timestamp(end)
    if not opened or not closed:
        return "-"
    seconds = max(0, int((closed - opened).total_seconds()))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}시간 {minutes}분 {seconds}초" if hours else f"{minutes}분 {seconds}초"


def wallet_rows(document: Any) -> list[dict[str, Any]]:
    entries = document.get("wallets", []) if isinstance(document, dict) else document
    rows: list[dict[str, Any]] = []
    for item in entries if isinstance(entries, list) else []:
        if isinstance(item, str):
            rows.append({"address": item})
        elif isinstance(item, dict) and item.get("address"):
            rows.append(item)
    return rows


def wallet_pipeline_metrics(
    wallet: dict[str, Any], performance: Any
) -> dict[str, Any]:
    """Build one live, consistent pipeline view for a watched wallet."""
    address = str(wallet["address"])
    records = performance.get("wallets", {}) if isinstance(performance, dict) else {}
    record = records.get(address, {}) if isinstance(records, dict) else {}
    record = record if isinstance(record, dict) else {}
    samples = record.get("samples", []) if isinstance(record.get("samples", []), list) else []
    pending = record.get("pending", []) if isinstance(record.get("pending", []), list) else []
    wins = int(record.get("wins", wallet.get("wins", 0)) or 0)
    losses = int(record.get("losses", wallet.get("losses", 0)) or 0)
    completed = int(record.get("evaluation_completed_count", len(samples)))
    waiting = int(record.get("evaluation_pending_count", len(pending)))
    total = wins + losses
    win_rate = wins / total * 100 if total else 0.0
    returns = [
        capped_return_percent(sample.get("return_percent", 0))
        for sample in samples if isinstance(sample, dict)
    ]
    roi = sum(returns) / len(returns) if returns else 0.0
    return {
        "지갑 주소": address,
        "승률 (관찰)": win_rate,
        "ROI (관찰)": roi,
        "지갑 거래 (평가완료/대기)": f"{completed} / {waiting}",
        "봇 가상매수": int(record.get("paper_buy_count", record.get("virtual_buys", 0)) or 0),
        "안전 차단": int(record.get("safety_block_count", record.get("scam_rejections", 0)) or 0),
    }


def winning_token_rows(
    address: str,
    performance: Any,
    token_metadata: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return compact, newest-first winning observations for one wallet."""
    records = performance.get("wallets", {}) if isinstance(performance, dict) else {}
    record = records.get(address, {}) if isinstance(records, dict) else {}
    samples = record.get("samples", []) if isinstance(record, dict) else []
    rows: list[dict[str, Any]] = []
    for sample in samples if isinstance(samples, list) else []:
        if not isinstance(sample, dict):
            continue
        raw_return = float(sample.get("return_percent", 0) or 0)
        if raw_return <= 0:
            continue
        mint = str(sample.get("mint", ""))
        token_name = (token_metadata or {}).get(mint, {}).get("name")
        rows.append({
            "토큰": sample.get("symbol") or token_name or short_mint(mint),
            "토큰 CA": mint,
            "관찰 수익률": capped_return_percent(raw_return),
            "평가 시각": format_kst(
                datetime.fromtimestamp(
                    float(sample.get("evaluated_at", 0)), tz=timezone.utc
                ).isoformat()
            ) if sample.get("evaluated_at") else "-",
            "이상치 조정": "5,000% 캡" if raw_return > MAX_RETURN_PERCENT else "-",
        })
    return list(reversed(rows))


def short_mint(mint: str) -> str:
    return f"{mint[:6]}…{mint[-4:]}" if len(mint) > 14 else mint


def position_rows(
    positions: Any, live_prices: dict[str, dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Build one consistent live mark-to-market view for every dashboard section."""
    rows: list[dict[str, Any]] = []
    values = positions.values() if isinstance(positions, dict) else []
    for position in values:
        if not isinstance(position, dict):
            continue
        mint = str(position.get("mint", ""))
        amount = int(position.get("token_amount_raw", 0))
        cost = int(position.get("remaining_cost_lamports", 0))
        has_decimals = "token_decimals" in position
        decimals = int(position.get("token_decimals", 0))
        quantity = amount / (10 ** decimals)
        live = (live_prices or {}).get(mint, {})
        # Legacy positions predate decimals persistence; do not publish a
        # misleading mark until a newly recorded position has exact units.
        live_price_sol = float(live.get("price_sol", 0)) if has_decimals else 0.0
        current_value = (
            int(quantity * live_price_sol * LAMPORTS_PER_SOL)
            if live_price_sol > 0 else int(position.get("current_value_lamports", cost))
        )
        entry_price = sol(cost) / quantity if quantity else 0.0
        current_price = live_price_sol if live_price_sol > 0 else (sol(current_value) / quantity if quantity else 0.0)
        return_percent = (current_value / cost - 1) * 100 if cost else 0.0
        rows.append({
            "토큰": live.get("name") or short_mint(mint), "Mint 주소": mint,
            "포지션 ID": str(position.get("position_id", "")),
            "매수 수량": quantity,
            "매수 평단가(SOL)": entry_price,
            "현재가(SOL)": current_price,
            "실시간 수익률": return_percent,
            "현재 가치(SOL)": sol(current_value),
            "위험 상태": str(position.get("risk_state", "NORMAL")),
            "견적 실패": int(position.get("consecutive_quote_failures", 0) or 0),
            "진입 지연(ms)": int(position.get("entry_latency_ms", 0) or 0),
            "가격 영향(%)": float(position.get("entry_price_impact_pct", 0) or 0),
            "고래 대비 격차(%)": float(position.get("copy_price_gap_pct", 0) or 0),
        })
    return rows


def render_position_rows(live_positions: list[dict[str, Any]]) -> None:
    """Render table-like position rows with a paper-only close action."""
    widths = [0.7, 1.5, 0.9, 1.0, 0.85, 0.85, 0.8, 0.8, 0.8, 0.85]
    headers = st.columns(widths)
    for column, label in zip(headers, (
        "토큰", "토큰 CA", "수량", "평단가(SOL)", "현재가(SOL)",
        "수익률", "위험 상태", "진입 지연", "고래 격차", "관리",
    )):
        column.caption(label)
    for row in live_positions:
        mint = str(row["Mint 주소"])
        columns = st.columns(widths, vertical_alignment="center")
        columns[0].write(str(row["토큰"]))
        with columns[1]:
            st.html(
                clipboard_button_document(mint),
                unsafe_allow_javascript=True,
            )
        columns[2].write(f"{float(row['매수 수량']):.6f}")
        columns[3].write(format_token_price(row["매수 평단가(SOL)"]))
        columns[4].write(format_token_price(row["현재가(SOL)"]))
        columns[5].write(f"{float(row['실시간 수익률']):+.2f}%")
        risk_state = str(row["위험 상태"])
        columns[6].write(
            f"⚠️ {risk_state}" if risk_state in {"DEGRADED", "NO_ROUTE", "EXIT_PENDING"}
            else risk_state
        )
        columns[7].write(f"{int(row['진입 지연(ms)'])}ms")
        columns[8].write(f"{float(row['고래 대비 격차(%)']):+.2f}%")
        if columns[9].button(
            "포지션 종료",
            key=f"manual_close_{row['포지션 ID'] or mint}",
            type="secondary",
            use_container_width=True,
        ):
            try:
                from src.risk_manager import close_paper_position

                proceeds = asyncio.run(close_paper_position(mint))
                st.session_state["manual_close_message"] = (
                    f"{row['토큰']} 포지션을 {sol(proceeds):.8f} SOL로 종료했습니다."
                )
                st.rerun()
            except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError, ValueError) as exc:
                st.html(
                    clear_manual_close_progress_document(),
                    unsafe_allow_javascript=True,
                )
                st.error(
                    "포지션 종료 실패: "
                    f"{redact_sensitive_text(exc)}"
                )
        st.divider()


def trade_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buys: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    for event in events:
        mint = str(event.get("mint", ""))
        position_id = str(event.get("position_id", ""))
        trade_key = position_id or mint
        if event.get("type") == "BUY":
            buys[trade_key] = event
            buys[mint] = event
            cost = int(event.get("cost_lamports", 0))
            amount = int(event.get("token_amount_raw", 0))
            decimals = max(0, int(event.get("token_decimals", 0)))
            quantity = amount / (10 ** decimals)
            rows.append(
                {"시간": format_kst(event.get("at")), "구분": "매수", "토큰 CA": mint,
                 "수량": quantity, "체결 SOL": round(sol(cost), 6),
                 "평단가(SOL)": f"{sol(cost) / quantity:.12f}" if quantity else None,
                 "개별 수익률": None, "사유": "진입"}
            )
        elif event.get("type") == "SELL":
            proceeds = int(event.get("proceeds_lamports", 0))
            pnl = int(event.get("realized_pnl_lamports", 0))
            amount = int(event.get("token_amount_raw", 0))
            released_cost = proceeds - pnl
            roi = pnl / released_cost * 100 if released_cost > 0 else 0.0
            buy = buys.get(trade_key) or buys.get(mint, {})
            buy_amount = int(buy.get("token_amount_raw", 0))
            buy_cost = int(buy.get("cost_lamports", 0))
            decimals = max(0, int(buy.get("token_decimals", 0)))
            quantity = amount / (10 ** decimals)
            buy_quantity = buy_amount / (10 ** decimals)
            rows.append(
                {"시간": format_kst(event.get("at")), "구분": "매도", "토큰 CA": mint,
                 "수량": quantity, "체결 SOL": round(sol(proceeds), 6),
                 "평단가(SOL)": (
                     f"{sol(buy_cost) / buy_quantity:.12f}" if buy_quantity else None
                 ),
                 "매도가(SOL)": f"{sol(proceeds) / quantity:.12f}" if quantity else None,
                 "개별 수익률": f"{roi:+.2f}%", "사유": event.get("reason", "청산")}
            )
        elif event.get("type") == "SIGNAL_REJECTED":
            rows.append(
                {"시간": format_kst(event.get("at")), "구분": "차단", "토큰 CA": mint,
                 "수량": None, "체결 SOL": None, "평단가(SOL)": None,
                 "매도가(SOL)": None, "개별 수익률": None,
                 "사유": f"안전점수 {event.get('safety_score', 0)} · {event.get('reason', '')}"}
            )
    return list(reversed(rows))


def render_trade_history_table(rows: list[dict[str, Any]]) -> None:
    """Render a compact history table with full-address clipboard buttons."""
    columns = [
        "시간", "구분", "토큰 CA", "수량", "체결 SOL",
        "평단가(SOL)", "개별 수익률", "사유", "매도가(SOL)",
    ]
    header = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
    body_rows: list[str] = []
    for row in rows:
        cells: list[str] = []
        for column in columns:
            value = row.get(column)
            if column == "토큰 CA" and value:
                address = str(value)
                compact = (
                    f"{address[:10]}…{address[-8:]}" if len(address) > 21 else address
                )
                safe_address = html.escape(address, quote=True)
                cells.append(
                    '<td class="mint-cell">'
                    f'<code title="{safe_address}">{html.escape(compact)}</code>'
                    f'<button class="copy-ca" data-ca="{safe_address}" '
                    'title="토큰 CA 복사" aria-label="토큰 CA 복사">⧉</button>'
                    "</td>"
                )
            else:
                display = "—" if value is None else str(value)
                cells.append(f"<td>{html.escape(display)}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    document = f"""
    <style>
      :root {{ color-scheme: dark; }}
      * {{ box-sizing: border-box; }}
      body {{ margin: 0; background: #07110f; color: #e7f7ef;
              font: 14px system-ui, -apple-system, "Segoe UI", sans-serif; }}
      .table-wrap {{ height: 390px; overflow: auto; border: 1px solid #293532;
                     border-radius: 9px; }}
      table {{ width: 100%; min-width: 1180px; border-collapse: collapse; }}
      th {{ position: sticky; top: 0; z-index: 2; background: #1b1e26;
            color: #aeb7bd; text-align: left; font-weight: 600; }}
      th, td {{ padding: 10px 11px; border-right: 1px solid #30343d;
                border-bottom: 1px solid #30343d; white-space: nowrap; }}
      th:nth-child(3), td:nth-child(3) {{ width: 220px; max-width: 220px; }}
      td:nth-child(8) {{ white-space: normal; min-width: 310px; }}
      .mint-cell {{ display: flex; align-items: center; gap: 8px; }}
      .mint-cell code {{ overflow: hidden; text-overflow: ellipsis; color: #eef8f3;
                         background: #20242c; padding: 5px 7px; border-radius: 6px; }}
      .copy-ca {{ flex: 0 0 auto; width: 28px; height: 28px; border-radius: 6px;
                  border: 1px solid #44504e; background: #20242c; color: #c9d6d1;
                  cursor: pointer; font-size: 17px; line-height: 1; }}
      .copy-ca:hover {{ color: #50e3ad; border-color: #50e3ad; }}
      .copy-ca.copied {{ color: #50e3ad; }}
      .copy-ca.copy-failed {{ color: #ff7a83; border-color: #ff7a83; }}
    </style>
    <div class="table-wrap">
      <table><thead><tr>{header}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>
    </div>
    <script>{clipboard_script()}</script>
    """
    st.html(document, unsafe_allow_javascript=True)


def position_trade_groups(
    events: list[dict[str, Any]],
    current_positions: dict[str, dict[str, Any]],
    token_metadata: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Group one BUY and all following SELL events into a distinct trade round."""
    open_groups: dict[str, dict[str, Any]] = {}
    latest_open_by_mint: dict[str, str] = {}
    round_counts: dict[str, int] = {}
    groups: list[dict[str, Any]] = []
    reason_labels = {
        "TAKE_PROFIT_50": "익절 기준 +50% · 절반 청산",
        "TAKE_PROFIT_100": "익절 기준 +100% · 남은 물량 절반 청산",
        "TAKE_PROFIT_30_SELL_80": "익절 기준 +30% · 보유 물량 80% 청산",
        "POST_TP_TRAILING_STOP_50": "익절 후 고점 대비 -50% · 잔량 청산",
        "STOP_LOSS_15": "손절 기준 -15% · 전량 청산",
        "LIVE_TAKE_PROFIT_50": "실거래 익절 기준 +50% · 절반 청산",
        "LIVE_TAKE_PROFIT_100": "실거래 익절 기준 +100% · 남은 물량 절반 청산",
        "LIVE_STOP_LOSS_15": "실거래 손절 기준 -15% · 전량 청산",
        "MANUAL_CLOSE": "사용자 수동 종료 · 전량 청산",
    }
    for event in events:
        if not isinstance(event, dict):
            continue
        mint = str(event.get("mint", ""))
        if event.get("type") == "BUY":
            round_counts[mint] = round_counts.get(mint, 0) + 1
            position_id = str(event.get("position_id") or f"legacy:{mint}:{round_counts[mint]}")
            initial_amount = int(event.get("token_amount_raw", 0))
            group = {
                "mint": mint,
                "position_id": position_id,
                "round": int(event.get("round_index", round_counts[mint]) or round_counts[mint]),
                "buy": event,
                "initial_amount": initial_amount,
                "remaining_amount": initial_amount,
                "exits": [],
                "realized_pnl_lamports": 0,
                "realized_cost_lamports": 0,
                "proceeds_lamports": 0,
            }
            groups.append(group)
            open_groups[position_id] = group
            latest_open_by_mint[mint] = position_id
            continue
        if event.get("type") != "SELL":
            continue
        position_id = str(event.get("position_id") or latest_open_by_mint.get(mint, ""))
        group = open_groups.get(position_id)
        if not group:
            continue
        buy = group["buy"]
        sold = int(event.get("token_amount_raw", 0))
        proceeds = int(event.get("proceeds_lamports", 0))
        pnl = int(event.get("realized_pnl_lamports", 0))
        released_cost = proceeds - pnl
        roi = pnl / released_cost * 100 if released_cost > 0 else 0.0
        initial_amount = int(group["initial_amount"])
        decimals = int(buy.get("token_decimals", 0))
        sold_quantity = sold / (10 ** decimals)
        sold_percent = sold / initial_amount * 100 if initial_amount else 0.0
        group["exits"].append({
            "청산 시각": format_kst(event.get("at")),
            "보유 시간": format_duration(buy.get("at"), event.get("at")),
            "청산 구분": reason_labels.get(str(event.get("reason", "")), str(event.get("reason", "청산"))),
            "청산 비율": sold_percent,
            "청산 수량": sold_quantity,
            "진입 원가(SOL)": sol(released_cost),
            "회수 금액(SOL)": sol(proceeds),
            "실현 손익(SOL)": sol(pnl),
            "실현 수익률": roi,
        })
        group["remaining_amount"] = max(0, int(group["remaining_amount"]) - sold)
        group["realized_pnl_lamports"] += pnl
        group["realized_cost_lamports"] += max(0, released_cost)
        group["proceeds_lamports"] += proceeds
        if group["remaining_amount"] <= 0:
            open_groups.pop(position_id, None)
            if latest_open_by_mint.get(mint) == position_id:
                latest_open_by_mint.pop(mint, None)

    for group in groups:
        mint = group["mint"]
        if open_groups.get(group["position_id"]) is group:
            position = current_positions.get(mint, {})
            if (
                isinstance(position, dict)
                and str(position.get("position_id", group["position_id"]))
                == group["position_id"]
            ):
                group["remaining_amount"] = int(
                    position.get("token_amount_raw", group["remaining_amount"])
                )
                group["position"] = position
        initial_amount = int(group["initial_amount"])
        remaining_amount = int(group["remaining_amount"])
        group["remaining_percent"] = (
            remaining_amount / initial_amount * 100 if initial_amount else 0.0
        )
        group["status"] = (
            "종료"
            if remaining_amount <= 0
            else "부분 청산"
            if group["exits"]
            else "보유 중"
        )
        realized_cost = int(group["realized_cost_lamports"])
        group["realized_roi"] = (
            int(group["realized_pnl_lamports"]) / realized_cost * 100
            if realized_cost > 0
            else 0.0
        )
        group["token"] = (
            (token_metadata or {}).get(mint, {}).get("name") or short_mint(mint)
        )
    return list(reversed(groups))


def render_position_trade_groups(groups: list[dict[str, Any]]) -> None:
    """Render compact trade-round summaries with expandable exit details."""
    with st.container(height=520, border=True):
        for group in groups:
            buy = group["buy"]
            status = str(group["status"])
            status_icon = {"보유 중": "🟢", "부분 청산": "🟡", "종료": "⚪"}.get(status, "•")
            label = (
                f"{status_icon} {group['token']} · {group['round']}차 거래 · {status} · "
                f"잔여 {group['remaining_percent']:.2f}% · "
                f"실현 {sol(int(group['realized_pnl_lamports'])):+.6f} SOL"
            )
            with st.expander(label, expanded=False):
                st.caption(
                    f"포지션 ID · {group['position_id']}  |  토큰 CA · {group['mint']}"
                )
                summary = st.columns(4)
                summary[0].metric("진입 시각", format_kst(buy.get("at")))
                summary[1].metric("최초 진입액", f"{sol(int(buy.get('cost_lamports', 0))):.6f} SOL")
                summary[2].metric("남은 물량", f"{group['remaining_percent']:.2f}%")
                summary[3].metric("누적 실현 수익률", f"{group['realized_roi']:+.2f}%")

                position = group.get("position", {})
                if isinstance(position, dict) and group["remaining_amount"] > 0:
                    current = st.columns(3)
                    decimals = int(buy.get("token_decimals", 0))
                    current[0].metric(
                        "잔여 수량",
                        f"{group['remaining_amount'] / (10 ** decimals):.8f}",
                    )
                    current[1].metric(
                        "현재 미실현 수익률",
                        f"{float(position.get('unrealized_return_percent', 0) or 0):+.2f}%",
                    )
                    current[2].metric("상태", status)

                st.markdown(
                    f"**소스 지갑:** {buy.get('source_wallet') or '기록 없음'}  \n"
                    f"**안전 점수:** {buy.get('safety_score', '기록 없음')}  \n"
                    f"**진입 사유:** {buy.get('entry_reason') or '기존 기록'}  \n"
                    f"**원본 서명:** {buy.get('source_signature') or '기록 없음'}  \n"
                    f"**신호→진입:** {int(buy.get('entry_latency_ms', 0) or 0)}ms  |  "
                    f"**Price impact:** {float(buy.get('entry_price_impact_pct', 0) or 0):.4f}%  |  "
                    f"**고래 대비 격차:** {float(buy.get('copy_price_gap_pct', 0) or 0):+.2f}%  |  "
                    f"**Slippage:** {int(buy.get('expected_slippage_bps', 0) or 0)}bps"
                )
                if group["exits"]:
                    st.dataframe(
                        group["exits"],
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "청산 비율": st.column_config.NumberColumn(format="%.2f%%"),
                            "청산 수량": st.column_config.NumberColumn(format="%.8f"),
                            "진입 원가(SOL)": st.column_config.NumberColumn(format="%.8f"),
                            "회수 금액(SOL)": st.column_config.NumberColumn(format="%.8f"),
                            "실현 손익(SOL)": st.column_config.NumberColumn(format="%+.8f"),
                            "실현 수익률": st.column_config.NumberColumn(format="%+.2f%%"),
                        },
                    )
                else:
                    st.info("아직 청산 이력이 없으며 최초 진입 물량을 보유 중입니다.")


@st.fragment(run_every=2.0)
def live_dashboard() -> None:
    wallets_doc = load_json(WALLETS_PATH, {"wallets": []})
    performance = load_json(PERFORMANCE_PATH, {"wallets": {}}, cache_key="performance")
    ledger = load_json(
        LEDGER_PATH,
        {"cash_lamports": int(DEFAULT_INITIAL_SOL * LAMPORTS_PER_SOL),
         "positions": {}, "events": []},
    )
    wallets = wallet_rows(wallets_doc)
    positions = ledger.get("positions", {}) if isinstance(ledger, dict) else {}
    events = ledger.get("events", []) if isinstance(ledger, dict) else []
    held_mints = [str(position.get("mint")) for position in positions.values() if isinstance(position, dict) and position.get("mint")]
    observed_mints: list[str] = []
    performance_records = performance.get("wallets", {}) if isinstance(performance, dict) else {}
    for wallet in wallets:
        record = performance_records.get(str(wallet.get("address")), {})
        samples = record.get("samples", []) if isinstance(record, dict) else []
        for sample in reversed(samples if isinstance(samples, list) else []):
            if (
                isinstance(sample, dict)
                and float(sample.get("return_percent", 0) or 0) > 0
                and sample.get("mint")
            ):
                observed_mints.append(str(sample["mint"]))
    history_mints = list(dict.fromkeys(
        str(event.get("mint")) for event in reversed(events)
        if isinstance(event, dict) and event.get("type") == "SELL" and event.get("mint")
    ))[:20]
    metadata_mints = list(dict.fromkeys(
        [*held_mints, *observed_mints, *history_mints]
    ))[:120]
    dex_prices = live_dexscreener_prices(metadata_mints) if metadata_mints else {}
    missing_symbols = tuple(
        mint for mint in metadata_mints
        if not (dex_prices.get(mint) or {}).get("name")
    )
    for mint, symbol in cached_helius_symbols(missing_symbols).items():
        dex_prices.setdefault(mint, {"price_sol": 0.0})["name"] = symbol
    live_positions = position_rows(positions, dex_prices)
    stats = load_json(STATS_PATH, {})
    reset_at = str(stats.get("reset_at", "")) if isinstance(stats, dict) else ""
    rejected = [item for item in events if isinstance(item, dict) and item.get("type") == "SIGNAL_REJECTED" and str(item.get("at", "")) > reset_at]
    low_liquidity_blocks = sum("liquidity" in str(item.get("reason", "")).lower() for item in rejected)
    performance_wallets = performance.get("wallets", {}) if isinstance(performance, dict) else {}
    evicted_wallets = sum(bool(row.get("evicted")) for row in performance_wallets.values() if isinstance(row, dict))
    cash_lamports = int(ledger.get("cash_lamports", DEFAULT_INITIAL_SOL * LAMPORTS_PER_SOL))
    invested_lamports = int(sum(row["현재 가치(SOL)"] for row in live_positions) * LAMPORTS_PER_SOL)
    realized_pnl = sum(
        int(item.get("realized_pnl_lamports", 0))
        for item in events if isinstance(item, dict) and item.get("type") == "SELL"
    )
    start_lamports = int(DEFAULT_INITIAL_SOL * LAMPORTS_PER_SOL)
    book_value = cash_lamports + invested_lamports
    total_return = (book_value - start_lamports) / start_lamports * 100
    running = service_is_fresh()

    status_class = "" if running else " status-off"
    status_text = "수집 서비스 가동 중" if running else "서비스 로그 확인 필요"
    st.markdown(
        f"""
        <header class="hero">
          <div class="eyebrow">SOLANA AI BOT · LIVE OPS</div>
          <h1>실시간 운영 대시보드</h1>
          <div class="muted hero-status">
            <span class="status-dot{status_class}"></span>
            <span>{html.escape(status_text)}</span>
            <span aria-hidden="true">·</span>
            <span>2초 자동 갱신</span>
          </div>
        </header>
        """,
        unsafe_allow_html=True,
    )
    render_operations_overview(
        total_return=total_return,
        realized_pnl=realized_pnl,
        book_value=book_value,
        cash_lamports=cash_lamports,
        invested_lamports=invested_lamports,
        wallet_count=len(wallets),
        position_count=len(positions),
        running=running,
    )

    degraded_positions = [
        position for position in positions.values()
        if isinstance(position, dict)
        and position.get("risk_state") in {"DEGRADED", "NO_ROUTE", "EXIT_PENDING"}
    ]
    if degraded_positions:
        labels = ", ".join(
            f"{short_mint(str(position.get('mint', '')))} "
            f"({position.get('risk_state')}, 실패 {int(position.get('consecutive_quote_failures', 0))}회)"
            for position in degraded_positions
        )
        st.warning(f"Jupiter 견적 또는 청산 확인이 필요한 포지션: {labels}")

    st.subheader("현재 보유 포지션 현황")
    st.html(
        manual_close_progress_document(),
        unsafe_allow_javascript=True,
    )
    manual_close_message = st.session_state.pop(
        "manual_close_message",
        None,
    )
    if manual_close_message:
        st.html(
            clear_manual_close_progress_document(),
            unsafe_allow_javascript=True,
        )
        st.success(manual_close_message)
    if live_positions:
        render_position_rows(live_positions)
    else:
        st.info("현재 보유 중인 포지션이 없습니다.")

    if st.button("통계 데이터 초기화", type="secondary"):
        atomic_json(STATS_PATH, {"reset_at": datetime.now(timezone.utc).isoformat()})
        st.rerun()

    safety = st.columns(2)
    safety[0].metric("저유동성 차단", f"{low_liquidity_blocks}건")
    safety[1].metric("성과 기반 지갑 퇴출", f"{evicted_wallets}개")

    history_tab, position_history_tab = st.tabs(
        ["전체 매매 및 차단 이력", "포지션 진입·청산 히스토리"]
    )
    with history_tab:
        rows = trade_rows(events)
        if rows:
            render_trade_history_table(rows)
        else:
            st.info("아직 매매 또는 차단 이력이 없습니다.")
    with position_history_tab:
        position_groups = position_trade_groups(events, positions, dex_prices)
        if position_groups:
            st.caption(
                "한 번의 진입과 이후 부분·최종 청산을 거래 회차별로 묶었습니다. "
                "같은 토큰을 다시 매수하면 새로운 회차로 분리됩니다."
            )
            render_position_trade_groups(position_groups)
        else:
            st.info("아직 포지션 진입 이력이 없습니다.")

    st.caption(
        "총자산과 포지션 수익률은 리스크 루프가 저장한 최신 Jupiter 청산 견적 기준입니다. "
        "대시보드는 2초마다 같은 평가 데이터를 다시 읽어 상단 현황판과 포지션 히스토리를 함께 갱신합니다."
    )

    st.subheader("스마트 머니 감시 현황")
    if wallets:
        display = [wallet_pipeline_metrics(row, performance) for row in wallets]
        st.caption("지갑 주소 행을 클릭하면 해당 행 바로 아래에서 승리 토큰 이력이 열립니다.")
        with st.container(height=420, border=True):
            for row in display:
                address = row["지갑 주소"]
                wins = winning_token_rows(address, performance, dex_prices)
                label = (
                    f"{address}  │  승률 {row['승률 (관찰)']:.2f}%  │  "
                    f"ROI {row['ROI (관찰)']:+.2f}%  │  "
                    f"평가 {row['지갑 거래 (평가완료/대기)']}  │  "
                    f"가상매수 {row['봇 가상매수']}  │  차단 {row['안전 차단']}"
                )
                with st.expander(label, expanded=False):
                    if wins:
                        st.caption(f"승리 토큰 {len(wins)}개 · 지갑 {address}")
                        st.dataframe(
                            wins,
                            width="stretch",
                            hide_index=True,
                            column_config={
                                "토큰 CA": st.column_config.TextColumn(width="large"),
                                "관찰 수익률": st.column_config.NumberColumn(format="%+.2f%%"),
                            },
                        )
                    else:
                        st.caption("아직 수익으로 평가 완료된 토큰이 없습니다.")
    else:
        st.info("아직 data/wallets.json에 수집된 지갑이 없습니다.")


require_dashboard_login()
live_dashboard()
