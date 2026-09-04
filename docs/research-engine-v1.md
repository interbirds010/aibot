# Research Engine V1 설계 기준

## 목적과 범위

Research Engine V1은 새 매매전략이나 별도 후보 원장을 만드는 작업이 아니다. 현재 `signal_observations.json`의 observation row를 canonical research event로 확장해, 진입 후보의 판정과 이후 성과를 같은 식별자로 분석할 수 있게 하는 기반이다.

연구 기록 실패는 거래 프로세스를 중단시키지 않도록 격리하되, 연구 데이터 오류나 누락을 근거로 거래를 허용해서는 안 된다. 기존 analyzer의 fail-closed 판정, live 활성화 조건, 포지션 크기, 손절·익절, Jupiter/Jito 안전장치는 변경하지 않는다.

## 현재 signal lifecycle

### Route A: smart money

1. `monitor.py`가 Helius WebSocket 또는 표준 RPC fallback에서 감시 지갑의 DEX 거래를 발견한다.
2. 지갑별 토큰 증가량과 SOL 지출을 계산하고 wallet-performance 관찰을 별도 경로로 기록한다.
3. 단건 1.5 SOL 또는 동일 지갑·토큰의 3분 누적 5 SOL 조건을 검사한다. 탈락 후보도 observation mode에서는 rejection reason과 함께 관찰 경로로 보낸다.
4. `process_paper_signal`이 observation row를 `DISCOVERED`로 먼저 생성한 뒤 연속 손실 제한, 토큰 cooldown, analyzer 검사를 수행한다.
5. analyzer가 Helius와 RugCheck로 mint authority, 개발자 보유량, LP 잠금률, 유동성과 안전점수를 검증한다.
6. Jupiter 매수 견적과 전량 매도 사전 견적이 실행 가능하고 가격 영향 제한을 통과하면 observation decision과 진입 기준값을 확정한다.
7. 승인된 신호는 설정에 따라 observation-only로 남거나 paper position으로 승격된다. 탈락 신호는 paper position을 만들지 않지만 실행 가능한 견적이 있으면 미래 성과를 계속 추적한다.

### Route B: momentum

1. DexScreener 후보에서 5분 거래량, 매수·매도 수, 순매수, 매수 우위, 유동성, pair age를 수집한다.
2. 기준 통과 후보와 near-miss를 분리한다. near-miss는 구체적인 momentum rejection reason을 갖는 shadow 후보가 된다.
3. 승인 후보는 미등록 대형 매수 지갑 수와 재조회한 momentum 강도를 확인한다. 지갑 수 부족이나 momentum 약화도 shadow 후보로 전환한다.
4. 이후 analyzer, Jupiter 견적, observation, 선택적 paper execution 단계는 Route A와 같은 공통 경로를 사용한다. Route B의 추가 안전 하한도 별도로 적용한다.

### Observation, paper execution, exit

- observation tracker는 실행 가능한 진입 기준으로 Jupiter 역방향 매도 견적을 조회해 1분, 3분, 5분, 15분, 30분, 60분 수익률을 기록한다.
- 신규 V1 표본은 60분 결과 뒤 observation을 `COMPLETE`로 만들고 장기 보관용 `shadow_trades.json`에 고정 보유 및 표본 기반 청산 결과를 저장한다. 기존 v4 완료 표본은 `legacy_15m` 프로필로 보존한다.
- paper buy는 `paper_trades.json`에 position과 `BUY` event를 원자적으로 기록한다.
- risk manager는 열린 position을 주기적으로 재평가하고 손절·익절·수동 종료 시 `SELL` event에 매도대금, 실현손익, 실현 ROI와 실행 진단값을 기록한다.
- observation과 paper position은 `observation_id`와 `paper_experiment_position_id`로 연결되며, 전량 종료 시 paper 상태가 `CLOSED`로 바뀐다.

## 현재 저장 데이터

| 요구 데이터 | 현재 상태 | 기존 위치 또는 해석 | V1 처리 |
|---|---|---|---|
| signal ID | 있음 | `observation_id = source_signature:source_wallet:mint` | 그대로 canonical ID로 사용 |
| mint | 있음 | observation, paper position/event | 재사용 |
| signal source/type | 부분 | `source_wallet`, `source_signature`, `entry_reason`과 route 관례로 간접 추론 | 명시적인 `signal_type` snapshot 추가 |
| route A/B | 있음 | `route_type` | 재사용 |
| wallet | 있음 | `source_wallet` | 재사용 |
| wallet score/performance | 없음 | wallet-performance 원장에는 있으나 신호 시점 snapshot이 아님 | V1에서는 저장하지 않고 unknown으로 취급 |
| signal timestamp | 있음 | `signal_detected_at`; observer 시작은 `started_at`/`started_at_epoch` | 원본 시각 보존 |
| entry candidate price | 부분 | `entry_cost_lamports / token_amount_raw`로 계산 가능 | 중복 가격 필드보다 기존 원가·수량을 기준값으로 사용 |
| whale reference price | 부분 | paper position의 `whale_reference_price` | V1 observation에는 중복 저장하지 않음 |
| whale 단건 매수량 | 있음 | `discovery_metadata.whale_paid_lamports` | 재사용 |
| whale 3분 누적량 | 없음 | gate 내부 메모리에만 존재 | V1에서는 저장하지 않고 unknown으로 취급 |
| liquidity | 있음 | `safety_metrics.liquidity_usd`, `momentum_metrics.liquidity_usd` | 출처별 의미를 유지 |
| safety score | 있음 | `safety_score` | 재사용 |
| LP lock | 있음 | `safety_metrics.lp_locked`, `lp_locked_percent` | 재사용 |
| developer holding | 있음 | `safety_metrics.developer_supply_percent`, `developer_below_ten_percent` | 재사용 |
| mint authority | 있음 | `safety_metrics.mint_authority_renounced` | 재사용 |
| volume/buy/sell | Route B만 있음 | `momentum_metrics.volume_m5_usd`, `buys_m5`, `sells_m5`, `net_buys_m5`, `buy_sell_ratio_m5` | Route A는 `None` 허용 |
| token/pair age | Route B pair age만 있음 | `momentum_metrics.pair_age_seconds` | token age를 억지로 추정하지 않음 |
| rejection reason | 있음 | `decision_reasons`, analyzer 사유, quote 상태 | 구조화 목록 유지 |
| 실제 진입 여부 | 부분 | paper 상태 `OPENED`/`CLOSED` 및 paper `BUY` event로 확인 | canonical `decision`을 명확히 파생·저장 |
| exit/PnL | paper만 있음 | `SELL` event의 reason, proceeds, realized PnL/ROI | `observation_id`로 join; 중복 원장화하지 않음 |
| horizon return | 부분 | 1m/5m/15m `samples` | 3m/30m/60m 추가 |
| MFE/MAE | 근사값만 있음 | 1m/5m/15m 표본의 `max_return_percent`/`min_return_percent` | horizon 표본 기반 근사임을 명시 |

## Canonical research event 설계

별도 research-event 원장을 만들지 않고 observation schema를 확장한다. 한 observation row는 발견부터 outcome 완료까지 갱신되는 단일 canonical research event다. 기존 필드를 보존하고 마이그레이션은 누락 필드만 멱등 보완한다.

권장 의미는 다음과 같다.

- `strategy`: 기존 `strategy_version`과 `strategy_variants`를 재사용한다.
- `signal_type`: `SMART_MONEY` 또는 `MOMENTUM`을 명시한다. 문자열 관례만으로 계속 추론하지 않는다.
- `research_decision`: 상호 배타적인 `ENTERED`, `REJECTED`, `SHADOW`를 사용한다.
- `REJECTED`: 진입 gate를 통과하지 못했지만 가능한 경우 미래 outcome을 추적한다.
- `ENTERED`: paper `BUY`가 실제로 원장에 반영되고 position ID가 연결된 경우에만 확정한다.
- `SHADOW`: 진입 조건은 평가했으나 observation-only 설정, 실험 capacity 또는 연구 목적 때문에 paper position을 만들지 않은 후보다.
- `decision_reasons`: rejection, shadow 전환, 처리 실패 사유를 제한된 문자열 목록으로 유지한다.
- 기존 `safety_metrics`, `momentum_metrics`, `discovery_metadata`가 feature snapshot 역할을 한다. 새 중첩 객체로 같은 값을 복제하지 않으며, 새로운 고비용 조회로 빈 값을 억지로 채우지 않는다.
- 기존 `samples`가 outcome 역할을 한다. horizon, 목표·실제 표본 시각, 지연, proceeds, return과 오류를 보존하고 `sample_attempts`에 재시도 횟수를 둔다.

기존 `decision_status`와 `paper_experiment_status`만으로도 대부분의 상태를 추론할 수 있지만 완전히 상호 배타적이지 않다. 예를 들어 `APPROVED + ELIGIBLE`은 observation-only인지 아직 paper 진입 전인지 모호하고, 실제 진입 후보도 별도의 shadow 거래로 함께 보관된다. V1의 canonical `decision`은 paper 원장 반영이 끝난 뒤 `ENTERED`로 확정하고 나머지 상태를 명시적으로 구분해야 한다.

## Outcome 추적과 호출 제한

최소 horizon은 신호 기준 1분, 3분, 5분, 15분, 30분, 60분이다. 가격 소스는 기존과 동일하게 보유 토큰 수량을 WSOL로 매도하는 Jupiter 실행 가능 견적의 `outAmount`를 사용한다. 이는 표시가격이 아니라 실제 청산 가능성에 가까운 reference return이다.

기존 observer의 bounded scheduler를 확장한다.

- observation마다 한 번의 loop에서 due horizon 하나만 처리하되, backlog에서는 현재 시각에 가장 가까운 최신 horizon을 먼저 보존한다.
- 전체 batch 크기와 active observation 상한을 유지한다.
- Jupiter 공유 rate limiter, 429 backoff, 요청 timeout과 최대 재시도 횟수를 그대로 사용한다.
- `NO_ROUTE`는 해당 horizon의 결과로 명시하고, 일시 오류는 제한된 횟수만 재시도한다.
- 목표 시각보다 60초를 초과해 밀린 horizon은 현재 견적을 과거 시점 값으로 오인하지 않도록 외부 호출 없이 `HORIZON_MISSED` 결측으로 기록한다.
- target horizon 시각과 실제 `sampled_at`을 함께 저장해 backlog 지연을 분석할 수 있게 한다.
- paper risk loop의 고빈도 quote를 연구용 전체 후보에 복제하지 않는다.
- 추가 horizon 때문에 모든 후보를 동시에 조회하지 않고 기존 queue 방식으로 분산한다.

### MFE/MAE 제한

V1에서 계산 가능한 값은 1/3/5/15/30/60분의 이산적인 horizon return 중 최댓값과 최솟값이다. 이는 tick-level 또는 연속 시세 기반의 진정한 MFE/MAE가 아니라 `mfe_percent`와 `mae_percent`에 저장하는 표본 기반 근사치다. `excursion_basis=scheduled_jupiter_executable_quotes`가 이 의미를 명시한다.

Excursion 계산에는 진입 시점의 0% 기준을 포함하므로 MFE는 음수가 되지 않고 MAE는 양수가 되지 않는다. 유효 가격 표본이 하나도 없으면 두 값 모두 `None`이다.

Jupiter를 고빈도로 조회해 진정한 excursion을 재현하려 하면 API 제한과 1GB VPS 자원을 훼손한다. 따라서 V1 지표와 문서·대시보드에서는 반드시 "horizon 표본 기반 근사"로 표시하고, 표본 사이의 순간 고점·저점은 측정하지 못한다는 한계를 유지한다. 표본 누락이나 `NO_ROUTE`는 0%로 대체하지 않고 계산 대상에서 제외한다.

## Research metrics

`observation_analysis.json.research_metrics`는 observation 원본 이벤트를 mint로 중복 제거하지 않고 집계한다. 전체 signal, ENTERED, REJECTED, SHADOW, 완료 outcome, 만료 수와 각 horizon의 win rate, 평균·중앙 return, 평균 승리·손실, expectancy, profit factor를 제공한다. 그룹은 `strategy_version + route_type + signal_type` 조합으로 나눈다.

- win은 return이 0보다 큰 표본이다.
- expectancy는 0을 포함한 모든 유한 return의 산술평균이다.
- profit factor는 양수 return 합계를 음수 return 합계의 절댓값으로 나눈 값이다. 손실이 없거나 데이터가 비면 JSON에 무한대를 쓰지 않고 `None`, 손실만 있으면 `0.0`이다.
- MFE/MAE 집계는 각 이벤트의 표본 기반 `mfe_percent`/`mae_percent` 평균이다.
- rejection reason별 표는 60분 outcome의 signal 수, 완료 수, 평균 return과 positive rate를 제공한다. 60분 결과가 없는 신호는 signal 수에는 포함하지만 성과 계산에서는 제외한다.
- 표본 수가 적다는 이유로 GOOD/BAD 판정을 자동 생성하지 않으며 기존 train/holdout 결과도 review-only로 유지한다.

## Trading 보호 원칙

- research 기록은 기존 analyzer와 거래 gate 뒤에 우회 경로를 만들지 않는다.
- feature가 `None`이거나 research 저장이 실패해도 거래 승인으로 해석하지 않는다.
- research write 실패는 가능한 범위에서 로깅하고 격리하며 monitor와 risk loop 전체를 종료시키지 않는다.
- live mode, 개인키 처리, Jupiter/Jito 제출·확인, price impact, slippage, quote age, rate-limit 검증을 변경하지 않는다.
- 기존 paper sizing, position capacity, stop loss, take profit, cooldown과 원가 불변식을 변경하지 않는다.
- paper 거래의 사실 원본은 계속 `paper_trades.json`이며, observation은 거래 상태의 독립 원본이 되지 않는다.
- shared JSON 갱신은 항상 `state_store`의 원자 API와 파일 락을 사용하고 version을 증가시킨다.
- 기존 관찰 표본과 과거 paper event를 파괴하거나 재분류하지 않는다. 새 필드는 nullable·멱등 마이그레이션으로 보완한다.
