# Research Engine V2 — Alpha Discovery

## 목적과 안전 경계

Alpha Discovery는 `signal_observations.json`의 과거 signal snapshot과 이후
Jupiter outcome을 읽어 조건별 연구 통계를 만드는 분석 계층이다. 결과의
`PROMISING`은 추가 검토가 가능하다는 연구 상태일 뿐 수익 보장이나 trading
activation을 의미하지 않는다. 이 모듈은 threshold, position sizing, SL/TP,
paper/live execution 또는 signal generation을 변경하지 않는다.

분석기는 observation 원장을 `read_json`으로만 읽는다. 현재 wallet 성과를
과거 signal에 join하거나 누락 feature를 현재 값으로 보충하지 않는다. 결과는
별도 `data/alpha_discovery.json`에 원자 저장한다.

## Feature allowlist

Feature는 observation row에 실제 저장된 pre-outcome snapshot만 사용한다.
`samples`, return, MFE/MAE, 완료 상태, paper 결과, candidate early failure,
현재 wallet 성과는 feature로 사용하지 않는다.

SMART_MONEY:

- `discovery_metadata.whale_paid_lamports`에서 변환한 `whale_paid_sol`
- `safety_score`
- `safety_metrics.developer_supply_percent`
- `safety_metrics.lp_locked_percent`
- `safety_metrics.liquidity_usd`
- `entry_price_impact_pct`, `exit_price_impact_pct`, `entry_latency_ms`
- row에 실제 snapshot이 존재할 때만 `copy_price_gap_pct`

MOMENTUM:

- `dex_momentum_score`
- `momentum_metrics.volume_m5_usd`
- `momentum_metrics.buys_m5`, `sells_m5`, `net_buys_m5`
- `momentum_metrics.buy_sell_ratio_m5`
- `momentum_metrics.liquidity_usd`
- `momentum_metrics.pair_age_seconds`
- `momentum_metrics.unknown_whale_count`
- `safety_score`, `safety_metrics.lp_locked_percent`
- `safety_metrics.developer_supply_percent`
- `entry_price_impact_pct`, `exit_price_impact_pct`, `entry_latency_ms`

SMART_MONEY liquidity와 MOMENTUM liquidity는 출처를 섞지 않는다. 현재
observation에는 historical wallet-performance snapshot이 없으므로 source wallet
성과 feature는 제공하지 않는다.

## 사전 정의 bucket

모든 경계는 lower-inclusive, upper-exclusive다. 예를 들어 `1.0`은
`1_to_below_1.5`에 속한다. `None`, 비유한 수, 허용 범위 밖의 음수 값은
`UNKNOWN`이나 0으로 대체하지 않고 feature-missing으로 제외한다.

| Feature | 고정 cut points |
|---|---|
| whale_paid_sol | 1, 1.5, 2, 3, 5 SOL |
| safety_score | 55, 70, 85, 95 |
| liquidity_usd | 7.5k, 10k, 20k, 50k, 100k |
| entry/exit_price_impact_pct | 0.25, 0.5, 1.0, 1.5 |
| entry_latency_ms | 1s, 3s, 10s, 30s |
| developer_supply_percent | 2, 5, 10, 20 |
| lp_locked_percent | 40, 80, 90 |
| dex_momentum_score | 55, 70, 85, 90, 95, 100 |
| volume_m5_usd | 10k, 15k, 25k, 50k, 100k |
| buys/sells/net_buys_m5 | 5, 10, 15, 25, 50 |
| buy_sell_ratio_m5 | 1.2, 1.5, 1.8, 2.5, 4 |
| pair_age_seconds | 5m, 15m, 30m, 1h, 3h |
| unknown_whale_count | 0, 1, 2, 3, 4+ |
| copy_price_gap_pct | -5, -2, -1, 0, 1, 2, 5 |

정확한 label과 cut은 `BUCKET_VERSION=alpha_v2_fixed_1`의 코드 상수와 출력
`configuration.bucket_definitions`가 단일 기준이다. 결과를 보고 같은 분석
run에서 cut을 재조정하지 않는다.

## Outcome과 coverage

분석 horizon은 5m, 15m, 30m, 60m이고 primary outcome은 60m다. outcome은
실제 체결 PnL이 아니라 보유 수량을 역방향으로 매도할 때의 Jupiter executable
quote return이다. route 부재나 API 실패는 0% return으로 대체하지 않는다.

각 cohort는 다음 두 coverage를 제공한다.

- raw coverage = `sampled_count / signal_count`
- trackable coverage = `sampled_count / trackable_count`

Entry가 untrackable인 row에 과거 sample이 모순되게 남아 있으면 그 return은 성과와
coverage 분자에서 제외하고 `inconsistent_untrackable_sample_count`로 보고한다.

MFE/MAE는 연속 가격의 진정한 excursion이 아니라 저장된 horizon 표본과 0%
entry baseline으로 계산한 근사치다. 각 horizon 분석은 해당 시각 이전 표본만
사용하므로 5m excursion에 15m 이후 정보를 섞지 않는다.

## Event와 unique mint

원본 event는 삭제하지 않고 event-level 결과에 모두 포함한다. 반복 signal이
특정 mint를 과대가중하는 영향을 보기 위해 별도의 `first_signal_per_mint` view를
함께 제공한다. unique view는 같은 signal family와 mint에서 timestamp가 가장
이른 event만 선택하고 이후 event를 제외한다.
동일 mint의 첫 timestamp와 identity가 완전히 같지만 row 내용이 충돌하면 outcome으로
대표 row를 고르지 않고 해당 mint를 unique view에서 제외해 별도 count로 보고한다.

Candidate status와 ranking은 event-level이 아니라 unique-mint view의 60m
성과를 기준으로 한다. 이 선택은 반복 mint dependency를 줄이지만 첫 signal만
대표로 쓰는 데 따른 편향 가능성도 있으므로 두 view를 함께 해석해야 한다.

## 시간순 train/holdout

각 signal family와 view를 signal timestamp 순서로 먼저 정렬하고 80% train,
20% holdout으로 한 번만 나눈다. 이후 모든 single-feature bucket과 interaction
cell이 같은 split assignment를 재사용한다. random split은 사용하지 않는다.
동일 timestamp는 `observation_id`, signal identity digest 순서로 결정적으로
처리하며 outcome과 완료 후 필드는 정렬에 사용하지 않는다.

Holdout을 반복 확인하며 cut이나 hypothesis를 바꾸면 더 이상 독립 검증이
아니다. 새 조건은 새 미래 기간에서 다시 검증해야 한다.

## Research status

`first_signal_per_mint`의 primary horizon에서 다음 중 하나를 부여한다.

`INSUFFICIENT_DATA`:

- 전체 sampled < 50, 또는
- holdout sampled < 15, 또는
- trackable coverage < 80% 또는 trackable outcome 없음

`PROMISING`:

- 위 표본·coverage 조건을 충족하고
- train/holdout expectancy가 모두 양수이며
- train/holdout profit factor가 모두 1보다 크고
- holdout expectancy가 train expectancy의 25% 이상

충분한 데이터가 있지만 위 성과·안정성 조건을 통과하지 못하면 `UNSTABLE`이다.
손실이 전혀 없는 양수 표본군의 JSON profit factor는 무한대 대신 `None`이지만,
판정에서는 수학적으로 1보다 큰 것으로 취급한다. JSON의
`profit_factor_above_one=true`와 `POSITIVE_WITH_NO_LOSSES` 해석값이 이를 명시한다.

## 제한된 interaction과 ranking

SMART_MONEY interaction:

- whale_paid_sol × liquidity_usd
- whale_paid_sol × entry_latency_ms
- liquidity_usd × safety_score

MOMENTUM interaction:

- volume_m5_usd × buy_sell_ratio_m5
- volume_m5_usd × pair_age_seconds
- buy_sell_ratio_m5 × unknown_whale_count
- liquidity_usd × pair_age_seconds

관측된 cell만 출력하고 무차별 feature 조합은 생성하지 않는다. ranking은
holdout 양의 expectancy, holdout profit factor, sample size, trackable coverage,
train/holdout retention 순으로 정렬한다. 단순 최고 expectancy 3건을 고르는
방식은 사용하지 않으며 모든 `PROMISING` 후보를 최대 100개까지 보존한다.

사전 정의된 hypothesis라도 bucket, interaction, horizon을 동시에 많이 보면
multiple-testing으로 우연한 양성이 생길 수 있다. 따라서 ranking과
`PROMISING`은 탐색 결과일 뿐 통계적 확증이나 자동 promotion이 아니다.

## 실행과 출력

```bash
python -m src.research.alpha_discovery
```

출력 파일의 주요 구조:

```text
schema_version
generated_at
primary_horizon
analyzed_horizons
configuration
input_summary
families
  SMART_MONEY
    summary
    single_features
    interactions
  MOMENTUM
    summary
    single_features
    interactions
candidate_counts
top_candidates
automatic_trading_changes=false
```

입력 원장 schema/version은 결과 manifest에 기록한다. 분석 결과가 좋아도 이
모듈은 trading configuration이나 원장을 변경하지 않는다.
지원 범위를 벗어난 미래 schema나 손상된 schema/version은 fail-closed하며,
입력과 출력이 같은 경로인 실행도 원장 보호를 위해 거부한다.
