# Paper MVP 운영 기준

## 상태

- Project phase: `PAPER_MVP_RUNNING`
- RPC state: `RPC_OPTIMIZATION_COMPLETE`
- Start UTC: `2026-09-16T09:48:28.973888+00:00`
- Start KST: `2026-09-16T18:48:28.973888+09:00`
- First review UTC: `2026-09-23T09:48:28.973888+00:00`
- First review KST: `2026-09-23T18:48:28.973888+09:00`
- Preferred observation end: start 이후 7~14일
- Runtime/origin SHA: `466bd8cab005c971d863c4a7324e51618832ba69`
- Stale deployment marker: `7e8012e144df82e12ec85f7cdd93883503184821`

Stale marker와 deployment certification 문제는 Paper 실행 blocker가 아니다. Marker를 수동 수정하거나 이 문서 때문에 production deploy를 실행하지 않는다.

## PRE_MVP_HISTORICAL_BASELINE

아래 값은 start 시점에 `paper_trades.json.lock`의 shared lock을 잡은 상태에서 읽은 고정 baseline이다. 원장을 초기화하거나 기존 결과를 제외하지 않는다.

| Metric | Baseline |
|---|---:|
| Ledger schema | 2 |
| Ledger version | 1,198,558 |
| Max event sequence | 9,474 |
| Total BUY / positions | 104 |
| Completed positions | 103 |
| Carry-in open positions | 1 |
| SELL events | 121 |
| Cash | 9,286,992,450 lamports |
| Cumulative realized PnL | -57,168,670 lamports |
| Current unrealized PnL | +3,165,322 lamports |
| Closed wins / losses | 18 / 85 |
| Expectancy | -555,035.63 lamports per completed position |
| Profit factor | 0.3541 |
| Median return | -10.9663% |
| Max realized drawdown | 61,188,363 lamports |
| Max loss | -7,426,649 lamports |
| Largest winner | 4,610,478 lamports |
| Largest-winner contribution | 14.71% of positive PnL |

Strategy-family baseline:

| Family | BUY | Completed | Realized PnL |
|---|---:|---:|---:|
| SMART_MONEY / Route A | 4 | 3 | -17,226,116 lamports |
| MOMENTUM / Route B | 100 | 100 | -39,942,554 lamports |

Start 시점의 carry-in position은 `risk_state=NORMAL`, consecutive quote failures 0, quote age 0.6초였다. 이 position의 향후 실현손익은 전체 MVP realized delta에는 포함하되, 새 MVP entry cohort와 별도로 표시한다.

## Cohort 경계

- 새 MVP trade: BUY event의 `event_seq > 9474`
- 새 MVP completed trade: 위 새 BUY와 같은 `position_id`가 start 이후 완전히 종료된 경우
- Carry-in 결과: start 시점 이전 BUY에서 start 이후 발생한 SELL
- MVP realized PnL delta: 현재 누적 realized PnL - `-57,168,670`
- 새 entry cohort 성과와 carry-in 성과를 분리하고, 전체 ledger 누적 성과를 MVP 성과로 오인하지 않는다.

## 동결된 운영 설정

- `OBSERVATION_MODE=true`
- `APPROVED_SIGNAL_PAPER_MODE=true`
- `APPROVED_SIGNAL_MAX_OPEN_POSITIONS=8`
- 기본 Paper 진입 크기: 현재 가상 현금의 0.5%
- MOMENTUM / Route B 진입 크기: 기본 크기의 15%
- Route A stop loss: -15%
- Route B stop loss: -10%
- 양 route 1차 take profit: +30%에서 80% 매도
- 1차 익절 후 잔여분: post-TP peak 대비 50% trailing stop
- SMART_MONEY / MOMENTUM의 나머지 filter와 threshold는 runtime SHA의 현재 semantics로 동결
- Live trading 금지

최소 7일 동안 threshold, safety/momentum score, sizing, SL/TP, trailing stop, max positions, RPC/provider/retry 설정을 변경하지 않는다.

## 측정 지표

- new trade count: cohort 경계 이후 BUY 수
- completed trade count: 새 MVP BUY 중 완전 종료 수
- open position count: 현재 open 및 MVP cohort open을 각각 보고
- realized PnL: 누적 delta, carry-in, 새 entry cohort를 분리
- unrealized PnL: current quoted value - remaining cost
- win rate: 완전 종료 position 중 realized PnL이 양수인 비율
- average win/loss: 양수/음수 completed-position PnL 평균
- expectancy: completed-position PnL 평균
- profit factor: 양수 PnL 합 / 음수 PnL 절댓값 합
- median return: completed position별 realized PnL / entry cost의 중앙값
- max drawdown: close timestamp 순 누적 realized PnL의 peak-to-trough 최대값
- max loss / largest winner: completed-position PnL의 최솟값/최댓값
- largest-winner contribution: largest winner / 전체 positive PnL
- extreme-winner-excluded expectancy: largest winner 한 건을 제외한 completed-position 평균 PnL
- holding time: BUY timestamp부터 최종 SELL timestamp까지
- fees: Jupiter quote output에 내재한 route fee를 사용하며 별도 network/Jito fee가 Paper 원장에 차감되지 않는 한 이를 명시
- slippage/price impact proxy: stored expected slippage bps, entry/exit price impact, trigger price impact
- entry latency: signal detected timestamp부터 entry quote timestamp까지
- family performance: SMART_MONEY / Route A와 MOMENTUM / Route B를 분리
- chronological daily performance: start timestamp 기준 UTC 일자별 delta

## Daily report 원칙

매일 new/completed/open trades, realized/unrealized PnL, win rate, expectancy, profit factor, drawdown, family 성과와 operational error만 관찰한다. 하루 성과를 근거로 전략이나 threshold를 변경하지 않는다.

## 즉시 개입 blocker

다음만 MVP보다 우선한다.

- monitor 반복 crash
- risk-manager offline
- Paper ledger corruption 또는 data loss
- position state corruption
- exit path failure
- security/safety issue
- Paper 거래가 실제로 중단됨

단발 `StateLockTimeout`, 단발 missed sample, 일부 RPC 429, stale deployment marker, minor dashboard 문제와 불완전한 telemetry는 기록만 하고 MVP를 계속한다.

## 7일 판단 기준

- `POSITIVE_MVP_SIGNAL`: 비용 반영 후 expectancy와 profit factor가 양수 방향이고 여러 날짜에서 반복되며 extreme winner 의존과 drawdown이 과도하지 않다.
- `NEGATIVE_MVP_SIGNAL`: expectancy와 median trade가 지속 음수이고 profit factor가 1 미만이며 winner 제거 또는 chronological view에서도 edge가 없다.
- `INSUFFICIENT_MVP_SAMPLE`: 7일이 지나도 trade count와 exposure가 결론에 부족하다.

새 window가 historical negative evidence를 반복하면 `CURRENT_STRATEGIES_NO_VALIDATED_ALPHA`로 판정하고 infrastructure optimization이 아니라 strategy research를 재설계한다.
