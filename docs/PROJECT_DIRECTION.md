# aibot 프로젝트 방향과 종료 조건

## 최우선 목표

이 프로젝트의 최우선 목표는 **완벽한 인프라를 만드는 것**이 아니라 **실제 production/paper 데이터에서 트레이딩 수익 가능성을 MVP 수준으로 빠르게 검증하는 것**이다.

Infrastructure는 목적이 아니라 수익 가능성 검증을 위한 수단이다. 다음 핵심 질문에 답하기 전에 인프라 완성도를 100%로 만들려고 하지 않는다.

> 이 전략이 실제 거래와 유사한 Paper 환경에서 비용과 슬리피지를 반영한 뒤에도 양의 기대값을 보일 가능성이 있는가?

## MVP 우선 원칙

현재 수준에서 Paper MVP를 막는 치명적 문제가 아니라면 작은 infrastructure 개선 때문에 trading validation을 계속 미루지 않는다. 다음 작업은 Paper MVP를 막는 material issue가 확인되지 않는 한 후순위로 둔다.

- minor RPC optimization
- telemetry 추가
- dashboard 개선
- 작은 coverage 개선
- memory micro-optimization
- deployment polish
- non-critical refactor

진행 판단의 기본 순서는 다음과 같다.

1. Infrastructure stabilization
2. 최소 coverage 확인
3. Paper MVP eligibility 판단
4. Alpha, Future Shadow, Paper MVP 준비로 이동

## 전략 범위와 검증 순서

현재 검증 전략은 다음 두 가지다.

- `SMART_MONEY`: `NO_EVIDENCE_OF_ALPHA`
- `MOMENTUM`: `NO_EVIDENCE_OF_ALPHA`

현재 Alpha evidence가 없다는 사실은 인정한다. 그러나 Alpha가 없다는 이유로 infrastructure 개선을 무한 반복하지 않는다.

기존 검증 순서는 유지한다.

> 관찰 → 데이터 수집 → 성과 측정 → Alpha Discovery → Future Shadow → Paper → 제한적 Live

Live trading은 Paper MVP보다 먼저 하지 않는다. 제한적 Live는 충분한 Paper evidence, positive expectancy evidence, risk limits, execution sanity가 확인된 뒤에만 검토한다.

## RPC 연구의 제한

Fresh RPC baseline 확인 이후 infrastructure 관련 Batch를 최대한 제한한다.

1. Fresh RPC baseline을 확인한다.
2. 정말 material한 RPC issue가 재현된 경우에만 최소 fix를 최대 1회 검토한다.
3. 그 뒤 RPC optimization을 종료한다.
4. Alpha, Future Shadow, Paper MVP 준비 단계로 이동한다.

새로운 RPC 문제를 계속 발견한다는 이유만으로 같은 최적화 주기를 반복하지 않는다.

## Paper MVP 측정 범위

Paper MVP는 실제 거래 흐름과 유사한 production-like paper operation으로 실행하고 다음을 측정한다.

- cumulative PnL
- trade count와 exposure
- win rate
- average win과 average loss
- expectancy
- profit factor
- max drawdown
- median return과 tail loss
- extreme winner dependency
- fees
- slippage
- latency/execution penalty
- route별 성과
- `SMART_MONEY`와 `MOMENTUM` 성과 비교
- 가능한 경우 chronological split

첫 판단 기간은 최소 7일, 권장 7~14일이다. 단, 충분한 trade count와 exposure가 없다면 날짜만 채웠다는 이유로 결론을 내리지 않는다.

## MVP 성공 신호

Paper MVP의 목표는 "확정된 돈 버는 전략"을 선언하는 것이 아니라 **더 깊게 투자할 가치가 있는 신호**를 찾는 것이다. 다음과 같은 조짐이 여러 표본에서 나타나면 infrastructure, execution, coverage를 더 깊게 개선할 가치가 있다.

- 비용 반영 후 expectancy가 양수 방향이다.
- 여러 시간과 날짜에서 결과가 반복된다.
- 양수 결과가 extreme winner 하나에만 의존하지 않는다.
- max drawdown이 감당 가능한 범위다.
- chronological split에서 결과가 완전히 붕괴하지 않는다.

## 중단 조건

충분한 Paper MVP sample에서도 다음 현상이 지속되면 infrastructure를 더 고쳐 Alpha를 억지로 만들지 않는다.

- expectancy가 계속 음수다.
- profit factor가 지속적으로 1 미만이다.
- 양수 결과가 극단적 winner 한두 개에 의존한다.
- chronological split이 불안정하다.
- 비용 반영 후 edge가 소멸한다.

이때 결론은 "봇을 만들 수 없다"가 아니라 **현재 `SMART_MONEY` / `MOMENTUM` 전략에서는 검증 가능한 Alpha가 없다**이다. 이후에는 infrastructure 최적화가 아니라 strategy research 자체를 새로 설계한다.

## Engineering intervention rule

Production issue는 다음 중 하나에 해당할 때만 MVP validation보다 우선하여 수정한다.

- process가 반복적으로 죽는다.
- data loss 또는 corruption 가능성이 있다.
- sample bias가 material하다.
- Paper trading 자체가 불가능하다.
- execution correctness가 깨진다.
- security 또는 safety issue가 있다.

그 외 문제는 backlog에 기록하고 MVP validation을 먼저 진행한다.

## Anti-overengineering rule

- 새 아이디어가 있다는 이유로 feature를 추가하지 않는다.
- 측정 가능한 material problem이 있을 때만 변경한다.
- 한 Batch에는 가장 큰 원인 하나만 다룬다.
- 최근 성과만 보고 threshold를 조정하지 않는다.
- Alpha를 만들기 위해 infrastructure를 계속 변경하지 않는다.

## Current Milestone

- Canonical runtime stabilization: complete
- Canonical production SHA: `7e8012e144df82e12ec85f7cdd93883503184821`
- Fixed RPC window: `2026-09-16 03:00–06:00 UTC`
- Next action: `2026-09-16 06:01 UTC` 이후 기존 fixed window를 read-only로 평가

Fresh RPC baseline 평가 결과 이후에는 다음 경계를 지킨다.

- material RPC issue가 재현되면 최소 fix를 최대 1회 검토한다.
- material issue가 재현되지 않으면 RPC optimization을 종료한다.
- 그 뒤 Alpha, Future Shadow, Paper MVP 준비 단계로 이동한다.
- 평가 전에는 RPC, Alpha, trading behavior를 변경하지 않는다.

## Codex operating instruction

앞으로 Codex가 aibot 작업을 수행할 때 다음 규칙을 따른다.

1. 작업 시작 전에 `docs/PROJECT_DIRECTION.md`를 읽는다.
2. 제안 작업이 MVP trading validation을 앞당기는지 확인한다.
3. Paper MVP를 막지 않는 infrastructure work는 우선순위를 낮춘다.
4. 새로운 broad refactor를 시작하기 전에 이 문서와 충돌하는지 확인한다.
5. 프로젝트 방향은 사용자의 명시적 승인 없이 변경하지 않는다.
