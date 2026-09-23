# Research Future Validation

## 목적과 안전 경계

`src.research.future_validation`은 현재 Paper MVP와 분리된 offline 연구 계층이다.
Research archive의 canonical observation과 이미 저장된 Jupiter outcome을 읽어 여러
진입 가설을 같은 미래 기간에 평가한다. 가설별 RPC 요청, paper position, trading
configuration 변경은 만들지 않는다.

다음 값은 feature로 사용하지 않는다.

- horizon `samples`, MFE/MAE, paper PnL과 SELL 결과
- 완료·청산 상태와 candidate early failure
- 현재 wallet performance를 과거 signal에 붙인 값
- signal 이후 prospective snapshot

안전 분류는 다음과 같다.

| 분류 | 값 |
|---|---|
| `SAFE_PRE_SIGNAL` | 정규화된 prospective raw snapshot, whale paid amount |
| `SAFE_AT_SIGNAL` | safety/momentum snapshot, price impact, latency, copy gap |
| `POST_OUTCOME_FORBIDDEN` | outcome, excursion, paper PnL, 현재 wallet 성과 |
| `AMBIGUOUS` | decision/status/reason처럼 기존 gate 선택을 포함하는 값 |

## Frozen hypothesis registry

Registry row는 다음을 고정한다.

- deterministic `hypothesis_id`
- family, version, rationale, required features
- pre-signal eligibility와 immutable condition definition
- creation/discovery source
- `discovery_data_end`와 registry 생성 시각 이후의 `validation_start`
- research-only status와 definition fingerprint

ID는 정의의 canonical JSON hash로 만들며 시간과 결과 status를 포함하지 않는다.
Fingerprint는 cutoff까지 포함하므로 생성 후 정의나 cohort 경계가 바뀌면 실행을
거부한다. Alpha bucket 규칙과 prospective snapshot/파생식은 각 contract version과
digest도 고정하며 현재 evaluator와 다르면 fail-closed한다. Family별 후보는 최대
8개다. 기존 Alpha Discovery의 `PROMISING` fixed bucket 후보만 import할 수 있고 자동
trading promotion은 없다. Alpha report의 schema, bucket version, 전체 bucket
definition이 현재 contract와 정확히 같지 않으면 stale 후보를 import하지 않는다.

기본 registry에는 결과를 보기 전에 고정한 MOMENTUM prospective 가설 5개가 있다.

1. volume growth + net-buy growth
2. net-buy growth + buy/sell ratio improvement
3. liquidity expansion + volume expansion
4. buyer growth without price growth exceeding volume growth
5. sustained volume and buyer growth across at least three minute buckets

경계는 결과를 보고 조정하지 않는다. Prospective 값은 collector에 파생값을 중복
저장하지 않고 최대 6개의 raw minute-bucket snapshot에서 offline 계산한다. 이
snapshot은 겹치는 rolling 5-minute projection이므로 정확한 1-minute flow로
해석하지 않는다.

## Historical / future isolation

`discovery_data_end`는 discovery 입력의 최신 signal 시각이고, `validation_start`는
그 시각과 registry 생성 시각 중 더 늦은 값이다. 따라서 등록 전에 발생했지만 outcome
backlog 때문에 나중에 archive된 signal도 미래 표본에서 제외한다. Future event
timestamp는 두 경계보다 모두 엄격히 커야 하며 경계와 같은 timestamp도 제외한다.
Future family cohort를 먼저 만든 뒤
mint별 첫 signal만 남기고 각 hypothesis eligibility를 평가한다. 따라서 과거에 본
holdout이나 같은 mint의 반복 signal이 진정한 future evidence로 섞이지 않는다.

동일 observation ID의 exact replay는 하나로 줄이고 내용이 충돌하면 그 identity를
전체 제외한다. 한 canonical event의 5m/15m/30m/60m outcome map은 한 번만 읽고
모든 eligible hypothesis가 같은 객체를 재사용한다.

## Metrics와 status

Event-level과 `first_signal_per_mint` view를 함께 제공하고 status gate는 unique-mint
60m view를 사용한다.

- eligible/completed/unique mint/coverage/signal density
- expectancy, PF, median, win rate, average win/loss, max loss
- additive-return drawdown proxy
- largest-winner contribution와 top-winner-removed expectancy
- UTC day/week split

기존 Research Engine V2의 sample 50, holdout-size 15를 unique-mint 최소치로 재사용하고
trackable coverage 80%를 요구한다. 그 전에는 `FUTURE_INSUFFICIENT`다. 충분한 표본에서
expectancy/PF/median/extreme-winner/여러 positive day gate를 통과하지 못하면
`FUTURE_NEGATIVE`, 모두 통과하면 review-only `FUTURE_PROMISING`이다.
평가 후 registry의 mutable status만 원자적으로 `FUTURE_VALIDATING`, `FUTURE_FAILED`,
`FUTURE_PROMISING` 중 하나로 갱신한다. 정의와 fingerprint는 변경하지 않는다.
Registry lock 안에서 source registry version과 evaluation run ID를 확인한 뒤 report와
status를 함께 publish한다. 경쟁 실행에서 stale run은 report를 덮어쓰기 전에 실패한다.

## 실행

기본 실행은 archive와 기존 Alpha report를 읽고 registry가 없을 때 한 번만 생성한다.

```bash
python -m src.research.future_validation
```

명시적 ledger를 이용한 로컬 점검은 다음과 같다.

```bash
python -m src.research.future_validation \
  --input data/signal_observations.json \
  --registry data/hypothesis_registry.json \
  --output data/future_validation.json
```

생성 파일은 research output일 뿐 거래 원장이 아니다. 기존 registry가 존재하면 새
Alpha 결과를 보고 자동 재생성하지 않는다. 새 selection iteration은 별도 registry와
새 cutoff로 명시적으로 시작해야 한다.
