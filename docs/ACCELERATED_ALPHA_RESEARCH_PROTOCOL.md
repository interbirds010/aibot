# Accelerated Paper Alpha Research Protocol v1

목표는 `DATA → FORENSIC → HYPOTHESIS → SHADOW CHALLENGERS → FRESH VALIDATION → PROMOTE / REJECT`를 짧게 반복하는 것이다. Phase는 `PAPER_MVP_RUNNING`, project status는 `PAUSE_STRATEGY_REVIEW`, live trading은 `DISABLED`다. Discovery 속도와 독립적인 미래 검증을 함께 높인다.

## Batch와 데이터 경계

Legacy canonical discovery batch는 **20 completed Control trades OR 7 active KST signal/entry days** 중 먼저 도달할 때 닫는다. 이미 존재하는 첫 데이터는 `initial discovery`로 명시한다. 완료 순으로 포함하며 늦게 끝난 open 거래는 이후 batch로 들어간다. 동일 거래를 여러 discovery batch에 중복 포함하지 않는다. 이 legacy 경로의 7일은 completed 거래로 대표되는 날짜다. 새 `ALPHA_BATCH_001`은 acceptance 후 등록하고 admitted BUY가 대표하는 KST signal 날짜 7개를 사용하며 open 거래도 포함한다. 해당 원본 종료 계약을 유지하는 `import-telemetry-batch`를 사용한다. [전환 runbook](ACCELERATED_ALPHA_TRANSITION.md)을 따른다.

`BATCH_001`, `BATCH_002`처럼 immutable dataset/identity와 boundary를 보존한다. Frozen batch는 재분할하지 않는다. 재실행, 동시 실행과 crash 후 복구는 봉인된 내용이 같은 경우만 멱등 허용한다. 과거 입력 내용이 달라지면 conflict로 차단한다. Open 거래를 강제 청산하거나 PnL을 추정해 completed에 넣지 않는다.

Canonical row는 position/event identity, signal/entry decision/close UTC, 원래 비용과 실현손익, 별도 `features` 및 `forensic`을 가진다. Feature마다 `value`, `observed_at`, `source`를 기록하며 observed_at이 entry decision 이후이면 predictor로 거절한다. 실제 저장된 entry-time projection만 사용하고 현재 wallet/market 값을 과거 거래에 붙이지 않는다. Feature 이름은 SMART_MONEY/MOMENTUM으로 제한하지 않는다.

외부 출력은 runtime과 분리한다. 상태 쓰기는 state_store lock/atomic API만 사용한다. 도구의 입력/메모리 상한은 offline 처리 단위이며 데이터 자동 삭제나 실험 중단 조건이 아니다. 더 큰 이력은 별도 bounded dataset으로 처리한다.

## 자동 forensic report contract

Batch close마다 다음을 봉인한다.

1. Source/dataset hash, batch boundary, origin, rule/schema/build reference.
2. 모든 winner와 loser의 비용·수익률·보유 시간 및 outcome labels.
3. Entry-time feature별 winner/loser 분포, missing count, coverage.
4. Candidate 탐색 범위와 시도 수, temporal split, tail dependence, missing/coverage screen.
5. 최대 10개 ranked discovery **proposal**과 discovery evidence. Active shadow challenger는 최대 5개다.
6. 후속 미래 start boundary와 검증 stage 제안; 자동 Control 적용 없음.

Winner에는 fast TP, 큰 winner, long-hold winner, low-liquidity/high-flow winner를 살피는 질문을 포함한다. Loser에는 빠른 SL, 즉시 급락, 상승 후 reversal, near-TP failure, sideways then SL, long-hold SL, loss magnitude를 살핀다. 단 labels의 수치 경계는 versioned label contract로 고정해야 한다. 경계나 price trajectory가 없으면 UNKNOWN으로 남긴다. 수익률만으로 즉시 급락·near-TP·reversal을 추측하지 않는다.

보유 시간은 entry/close로 계산 가능하다. TP 도달 시각, sampled MAE/MFE, liquidity/flow 변화와 price trajectory는 실제 시계열이 있는 경우만 계산하며 sampling cadence와 missingness를 공개한다. 실제 intratrade maximum이라고 주장하지 않는다. Entry 직후 acceleration은 outcome label이고, entry 이전 acceleration만 predictor가 될 수 있다.

False Momentum은 진입 후 reversal/SL에 관한 forensic label이다. 사후 trajectory를 predictor로 쓰지 않는다. 그 label과 entry 이전 liquidity·flow·wallet 조합·quote/retry·concurrency·pool age·시장 activity의 관계를 discovery에서 찾고, 다음 미래 batch에서 재검증한다.

현재 frozen N3 snapshot에는 주로 analysis/preflight duration과 quote age만 있다. 광범위한 liquidity/flow/wallet/trajectory 연구는 추가 telemetry가 없으면 UNKNOWN이다. 없는 필드를 가짜 값으로 채우지 않는다.

## Multi-challenger architecture

Control signal/trade는 하나다. Entry-time feature projection을 최대 **5개의 active challenger**가 함께 읽고 would_enter, would_skip 또는 alternative classification을 기록한다. Challenger는 BUY/SELL을 호출하지 않는다. Position sizing·exit·경로를 바꾸는 가상 fill은 현재 도구로 주장하지 않는다. Classification은 annotation이며 경제 효과를 계산하려면 별도 entry/filter 의미가 필요하다.

각 definition은 candidate ID/version, immutable rule/hash, kind, creation batch, discovery evidence, 미래 start event_seq와 UTC timestamp를 가진다. Rule은 bounded numerical/category predicates와 AND/OR interaction을 표현한다. Missing predictor는 UNKNOWN이며 Control에는 영향이 없다. Future signal은 event_seq와 timestamp 경계를 모두 만족해야 한다.

A-v1의 threshold를 결과에 맞춰 수정하지 않는다. 변경은 A-v2라는 새 definition과 새 미래 경계로 등록한다. Immutable definition과 mutable lifecycle ledger를 분리하고 registry 안에서 max-five를 원자적으로 선점한다. Retired candidate의 원본과 결과는 보존한다.

등록은 구현된 explicit offline CLI/API로 수행한다. 이번 단계는 runtime hook을 추가하지 않으며 prospective로 저장된 entry projection을 나중에 immutable rule과 결합한다. 활성화된 telemetry에 realtime shadow fan-out을 연결하는 것은 별도 integration이다. 아직 recording되지 않은 predictor를 결과를 본 뒤 재구성해 fresh 검증으로 사용하지 않는다.

## Lifecycle와 검증 stage

가능한 상태는 `DISCOVERY_ONLY`, `FRESH_DIRECTION_CONFIRMED`, `PROMISING_CHALLENGER`, `ROBUST_PAPER_ALPHA`, `ONLY_NEGATIVE_FILTER`, `REJECT_EARLY`, `INSUFFICIENT_COVERAGE`다. Lifecycle 결정은 evidence와 연결해 append history로 남긴다. Tool이 Control strategy를 자동 변경하지 않는다.

- Stage 1: 등록 이후 첫 **20 fresh completed**의 방향성을 본다. User 허용 범위 20–30 중 v1은 20으로 고정한다.
- Stage 2: Stage 1 생존 후보만 그 뒤 **추가 30 fresh completed**. User 허용 범위 30–50 중 v1은 30으로 고정한다. 첫 20을 Stage 2에 재사용하지 않는다.
- Stage 3: 유망 후보의 더 긴 prospective validation. 추가 표본/day/coverage/cost 기준을 별도 사전 등록한 뒤 수집하며, Stage 1/2 성과를 보고 즉흥 종료하지 않는다. 현재 tool은 Stage 3 자동 ROBUST 판정을 하지 않는다.

Stage 1 방향 역전, winner opportunity cost가 avoided loss보다 큼, normalized net <= 0이면 `REJECT_EARLY` 후보가 된다. Zero coverage는 `INSUFFICIENT_COVERAGE`; 과도한 missing, 한두 거래 의존, temporal instability는 명시적 screen과 함께 보류/폐기한다. Discovery positive point estimate만으로 성공을 선언하지 않는다. Same batch에서 탐색하고 검증 완료했다고 주장하지 않는다.

Stage 2에서도 out-of-sample 방향성과 비용·tail/temporal 안정성을 확인한다. 상태를 올리더라도 실제 Control 변경은 `PROMISING_CHALLENGER` 이상에서 별도 사용자 결정이다. `ROBUST_PAPER_ALPHA`는 live 수익성이나 live trading 승인이 아니다. Negative filter가 Control 손실을 줄여도 남은 portfolio가 손실이면 profitable Alpha로 부르지 않는다.

## Economic evaluation

각 filter/subset은 included/excluded/unknown, winners/losers retained/excluded, avoided loss, missed profit, net effect, expectancy, PF, win rate, return %, drawdown, coverage를 보고한다. Normalized effect는 원래 entry cost를 분모로 하는 equal 1 SOL bookkeeping이며 actual SOL과 함께 제시한다. 큰 position 몇 건의 기여와 top-2 removal/chronological split을 공개한다.

Filter의 효과는 `avoided loss - missed profit`이다. Loser 적중 수만으로 성공하지 않는다. UNKNOWN은 포함 또는 제외로 조용히 환산하지 않는다. 경제 효과는 기존 Control의 동일 exit 결과를 사용하는 counterfactual이며 capital recycling, alternative fill, market impact를 시뮬레이션한 것이 아니다.

유망 후보의 stress는 기존 recorded-paper, extra 50 bps, extra 100 bps + fixed 10,000 lamports sensitivity를 사용한다. Network/tip/adverse/slippage 비용이 정확히 계측되지 않으면 가정값과 미포함 항목을 공개한다. 이미 원장에 포함된 비용을 재가산하지 않는다. Paper에서 겨우 양수인 후보를 Alpha로 확정하지 않는다.

## Telemetry transition와 운영 자동화

N3 feasibility checkpoint는 경제 지표 없이 별도 monitor가 담당한다. Low coverage 결과 보고 후 `86d7ddc` telemetry activation을 우선 다음 작업으로 권고하되 자동 실행하지 않는다. [N3 amendment](N3_FEASIBILITY_AMENDMENT_2026-10-07.md)의 기존 cutover guard 제약을 해결하고 원래 기록을 보존해야 한다.

Telemetry activated 이후 BUY뿐 아니라 rejected/RPC skipped/quote failed predictor population도 freeze한다. Predictor-only 행의 coverage/selection bias는 completed 경제 모집단과 분리하며 미실행 signal에 임의 PnL을 만들지 않는다. Existing split-stream epoch/build/config fingerprint와 join을 유지한다.

Offline refresh 도구는 input freeze → batch close → forensic/comparison → bounded candidate generation → overfit screen → ranking → proposal을 실행한다. 등록/retire는 명시적 API/CLI이고 Control 적용은 별도 결정이다. External checker는 runtime이 쓰는 파일을 읽고 외부 artifact만 쓴다. PC offline 동안 실행·즉시 알림은 보장하지 않는다.

```powershell
$repo = 'C:\Users\user\.codex\worktrees\accelerated-alpha\solana-ai-bot'
$python = 'C:\Users\user\Documents\solana-ai-bot\.venv\Scripts\python.exe'
$runtime = 'C:\Users\user\Documents\aibot-local-paper'
$out = 'C:\Users\user\Documents\aibot-alpha-research\batches'
& $python "$repo\scripts\accelerated_alpha_research.py" refresh --runtime $runtime --output $out
& $python "$repo\scripts\accelerated_alpha_research.py" refresh --runtime $runtime --output $out --execute
```

기본 preview는 파일을 만들지 않는다. Explicit `--execute`는 외부 초기 input/dataset와 닫힌 batch/report만 봉인한다. `--watch --execute --interval 60`은 같은 refresh를 반복하는 선택적 offline watcher이며 이번 작업에서 계속 실행하는 프로세스로 등록하지 않았다. 현재 runtime에는 자동 forensic watcher나 challenger hook을 추가하지 않고 checkpoint heartbeat만 실행한다.

Canonical telemetry-derived input은 `--runtime` 대신 `--input <dataset.json>`으로 처리할 수 있다. 새 telemetry batch는 별도 namespace로 `import-telemetry-batch`를 사용한다. `register --definition <definition.json> --output <research-root>`는 discovery screen, 미래 start boundary와 max-five를 검증한다. `stage --definition <definition.json> --research-root <registered-root> --input <fresh-dataset.json> --stage 1|2|3`은 봉인 definition과 실제 등록 시각을 확인하고 immutable stage report 및 연구 상태를 기록한다. Control에는 적용하지 않는다. 등록되지 않은 draft로 정식 fresh 상태를 주장할 수 없다. Numerical coverage/missing/discovery screen 정책은 등록 전에 고정하며 자동 proposal을 등록 후보로 취급하지 않는다. Library replay boundary도 실제 registry 등록 시각을 대체하지 않는다.

Telemetry split-stream에서 canonical dataset을 만드는 생산 adapter와 runtime fan-out은 다음 phase integration이다. 현재 직접 제공하는 verified source adapter는 frozen N3 snapshot이며, wider telemetry 연구 계약과 generic canonical input API를 준비한 상태다.

이번 변경에는 추가 RPC 최적화, PM2 tuning, supervisor 개선, cosmetic refactor, dependency upgrade, frozen source 수정, telemetry activation, push/배포를 포함하지 않는다.
