# Accelerated Alpha checkpoint 전환 준비

2026-10-14 KST. 이 문서는 **전환 도구의 준비 상태**를 설명한다. 현재 frozen runtime의 실행, 전략, rule, threshold, cohort, sizing, SL/TP와 원장은 변경하지 않는다. Telemetry activation과 natural acceptance는 앞으로의 명시적 운영 작업이다.

## A. Current state

- Repository: `interbirds010/aibot`.
- Frozen root: `C:\Users\user\Documents\aibot-local-paper`, HEAD `8f0b9429352c9961b895a9f9c0e2d1f3e6b19d05`.
- 준비 기준 telemetry 후보: `86d7ddc96fb6b8584df26bf1ef2a964e83454480`.
- 구현 root: `C:\Users\user\.codex\worktrees\accelerated-alpha-transition\solana-ai-bot`, branch `codex/accelerated-alpha-transition`.
- 최초 읽기 전용 점검: completed 25, active KST days 6, hit 0, non-hit 25, unknown 0, recording OK, backlog 0. 경제 평가는 하지 않았다.
- 기존 `accelerated-alpha`의 미커밋 연구 도구를 사본으로 가져와 이어서 구현했다. 원래 checkout/worktree의 foreign 파일은 수정하지 않았다.
- 실제 frozen BUY 27개의 timing baseline을 `C:\Users\user\Documents\aibot-alpha-transition-evidence\latency-baseline-20261014.json`에 봉인했다. 완료 거래 수와 timing BUY 표본 수는 다르다.

## B. N3 checkpoint transition matrix

Gate는 **completed Control >=30 OR 완료 거래가 대표하는 active KST signal days >=7**다. 경제 필드는 판정에 사용하지 않는다.

| Checkpoint | 판정 | N3 처리 | Alpha 처리 |
|---|---|---|---|
| hit <=1, recording OK, unknown 0 | `N3_LOW_FRESH_COVERAGE` | 외부 feasibility amendment close annotation, frozen evidence 보존 | 별도 telemetry root 전환 후 acceptance와 Batch 001 |
| hit >=2, recording OK, unknown 0 | `N3_CONTINUE_PROSPECTIVE_VALIDATION` | 동일 rule을 새 epoch의 별도 observational segment에서 유지 | N3 완료를 기다리지 않고 같은 단일 Control의 telemetry로 Batch 001 |
| recording 이상/unknown/provenance 불일치 | `BLOCKED` | 결론·전환 보류 | state를 고쳐 맞추지 않고 원인 검토 |

LOW COVERAGE는 `N3_FAILED` 또는 손실 filter 오류를 뜻하지 않는다. 새 전환 annotation은 원래 N3의 hard-cap `closure.json`, `final_report.json`, observer `CLOSED`를 위조하지 않는다.

CASE B에서는 두 Control을 병렬 실행하지 않는다. 정지 경계에서 원장을 그대로 이전한 **한 개의 새 Paper Control**에 Alpha와 N3 관찰기를 붙인다. 원래 frozen 증거는 옛 root에 보존하고 새 관찰은 별도 epoch/build/latency segment로 기록한다. 원래 observer 프로세스가 새 root를 frozen build로 읽도록 속이지 않는다. N3 rule·sample gates·hard cap은 그대로이며 빌드 간 결과를 무조건 동일 분포로 합치지 않는다.

N3 raw interval은 telemetry mark의 미세한 시차를 대신 쓰지 않고 BUY 원장에 저장된 원래 `analysis_completed_at`/`entry_quote_at`과 signal 시각으로 계산한다. 진입 전에 생성된 시각만 사용하며 이후 가격/손익은 분류에 사용하지 않는다. 원본 시각·identity를 확인할 수 없으면 UNKNOWN/BLOCKED다.

## C. Telemetry activation plan

Unmodified `86d7ddc`는 원래 N3 CLOSED를 요구하므로 그대로는 두 feasibility 분기를 지원하지 않는다. 이번 준비 build의 **명시적 feasibility boundary adapter**가 필요하다. Boundary 없는 기존 정식 종료 검증은 유지된다.

운영 순서는 다음과 같다.

1. checkpoint, amendment, frozen source와 recorder를 검증한다. 최초 checkpoint/transition report를 봉인한다.
2. 실제 latency baseline과 허용 정책을 activation 전에 봉인한다. 허용 수치를 임의로 선택하지 않는다.
3. `capture-before`로 실제 PID/creation identity, ledger sequence/hash, UTC/KST를 외부에 기록한다.
4. 기존 supervisor의 maintenance disable 명령이 성공한 뒤, 예약 작업을 disable/stop하고 별도로 승인된 operator가 owned Control과 observer를 중지한다. DISABLED supervisor도 health 파일을 쓰므로 프로세스 자체를 중지해야 exact archive를 봉인할 수 있다. Frozen build에는 협력 drain handler가 없으므로 `clean-stop`으로 정상 drain했다고 주장하지 않는다. 미완료 포지션은 강제 매도하지 않는다.
5. 새 telemetry checkout과 기존 환경 기반 자격 증명을 준비한다. `.env` 내용은 출력하거나 연구 파일에 저장하지 않는다. 두 root의 프로세스 inventory가 0인지 확인한다.
6. `clone-state`로 현재 원장/상태의 exact bytes를 state_store 원자 API를 통해 새 root로 옮긴다. `.env`, logs, `data/n3_shadow`와 telemetry 원본은 복사 대상이 아니다. 기존 충돌 파일을 덮어쓰지 않는다.
7. `prepare-boundary --execute`는 최초 checkpoint 당시 counts를 실제 immutable 기록에서 다시 확인하고 source/target state hash와 event boundary를 봉인한다.
8. 새 checkout의 compile/full tests를 통과한다. `snapshot`, `create-epoch`, `validate`를 실행한다.
9. CASE B는 새 Control 시작 전에 외부 N3 continuation segment를 등록한다. 이전 admitted/open positions와 원래 분류도 이어서 보존한다.
10. `resume --execute`로 새 Control만 한 번 시작한다. 옛 root stopped/maintenance 상태를 재확인한다.
11. `record-gap`으로 실제 새 PID/session, epoch/build/config, event boundary와 관찰 가능한 시간 구간을 기록한다. 정확한 stop/resume instant가 관찰되지 않았으면 `UNOBSERVED`로 남긴다.
12. natural runtime acceptance와 real latency를 통과한다. 통과 전 Alpha economics/discovery는 시작하지 않는다.

아래 명령은 **앞으로 전환할 때의 runbook**이며 이번 준비 작업에서는 실행하지 않는다.

```powershell
$py = 'C:\Users\user\Documents\solana-ai-bot\.venv\Scripts\python.exe'
$tools = 'C:\Users\user\.codex\worktrees\accelerated-alpha-transition\solana-ai-bot'
$old = 'C:\Users\user\Documents\aibot-local-paper'
$new = 'C:\Users\user\Documents\aibot-telemetry-paper'
$feasibility = 'C:\Users\user\Documents\aibot-alpha-research\n3-feasibility'
$evidence = 'C:\Users\user\Documents\aibot-alpha-transition-evidence'
$batch = 'C:\Users\user\Documents\aibot-alpha-batch-001'
$research = 'C:\Users\user\Documents\aibot-alpha-batch-001-research'

& $py "$tools\scripts\alpha_transition.py" plan --runtime $old --checkpoint "$feasibility\checkpoint.json" --telemetry-runtime $new --output "$evidence\cutover-plan.json"
& $py "$tools\scripts\entry_telemetry_cutover.py" capture-before --root $old --checkpoint "$feasibility\checkpoint.json" --evidence-output "$evidence\before-stop.json"

# 다음 maintenance와 owned-stop은 전환 실행 결정 뒤에만 수행한다.
& 'C:\Users\user\Documents\aibot-n3-runtime-harness\set_n3_runtime_maintenance.ps1' -Mode disable -RuntimePath $old -PythonPath $py -HarnessHome 'C:\Users\user\Documents\aibot-n3-runtime-harness'
Disable-ScheduledTask -TaskName 'SolanaAIBot-N3-Frozen-Runtime'
Stop-ScheduledTask -TaskName 'SolanaAIBot-N3-Frozen-Runtime'
# 모든 서비스의 owned PID/creation identity를 확인한 후 기존 runner로 Control을 중지한다.
& $py "$old\scripts\local_paper_runner.py" stop --root $old --python $py
# 원래 observer도 capture-before의 PID/creation 및 실제 명령 출처를 대조한 뒤 operator가 중지한다.
# 실제 owned-stop 뒤 별도 reviewed checkout을 만든다. 각 명령이 실패하면 다음 단계로 가지 않는다.
git -C $tools worktree add --detach $new codex/accelerated-alpha-transition
# 기존 환경 기반 자격 증명을 비공개로 준비하고 fingerprint가 같음을 확인한다.
& $py "$tools\scripts\entry_telemetry_cutover.py" clone-state --root $new --frozen-root $old --execute
& $py "$tools\scripts\entry_telemetry_cutover.py" prepare-boundary --root $new --frozen-root $old --checkpoint "$feasibility\checkpoint.json" --amendment "$feasibility\amendment.json" --execute

Set-Location -LiteralPath $new
& $py -m compileall -q src tests scripts
& $py -m unittest discover -s tests -v
& $py "$new\scripts\entry_telemetry_cutover.py" snapshot --root $new --execute
& $py "$new\scripts\entry_telemetry_cutover.py" create-epoch --root $new --execute
& $py "$new\scripts\entry_telemetry_cutover.py" validate --root $new

# CASE B에서만 실행한다. 원래 manifest와 entry/completed 원본은 읽기 전용이다.
& $py "$new\scripts\alpha_n3_continuation.py" register --root $new --original-manifest "$old\data\n3_shadow\manifest.json" --output "$evidence\n3-continuation" --execute
& $py "$new\scripts\entry_telemetry_cutover.py" resume --root $new --python $py --execute
& $py "$new\scripts\entry_telemetry_cutover.py" record-gap --root $new --frozen-root $old --before-evidence "$evidence\before-stop.json" --evidence-output "$evidence\cutover-gap.json"
```

실행 소스는 새 root의 검토한 동일 build여야 한다. `git pull`로 dirty frozen checkout을 갱신하지 않는다. 실패 시 최신 원장과 모든 원본을 보존하며 과거 snapshot으로 rollback하지 않는다.

## D. Runtime acceptance와 real latency

`alpha_telemetry_batch.py acceptance`가 물리 predictor/receipt/outcome, 독립 natural 사건, session·build·schema·config, duplicate, dropped/write counters, queue, ledger BUY/SELL와 future leakage를 검사한다. BUY predictor를 별도로 요구하며 기존 natural 검사에 의존한다. Missing natural 사건은 `TELEMETRY_RUNTIME_BLOCKED`/pending 사유이며 합성 row로 채우지 않는다. Health/latency 이상은 `TELEMETRY_RUNTIME_DEGRADED`다. 모든 검사와 latency가 통과해야 `TELEMETRY_RUNTIME_ACCEPTED`다.

Frozen baseline은 UTC wall-clock 차이를 사용하므로 post sample도 같은 시계로 비교한다. 최초 baseline에는 signal→analysis start, analysis end→preflight start, preflight start→paper BUY, signal→entry decision이 있다. 이 이름과 실제 양 끝점을 artifact에 명시한다. 순수 quote 요청 latency는 frozen source에 start 시각이 없어 `UNAVAILABLE`이며 preflight 시간으로 대체하지 않는다. 새 telemetry의 별도 quote/queue/RPC durations는 기록된 그대로 보고한다.

Latency policy는 `minimum_samples`, `maximum_increase_ms`, `median|p95`와 대상 metric을 **명시적으로 사전 등록**해야 한다. 고정되지 않은 정책이나 실제 표본 부족은 PASS가 아니다. 다음 명령의 수치 인자는 운영자가 결정한 값이며 도구에 기본 허용값은 없다.

```powershell
& $py "$tools\scripts\alpha_latency_baseline.py" collect --runtime $old --output "$evidence\final-pre-activation-baseline.json"
# policy 명령의 필수 인자: --baseline --metrics --minimum-samples --maximum-increase-ms --statistic --output
& $py "$new\scripts\alpha_telemetry_batch.py" acceptance --root $new --baseline "$evidence\final-pre-activation-baseline.json" --policy "$evidence\latency-policy.json" --output "$evidence\acceptance-attempt-001.json"
```

Acceptance report는 immutable이므로 재점검은 새 이름을 사용한다. 근소한 paper 양수 또는 합성 microbenchmark를 실제 latency/robust Alpha로 부르지 않는다.

## E. Batch 001

Acceptance 후 `register`로 `ALPHA_BATCH_001`의 start event_seq/UTC/KST, epoch/build/schema/Control fingerprint와 acceptance hash를 고정한다. 시작 경계는 acceptance 이후여야 한다.

Close는 **20 completed Control trades OR admitted BUY가 대표하는 7개 distinct KST signal days**의 첫 도달이다. Open trade도 active day를 대표하지만 rejects만 발생한 날은 제외한다. 7번째 signal day를 admission한 BUY receipt의 실제 시각/event_seq가 종료 경계다. 자정 이후 BUY receipt가 찍혀도 원래 signal 날짜를 사용한다. N3 feasibility의 완료 거래 기반 active-day 정의는 바꾸지 않는다.

```powershell
# register 필수 인자: --acceptance --output --start-seq --start-utc
& $py "$new\scripts\alpha_telemetry_batch.py" freeze --root $new --output $batch
& $py "$new\scripts\accelerated_alpha_research.py" import-telemetry-batch --input "$batch\ALPHA_BATCH_001\dataset.json" --output $research
```

Freeze는 predictor의 BUY/reject/risk reject/quote failure/RPC skip/OTHER, BUY receipt, SELL legs와 completed outcomes를 별도 물리 파일로 보존한다. Entry와 outcome은 offline join에서만 연결한다. Open-at-cutoff와 late outcome은 pending/excluded로 표시하며 과거 batch를 재작성하지 않는다. Import bridge는 7일 분기에 completed <20이어도 원래 종료를 보존하고 별도 namespace의 `BATCH_001` forensic으로 연결한다. 기존 N3 discovery namespace에 덮어쓰지 않는다.

## F. Forensic pipeline

완료 Control만 actual SOL/return/equal-notional economics에 사용한다. Reject/open candidate에 가상 손익을 만들지 않는다. Feature projection은 원래 predictor schema v2의 봉인 scalar와 exact source path를 사용한다. Raw score components, RPC/retry/wait, quote age/impact, liquidity/flow/wallet concentration/age/pressure와 route를 가능한 범위에서 보존한다. 봉인 시각은 field 획득 시각을 정밀하게 측정했다는 뜻이 아니다.

Loser의 immediate collapse/fast SL/reversal/near-TP/sideways/long-hold, loss magnitude와 winner의 fast TP/continuation/large/long-hold/low-liquidity/high-flow는 명시적 `label_contract`에 따라 분류한다. 필요한 trajectory/exit/threshold가 없으면 `UNKNOWN`이다. Outcome schema v1에는 post-entry price trajectory가 없으므로 현재 adapter만으로 모든 path label/MAE/MFE를 측정했다고 주장하지 않는다. 후속 timestamped trajectory 자료는 forensic 영역에만 붙일 수 있다.

## G. False Momentum

False Momentum은 사후 reversal/SL label이다. Entry predictor에는 label, realized PnL, holding, MAE/MFE와 이후 trajectory를 넣지 않는다. Pre-entry pump/burst/flow acceleration/liquidity velocity/wallet/quote degradation/latency/concurrent pressure/pool age를 대상으로 질문을 생성하고, 자료가 있는 entry-time feature만 discovery에 사용한다. 없는 축은 missing으로 남긴다. Winner의 동일 entry 조건도 함께 비교해 winner destruction을 확인한다.

## H. Challenger registry와 overfit screen

Batch당 discovery proposal 최대 10개이며 active challenger는 최대 5개다. 후보는 exact rule/version/hash/creation batch/discovery evidence/future start boundary를 봉인한다. Rule 변경은 새 `-vN` identity이며 기존 정의는 수정하지 않는다. Proposal의 rationale/expected mechanism/coverage/economics를 보존한다. 실제 등록에서는 frozen discovery rows로 screen을 다시 계산한다.

Screen policy 필수값은 `minimum_sample`, `minimum_affected`, `maximum_single_trade_share`, `maximum_single_day_share`, `maximum_route_share`, `maximum_position_size_share`, `maximum_missing_predictor_rate`, `maximum_winner_profit_destroyed_share`, `minimum_threshold_tail_fraction`이다. 정책 미정/표본 부족/미관측 의존 축/극단 threshold/winner destruction/net<=0은 `REJECT_DISCOVERY`다. Concentration은 기술적 screening이며 인과 또는 통계적 Alpha 증명이 아니다.

```powershell
& $py "$new\scripts\accelerated_alpha_research.py" register --definition "$evidence\reviewed-candidate-v1.json" --output $research
& $py "$new\scripts\alpha_shadow_stream.py" --root $new --research-root $research --execute
```

Physical shadow consumer는 BUY predictor와 receipt identity만 읽고 outcome stream을 읽지 않는다. 등록 시각 이전 신호는 prospective evidence에서 제외한다. 같은 Control 신호의 would_enter/would_skip/classification과 optional score(null)를 외부에 봉인하며 BUY를 추가하지 않는다. 실시간 Control hook은 설치하지 않는다.

## I. Stage lifecycle

Stage 1은 다음 fresh completed 20개, Stage 2는 그 뒤 추가 30개로 고정한다. Discovery/실제 등록 전 신호를 제외하고 두 stage의 position IDs와 rows hash를 봉인한다. Stage 1 outcome 또는 표본을 바꿔 Stage 2를 재선택할 수 없다. CLI `stage`는 immutable report와 연구 상태만 기록한다.

`DISCOVERY_ONLY → FRESH_DIRECTION_CONFIRMED → PROMISING_CHALLENGER` 또는 `ONLY_NEGATIVE_FILTER`, `REJECT_EARLY`를 지원한다. Stage 3는 별도 immutable longer sample design과 사람의 판단이 필요하고 자동 승격하지 않는다. `ONLY_NEGATIVE_FILTER`를 검증된 robust negative filter로 과장하지 않는다. Control 변경은 `PROMISING_CHALLENGER` 이후에도 별도 승인과 구현 검토가 필요하다. Live 자동 승격은 없다.

## J. Fast rejection

Fresh normalized net<=0, missed profit가 avoided loss를 상쇄함, 한두 tail trade 의존, actual/normalized 방향 역전, 심한 route/size/day confound는 `REJECT_EARLY`다. Coverage/missing policy는 등록 전에 숫자로 고정한다. 자료 부족은 `INSUFFICIENT_COVERAGE`이며 성공으로 승격하지 않는다. Stage 2의 시간 분할 불안정/비용 스트레스 취약은 PROMISING 승격을 막는다.

## K. Economic metrics와 cost stress

affected/included/excluded/unknown, losers avoided/winners missed, avoided loss/missed profit/net effect, expectancy/PF/drawdown/win rate/coverage/return%, actual SOL과 equal 1 SOL normalized PnL을 계산한다. Infinity/undefined PF는 JSON-safe 상태로 구분한다. Top-two removal, chronological halves, route/size dependence를 노출한다.

Recorded paper / incremental 50bps / incremental 100bps+10,000 lamports stress는 sensitivity다. 기존 원장 비용을 또 차감하지 않는다. 미측정 network/tip/adverse execution은 실제 fill 비용으로 가장하지 않고 추가 explicit scenario의 proxy로 표시한다. Capital recycling 시뮬레이션이나 live 실행을 포함하지 않는다.

## L. Automation

기존 checkpoint watcher의 `--record`는 같은 호출에서 최초 checkpoint와 `checkpoint_transition.json`을 외부에 생성한다. Unknown recording이면 보고서 생성도 BLOCKED다. Process stop/start는 watcher가 하지 않는다. 기존 hourly heartbeat의 경로를 이 검증된 도구로 갱신하며 unchanged waiting에는 조용히 유지한다. PC/app offline 중 즉시 실행은 보장하지 않는다.

```powershell
& $py "$tools\scripts\n3_feasibility_checkpoint.py" --runtime $old --amendment "$feasibility\amendment.json" --record --output "$feasibility\checkpoint.json" --transition-output "$feasibility\checkpoint_transition.json"
```

`alpha_batch_pipeline.py --root $new --batch-output $batch --research-output $research --execute --watch --interval 60 --max-polls 1440`은 acceptance 후 등록된 batch만 감시한다. Close 시 freeze→offline join→검증된 import→forensic→discovery proposals까지 처리하고 종료한다. Waiting이 같으면 출력을 반복하지 않으며 poll 예산 소진을 완료로 보고하지 않는다. `--screen-policy`와 `--label-contract`를 선택적으로 공급하되 정책이 없으면 후보를 자동 등록하지 않는다. 현재 이 watcher는 시작하지 않았다.

Batch freeze/import는 재실행·충돌·crash recovery를 지원한다. Shadow/stage report는 별도 외부 consumer/CLI로 자동화할 수 있다. 후보 등록과 Control 적용은 서로 다른 결정이다. 현재 forensic 상주 프로세스나 새 Control은 시작하지 않았다.

## M. Tests

Compile와 전체 suite의 실제 결과는 최종 검증 후 [검증 보고서](ACCELERATED_ALPHA_TRANSITION_VALIDATION.md)에 기록한다. 필수 범위는 A/B checkpoint, original CLOSED 회귀, telemetry epoch/provenance/state cloning, physical separation/natural acceptance/latency, 20/7 freeze+offline join+midnight, max-five multiprocess registry, immutable versioning, Stage 1/2 및 rejection, shadow duplicate와 future leakage, Windows 공백/한글 경로, 기존 N3 회귀다. 모든 state 테스트는 TemporaryDirectory를 사용한다.

## N. Git/runtime isolation

Frozen source/manifest/기존 immutable 증거와 foreign 파일의 baseline hash를 외부에 저장해 완료 시 대조한다. 원래 runtime 소스를 수정하거나 프로세스를 restart하지 않는다. 준비 build에만 research/tooling/tests/docs와 state_store 전용 atomic clone API를 추가한다. monitor/analyzer/executor/risk-manager/strategy threshold 변경은 없다. Main push, history rewrite, 배포와 live 전환은 하지 않는다.

## O. Ready state

`READY`는 양쪽 분기의 **검증된 전환 절차와 offline 도구 준비**를 뜻한다. 현재 runtime activation/acceptance 또는 수익 가능한 Alpha 검증 성공을 뜻하지 않는다. 실제 activation 전 checkpoint 도달, operator stop/maintenance, state continuity, reviewed build, explicit latency/screen policies, natural acceptance가 필요하다. 최초 checkpoint의 일시적 backlog는 당시 기록으로 보존하고 실제 cutover에서 healthy/zero-backlog와 source/target 경계를 재검증한다. 관측되지 않은 가격 trajectory/quote baseline와 cost 요소는 그대로 한계로 기록한다.

Offline 입력은 row/byte 한도를 넘으면 조용히 자르지 않고 BLOCKED로 반환한다. Telemetry wrapper는 stream당 10,000행 및 합계 64MiB, canonical/forensic은 2,000 completed, N3/shadow consumer는 BUY stream당 2,000행이다. 대형 epoch는 별도 검토된 subset/partition으로 처리해야 하며 이 제한이 원본 telemetry retention cap 또는 Control 거래 중지 조건은 아니다.

```text
ACCELERATED_ALPHA_TRANSITION: READY (전체 검증 통과 시)
N3_FEASIBILITY_STATUS: WAITING_FOR_30_OR_7D
TELEMETRY_RUNTIME: NOT_STARTED
LIVE_TRADING: DISABLED
PROJECT_STATUS: PAUSE_STRATEGY_REVIEW
```
