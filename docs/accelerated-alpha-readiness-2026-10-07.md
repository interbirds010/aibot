# Accelerated Alpha Research 준비 보고

2026-10-07 KST. 현재 N3는 완료 17건, valid hit 0건, valid non-hit 17건, active KST days 4일, recording OK, backlog 0이다. 이번 작업에서 N3 경제 성과를 판정하지 않았다.

## A. Current research bottleneck

Fresh N3가 아직 한 번도 발동하지 않은 상황에서는 hit 40건을 포함한 원래 큰 표본 gate까지 기다리는 시간이 길어질 수 있다. 이는 아직 Alpha 또는 filter 실패의 증거가 아니며, fresh regime에서 실험 feasibility/coverage가 낮을 가능성이다. Paper 연구는 한 challenger의 최종 판단을 기다리는 동안에도 별도 discovery와 미래 shadow 검증을 준비할 수 있다.

## B. N3 feasibility amendment

[별도 amendment 문서](N3_FEASIBILITY_AMENDMENT_2026-10-07.md)를 작성하고 외부 `C:\Users\user\Documents\aibot-alpha-research\n3-feasibility\amendment.json`을 봉인했다. 채택 시각은 2026-10-07 **10:29:43 KST**다. 이미 완료 17건·hit 0건을 관찰한 뒤 새 기준을 정했다는 disclosure와 당시 count/cursor를 포함한다.

Gate는 누적 completed >=30 **OR** active KST days >=7이다. Hit <=1은 `N3_LOW_FRESH_COVERAGE` / `LOW_COVERAGE_IN_FRESH_REGIME`, hit >=2는 `N3_CONTINUE_PROSPECTIVE_VALIDATION`이다. Recording 이상/unknown이면 feasibility 결론을 보류한다. PnL·PF·expectancy·winner/loser 경제 지표를 checkpoint 계산에 사용하지 않는다.

원래 manifest의 200/40/40/20/14 gate와 hard cap, cohort/rule/기존 결과를 보존했다. 현재 `WAITING_FOR_30_OR_7D`이며 checkpoint artifact는 아직 만들지 않았다. Amendment hash는 `0916fdfa68d12039be10fc4b468428add40b8149aad313a7811b3ca6c574681d`다.

## C. Accelerated batch loop

[Protocol v1](ACCELERATED_ALPHA_RESEARCH_PROTOCOL.md)의 기본 batch는 20 completed OR 7 active KST signal/entry days다. Offline canonical row 검증 → dataset freeze → non-overlapping BATCH_001/BATCH_002 → forensic/comparison → bounded candidate proposals 순서로 처리한다.

Initial discovery origin, input hash, immutable row/batch hash와 completion/discovery boundary를 보존한다. 늦게 청산된 거래는 다음 batch로 들어가고 과거 batch를 재분할하지 않는다. 재실행/동시 실행/일부 write 후 crash에 대해 같은 내용만 복구한다. 변경된 과거 입력은 차단한다.

현재 외부 `C:\Users\user\Documents\aibot-alpha-research\batches`에 17건 initial discovery input/origin만 저장했다. Batch는 열려 있고 보고서·proposal·등록 후보는 0이다. Offline 입력은 2,000 completed 단위로 bounded이며 매 poll마다 전체 dataset 사본을 저장하지 않는다. 이 한도는 원본 retention 정책이나 거래 중단 기준이 아니다.

## D. Winner forensic

모든 winner를 손실 거래와 함께 분석하도록 구현했다. 원래 cost, actual SOL, return %, equal-notional return, holding duration, 사용 가능한 entry features와 missing strata를 제공한다. 실제 timestamped trajectory가 있다면 sampled TP time·MAE/MFE와 explicit label contract를 사용할 수 있다.

Fast TP, 큰 winner, long-hold, liquidity/flow 변화와 acceleration 질문을 포함하지만 자료가 없으면 UNKNOWN이다. 현재 N3에는 풍부한 trajectory/liquidity/flow/wallet telemetry가 없으므로 이를 복원했다고 주장하지 않는다.

## E. Loser forensic

Loss magnitude와 rapid SL/immediate fall/reversal/near-TP/sideways/long-hold label contract를 정의했다. Threshold는 explicit positive finite 값으로 공급하고, SL label에는 exit reason도 필요하다. 자료나 contract가 없으면 UNKNOWN이다. Holding duration은 실제 entry/close에서 계산한다.

Winner/loser entry-time feature 분포와 coverage/missing reason을 함께 비교한다. 현재 wallet 또는 시장 값을 과거 entry feature로 덧붙이지 않는다. Loss count만으로 후보가 성공했다고 처리하지 않는다.

## F. False Momentum research

False Momentum은 사후 trajectory/exit에 관한 label로만 취급한다. Entry 이후 reversal·SL label을 entry 이전 quote/timing/liquidity/flow/wallet combination/pressure/age와 연결하는 질문을 만든다. Outcome label, MAE/MFE, holding/PnL을 predictor에 넣지 않는다.

Current N3 adapter는 실제 snapshot의 analysis/preflight duration·quote age만 가져온다. Future telemetry-derived canonical input은 더 넓은 entry projection을 담을 수 있으며 family별 고정 feature 목록에 갇히지 않는다. Runtime telemetry join에서 canonical dataset을 만드는 production adapter는 다음 integration 범위다.

## G. Multi-challenger architecture

하나의 Control row에 최대 5개 active challenger rule을 shadow 평가하는 API를 구현했다. Would-enter/would-skip/classification만 반환하며 추가 BUY/SELL이나 sizing/exit 변경은 없다. Classification만 있는 후보는 경제적 counterfactual을 주장하지 않는다.

Definition은 immutable rule/hash, candidate ID/version, creation batch, evidence, future start sequence/timestamp를 가진다. Source timestamp가 decision 이후이면 거절하고 decision 시각에 이미 고정된 projection은 허용한다. Rule 수정은 새 버전이 필요하며 start boundary 전 signal은 검증 표본에서 제외한다.

등록 시 registry를 잠그고 최신 상태를 다시 읽어 max-five를 보장한다. Offline refresh는 최대 5개 ranked **proposal**만 만들고 실제 후보 등록은 explicit API/CLI다. 이번에는 0개를 등록했다. Real-time telemetry shadow hook은 frozen runtime에 설치하지 않았다.

## H. Candidate lifecycle

DISCOVERY_ONLY → FRESH_DIRECTION_CONFIRMED → PROMISING_CHALLENGER → ROBUST_PAPER_ALPHA 또는 ONLY_NEGATIVE_FILTER의 evidence history를 기록한다. REJECT_EARLY, INSUFFICIENT_COVERAGE와 retirement도 보존한다. 사람의 명시적 decision evidence와 sample gate 없이 승격하지 않는다.

Discovery positive point estimate만으로 validated/Alpha 상태를 만들지 않는다. Numerical missing/coverage policy는 등록 전에 고정해야 하며, 미정이면 fresh direction confirmation도 보류한다. 실제 Control 변경은 도구가 하지 않는다.

## I. Stage 1/2/3 validation

Stage 1은 다음 fresh 20 completed, Stage 2는 그 뒤 추가 30 completed로 v1을 고정했다. 두 stage의 position set은 겹치지 않는다. Creation batch/discovery positions, old signals, event/timestamp boundary 전 거래를 제외한다.

Normalized direction reversal/기회비용 역전/한두 거래 의존을 조기 폐기 대상으로 처리한다. Stage 2가 양수여도 temporal halves 불안정 또는 cost stress 취약이면 PROMISING으로 올리지 않는다. 개선된 retained strategy도 손실이면 ONLY_NEGATIVE_FILTER로 구분한다.

Stage 3는 별도의 immutable longer design을 사전 등록하고 human evidence를 요구한다. 현재 evaluator가 자동 ROBUST 또는 live-ready 판정을 내리지는 않는다.

## J. Economic evaluation

Included/excluded/unknown, winners/losers retained/excluded, avoided loss, missed profit, net effect, expectancy, PF/win rate, normalized/actual return과 drawdown을 계산하는 offline API를 구현했다. Equal 1 SOL notional과 actual SOL을 함께 제공해 sizing에 따른 부호/집중도 차이를 드러낸다.

Unknown을 조용히 skip/include로 바꾸지 않는다. Profit factor의 zero loss/infinity/undefined는 JSON-safe state로 구분한다. Top-2 removal과 chronological halves를 공개하며 avoided loss - missed profit를 사용한다.

Original recorded-paper/50bps/100bps+10,000 lamports stress를 sensitivity로 재사용한다. 이미 반영된 비용을 재가산하지 않는다. 미계측 network/tip/adverse-execution 요소는 정확한 실거래 비용처럼 보이지 않게 명시한다. 새로운 fill/capital recycling 시뮬레이션은 포함하지 않는다.

## K. Telemetry transition

Low-coverage checkpoint 결과 보고 후 `86d7ddc` telemetry activation을 우선 다음 작업으로 권고하는 전환안을 작성했다. 실제 전환은 하지 않았다.

기존 cutover는 원래 N3 CLOSED+matching closure/final_report를 요구한다. Feasibility 조기 종료를 이 상태처럼 위조하지 않는다. 별도 사용자 결정 후 amendment resolution/종료 경계, observer stop, supervisor maintenance, state/foreign backup, explicit amendment closure 지원과 runtime acceptance/latency 검증이 필요하다. Open position 강제 청산이나 과거 snapshot rollback은 하지 않는다.

Telemetry activated 이후 rejected/RPC-skipped/quote-failed population을 별도 coverage/selection-bias 분석에 포함한다. 미실행 signal에 가상 결과를 만들어 completed economics에 섞지 않는다.

## L. Automation

현재 채팅에 **1시간 간격** `N3 feasibility 30건·7일 checkpoint` heartbeat를 등록했다. ID는 `n3-feasibility-30-7-checkpoint`, ACTIVE다. 대기 상태가 같으면 조용히 종료하며 최초 도달·recording 이상/BLOCKED·실행 실패만 알린다. 도달 시 count/day/hit/non-hit/coverage/recorder health만 보고하고 경제 분석이나 전환은 하지 않는다.

Offline refresh CLI는 batch close 때 immutable forensic report와 ranked proposals까지 자동 생성할 수 있다. `--watch --execute` 반복 모드도 제공하지만 현재 frozen runtime을 대상으로 상주 forensic watcher를 등록하지 않았다. Checkpoint 자동화와 forensic execution 경로를 분리했다.

Managed worktree와 기존 venv 경로를 보존해야 자동 점검이 실행된다. PC/앱 offline 중 즉시 검사/알림을 보장하지 않는다. Worktree 제거 전에 저장된 automation path를 검토한 새 위치로 변경해야 한다.

## M. Tests

`python -m compileall -q src tests`와 최종 `python -m unittest discover -s tests -v`가 통과했다. 최종 전체 suite는 **961개, 157.835초, OK (skipped=17)**다. 건너뛴 검증을 통과로 계산하지 않았다. 결과는 외부 `C:\Users\user\Documents\aibot-alpha-research\full-tests.log` 및 `full-tests-final.log`에 보존한다. Targeted 검증은 core 19개, forensic 22개, feasibility 15개, CLI 8개로 총 64개 통과했다. 첫 전체 suite는 958개 OK (skipped=17)였으며, 봉인 policy 및 미등록 stage 경로 보완 이후 최종 전체 suite를 다시 실행했다.

범위는 20/7 및 30/7 경계, hit1/2 판정, 경제 필드 접근 없는 checkpoint, source/cohort/hash corruption, no-write status, immutable conflict/idempotence, late completion, crash recovery, 실제 multiprocess max-five/batch race, predictor leakage, future-only stage 분리, opportunity cost/size normalization/cost stress, missing/UNKNOWN, altered candidate policy seal을 포함한다.

## N. Git/runtime isolation

Fetch 후 origin/main은 `86d7ddc96fb6b8584df26bf1ef2a964e83454480`이었다. 별도 worktree `C:\Users\user\.codex\worktrees\accelerated-alpha\solana-ai-bot`, branch `codex/accelerated-alpha-research`에서만 구현했다. 기존 harness worktree와 원래 checkout의 foreign 변경은 보존한다. Commit/push/GitHub Actions 배포는 하지 않았다.

변경은 새 research module 3개, offline CLI 2개, test 4개, 문서 3개다. 기존 monitor/analyzer/executor/risk-manager/N3 source/tests/manifest는 변경하지 않았다. 일반 git diff --stat는 새 파일이 untracked여서 비어 있고 status/new-file inventory 및 no-index diff로 실제 범위를 검토했다.

| 종류 | 새 파일 |
|---|---|
| Modules | `src/research/accelerated_alpha.py`, `src/research/alpha_batch_forensics.py`, `src/research/n3_feasibility.py` |
| CLI | `scripts/accelerated_alpha_research.py`, `scripts/n3_feasibility_checkpoint.py` |
| Tests | `tests/test_accelerated_alpha.py`, `tests/test_alpha_batch_forensics.py`, `tests/test_n3_feasibility.py`, `tests/test_accelerated_alpha_research_cli.py` |
| Docs | `docs/ACCELERATED_ALPHA_RESEARCH_PROTOCOL.md`, `docs/N3_FEASIBILITY_AMENDMENT_2026-10-07.md`, 이 보고서 |

Frozen source 56개가 계속 matching하고 manifest raw SHA256은 `f162040a0d90d905b4b4aa16204262c626e9d15bd496f7439ee865fae3645390`으로 유지됐다. 이전 baseline의 immutable N3 기록 43개도 bytes hash 변경이 0개였다. 현재 서비스 PID monitor31284/risk32640/dashboard31392/observer20956를 종료하거나 재시작하지 않았다. Cohort/rule/threshold/strategy/sizing/SLTP/cursor/telemetry 상태를 바꾸지 않았다.

## O. Recommended next action

N3 feasibility checkpoint를 기다리는 동안 batch/challenger 도구와 protocol을 검토한다. Checkpoint 도달 시 오직 coverage/recording 결과를 먼저 보고한다. Low coverage이면 200건까지 무조건 기다리는 대신 별도 사용자 전환 결정 후 telemetry activation/acceptance와 wider predictor population 연구로 진행한다.

현재 도구는 offline 연구 iteration을 실행할 수 있는 상태다. 풍부한 winner/loser feature를 확보하려면 telemetry activation 및 dataset adapter/runtime fan-out integration이 필요하다. 이 보고서는 N3 실패나 새 전략의 수익성 검증 성공을 선언하지 않는다.

```text
N3_FEASIBILITY_STATUS: WAITING_FOR_30_OR_7D
ALPHA_RESEARCH_MODE: ACCELERATED_PAPER_ITERATION_READY
PROJECT_STATUS: PAUSE_STRATEGY_REVIEW
LIVE_TRADING: DISABLED
```
