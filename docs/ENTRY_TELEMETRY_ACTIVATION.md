# Entry telemetry split-stream readiness와 activation 계약

2026-10-03 KST. **TELEMETRY_ACTIVATION_READY_WITH_LIMITATIONS**는 아래 검증을 통과한 코드의 준비 상태다. 현재 N3 종료·Control 정지·telemetry activation을 실행했다는 뜻이 아니다. 현재 frozen runtime은 그대로 유지한다.

## A. Preflight

실제 실행 checkout은 `C:\Users\user\Documents\aibot-local-paper`, HEAD `8f0b9429352c9961b895a9f9c0e2d1f3e6b19d05`, branch `codex/memory-failure-diagnostics`다. Candidate는 `C:\Users\user\Documents\aibot-entry-telemetry`, branch `codex/entry-telemetry`, 시작 HEAD/origin/main `7fad7db2bd18a4b223e0d38aded39687da2a1df7`, ahead/behind 0/0, clean이었다. 명시적인 main ref fetch를 사용했다. Git 설정·history는 변경하지 않는다.

기존 live foreign monitor Windows feeder/sys patch, Linux feeder test patch, runner/Windows tests, local-paper 문서, .deployed-sha, research_tools와 first-BUY acceptance test는 보존한다. 필요한 Windows runner/feeder 동작과 관련 tests만 candidate에 이관했다. Live 파일이나 HEAD를 수정하지 않았다. Runtime telemetry `data/research/entry_telemetry`는 존재하지 않는다. Legacy v1 runtime rows 발견 시 epoch 생성은 차단하고 변환하지 않는다.

## B. Previous blockers

7fad7db audit에서 A 동일 물리 파일, B nested label/future-wallet 유입, C epoch/Windows 연결 미완료를 확인했다. 이번 구현은 세 항목을 각각 코드·테스트로 해결한다. 이전 문서의 v1 내용과 취약 동작 characterization을 현재 readiness 승인으로 사용하지 않는다.

## C. New split architecture

`src/research/entry_telemetry.py`의 세 bounded queue(각16)가 실제 serializer/writer를 연결한다. 후보 critical path에는 projection/put_nowait만 있고 JSON 직렬화·Git/source 조회·락·fsync는 startup 또는 background writer에 있다. 한 stream 오류는 다른 stream 큐/파일/용량 예약에 전파되지 않는다.

```text
data/research/entry_telemetry/
  active.json                         # versioned, hash-validated pointer
  epochs/<telemetry_epoch_id>/
    epoch.json                        # immutable marker
    sessions/<session_hash>.json      # immutable process association
    predictors/YYYY-MM-DD/<signal_id>.json
    receipts/YYYY-MM-DD/<signal_id>.json
    outcomes/YYYY-MM-DD/<trade_id>.json
    <stream>/storage.json              # separate bounded reservation
    health/monitor.json
    health/risk-manager.json
```

날짜는 signal UTC를 KST로 정규화한다. JSONL append 대신 state_store 락·원자 교체로 봉인한 개별 JSON을 생성한다. Runtime row는 덮어쓰거나 outcome으로 update하지 않는다. 이미 존재하는 동일 identity는 duplicate, 충돌/손상은 fail-closed다. 날짜 partition이 달라도 epoch/stream당 4096행 또는128MiB 제한은 유지한다. 자동 삭제/무한 rotation은 없다. 한도 도달은 해당 stream drop/degraded이며 기존 원본을 보존한다.

## D. Predictor allowlist

`entry_predictor_schema.py`는 section뿐 아니라 score raw component, allocated component, threshold, wallet contribution, quote leg, pre-signal snapshot의 중첩 구조를 명시한다. 모르는 key·result/position/trade 객체·scalar 자리에 들어온 dict는 serializer 출력에서 제외/null 처리한다. Identity/timestamp/counter/duration/provenance도 명시적 허용 구조로 다시 projection한다. Capture에 직접 오염된 값을 넣은 테스트도 통과했다.

Predictor schema2에는 entry-time raw signal/timing/RPC/retry/wait/quote/score/wallet set/flow/trajectory/pressure 및 provenance가 있다. `decision`/`decision_outcome`은 BUY/REJECT_ANALYZER/REJECT_RISK/QUOTE_FAILED/RPC_SKIPPED/OTHER라는 admission 분류 metadata다. 이는 학습 feature 입력에서 제외해야 한다. 실제 trade_id/event_seq는 predictor에서 null이며 execution_receipt/outcome 객체는 없다. Receipt 없이 reject/RPC skipped/quote 실패 predictor도 독립 valid다.

## E. Leakage guard

Allowlist가 primary defense이고 normalized semantic token 검사가 secondary defense다. realized_pnl/pnl/profit/loss/exit/SELL/closed/outcome/winner/loser/MFE/MAE/horizon/future/final_status 계열과 unknown nested object를 통과시키지 않는다. Entry 이전 `sell_count`, `sells_m5`, `quote_exit_preflight`는 정확한 schema 위치에서만 허용한다. Token substring만으로 실제 pre-entry field를 제거하지 않는다.

Entry decision 시 predictor를 freeze한다. 이후 Control BUY acknowledgment 또는 완료 거래가 predictor를 바꾸지 않는다. 미래 pre-signal snapshot도 signal UTC와 비교해 제외한다. Known counter/timestamp/identity/provenance에 직접 outcome dict를 넣어도 serializer 및 acceptance가 거절한다. 과거/미래 성과를 proxy scalar로 만들어 넣는 새 계산이나 backfill은 하지 않는다.

## F. Wallet snapshot handling

현재 entry-time에 검증된 historical wallet snapshot source가 없으므로 predictor wallet_performance는 항상 `entry_time_snapshot: null`, reason `NO_VERIFIED_ENTRY_TIME_SNAPSHOT_SOURCE`다. Current wallet lookup·future snapshot·realized PnL/ROI/wins/losses 객체를 입력해도 출력하지 않는다. 나중 값으로 채우지 않는다. 향후 snapshot 구현은 timestamp와 source 증거를 갖춘 별도 변경으로 검토해야 한다.

## G. Receipt / outcome separation

Receipt schema1은 실제 Paper BUY commit 이후 trade ID/event_seq/생성 시각을 보존한다. 기존 원장 트랜잭션의 BUY event_seq를 scalar로 받아 lock 밖에서 safe hook으로 전달한다. 새 원장 조회나 schema 변경은 없다. Monitor의 후속 acknowledgment는 최초 생성 시각과 event_seq를 보존한다.

Outcome schema1은 실제 완료 SELL만 기록한다. 기존 원장 lock 안의 이미 읽은 자료에서 최대2048개 event를 역조회하고 최대64개 SELL leg만 명시적으로 복사한다. Cumulative proceeds/entry cost/realized PnL/최종 exit reason은 **outcome 파일에만** 있다. Copy/submit 실패는 SELL commit·포지션 삭제·현금 갱신을 차단하거나 되돌리지 않는다. BUY identity가 조회 범위 밖이면 seq를 추정하지 않고 drop/missing을 보고한다. Epoch 이전 legacy carry-in은 BUY seq와 signal UTC 양쪽으로 제외한다. 중간 SELL은 completed outcome을 만들지 않는다.

## H. Offline join

`src/research/entry_telemetry_join.py`의 읽기 전용 `join_epoch(root, epoch_id)`로 연결한다. 공통 epoch_id+signal_id가 predictor/receipt를 연결하고, receipt의 epoch_id+trade_id가 완료 outcome을 연결한다. Runtime에는 이 join을 호출하거나 predictor를 수정하는 코드가 없다.

Hash/schema/epoch/build/mint/route/family/signal UTC와 BUY seq가 일치해야 한다. 중복 signal 또는 여러 signal에 연결된 trade ID는 거절한다. Reject/RPC predictor-only는 정상이며 BUY missing receipt와 orphan outcome은 명시한다. Read-only join에는 epoch marker 검증이 별도로 필요하며 acceptance 도구가 이를 함께 수행한다.

## I. Epoch contract

`src/research/entry_telemetry_epoch.py`는 explicit activation 직전 immutable UUID marker를 만든다. Marker에는 build SHA 및 전체 src/scripts/requirements/ecosystem inventory, safe config/hash, stream schemas2/1/1, start_event_seq, 같은 순간 UTC/KST, OS/platform, creator PID/session, 이전 N3 closure seal과 continuity snapshot ID가 있다. Runtime PID/생성 시각/session은 별도 immutable binding에 등록한다. Epoch는 재시작에도 유지되고 새 activation은 새 UUID를 만든다. Active pointer만 state_store version/hash 방식으로 바뀐다.

Writer startup은 현재 N3 CLOSED/closure/final_report 일치, build/config/schema/platform, 실제 provenance Git/config를 동기 검증한다. Missing epoch·Git identity·불일치이면 telemetry만 DISABLED이고 Control은 원래 실행 경로를 유지한다. 첫 신호를 받기 전에 초기화하여 background startup race를 피한다. Row source digest는 검증된 전체 epoch inventory digest와 scope를 사용한다.

Config fingerprint는 실제 monitor 상수9개의 명시적 비밀 제외 subset이다. 전체 .env fingerprint가 아니다. API key/RPC URL/credential을 저장하지 않는다. Snapshot source digest가 effective secret configuration까지 검증한다고 주장하지 않는다. Runtime config를 바꿀 경우 별도 검토와 epoch를 요구한다.

## J. Windows cutover

이번에 실행한 것은 tests뿐이다. 아래 명령은 **N3 자연 종료 후 별도 activation 작업에서만** 사용한다.

1. Frozen N3 hard-cap에 의해 closure.json/final_report.json이 생성되고 observer가 CLOSED 후 자연 종료했는지 확인한다. 현재 작업에서 평가/marker 생성/강제 종료를 하지 않는다. Closure/cohort/hash/end_seq 일치가 tool precondition이다.
2. 현재 frozen8f0에는 cooperative handler가 없으므로 처음에는 기존 owned Windows runner의 승인된 종료 절차가 필요하다. Candidate의 clean-stop을 old process에 보내면 거절한다. 강제 fallback을 숨기지 않는다. N3 종료 이후, 기존 `local_paper_runner.py status all`로 PID/creation ownership을 검증하고 operator가 별도 승인된 stop을 수행한다. 모든 writer/PID 종료와 원장 정상성을 확인하기 전 snapshot/activation을 진행하지 않는다.
3. Candidate CLI `python scripts/entry_telemetry_cutover.py snapshot --root <runtime-root> --execute`는 모든 runtime PID 종료를 다시 확인하고 원장/연구/N3 JSON의 실제 byte backup과 SHA-256 manifest를 만든다. 1MiB streaming, 최대256MiB/file·512MiB total·20000 files; 초과는 BLOCKED다. Current state와 backup hash 모두 검증한다. Secret files는 복사/출력하지 않는다.
4. Foreign 변경의 별도 복구 가능 backup을 확보한 뒤 reviewed 최종 telemetry commit으로 checkout한다. 현재 live dirty monitor에 blind pull/checkout하지 않는다. Windows feeder/runner/tests가 candidate에 이미 이관됐는지 diff를 대조한다. 필요하면 **이 미래 단계에서만** foreign 관련 경로를 명시한 Git stash로 보존하고 final commit으로 전환한다. 이미 이관된 patch를 pop해 중복 적용하지 않는다. State snapshot은 거래 복원용 임의 reset으로 사용하지 않는다.
5. 최종 commit에서 `python -m compileall -q src tests scripts`, `python -m unittest discover -s tests -v`를 실행한다. 실제 deployment/source inventory와 config를 고정한다.
6. `python scripts/entry_telemetry_cutover.py create-epoch --root <runtime-root> --execute`, 이어 `validate`를 실행한다. Restart는 같은 epoch를 쓰며 별도 activation만 `--new-activation`을 사용한다. State continuity나 N3 증거가 달라지면 거절한다.
7. `python scripts/entry_telemetry_cutover.py resume --root <runtime-root> --python <verified-python> --execute`는 정지·snapshot·N3 closure·epoch를 재검증하고 기존 hidden Windows runner로 monitor/risk/dashboard를 한 번 시작한다. Duplicate/unmanaged/ambiguous module PID가 있으면 거절한다. WSS/heartbeat/freshness/backlog/dashboard와 두 role health를 확인한다.
8. 새 build 이후의 종료는 `clean-stop --execute`를 쓴다. PID creation/session이 맞는 immutable request를 보내 신규 discovery를 닫고 등록된 기존 신호/진행 중 청산을 완료한다. 큐 drain acknowledgment와 실제 process exit가 모두 필요하다. Timeout이면 프로세스를 강제로 죽이지 않고 cutover를 거절한다. Dashboard만 기존 소유 runner로 종료한다.

Path 비교는 Windows case-insensitive이고 registry launcher/child PID도 inventory에 포함한다. Cutover 자신의 현재 PID와 생성 시각·executable·전체 CLI argv가 일치하는 직접 venv launcher만 제외한다. cwd가 불분명한 동일 core `-m src...` host process는 보수적으로 차단한다. 다른 checkout의 모듈 실행이 차단 원인일 수 있으며 ownership을 확인해야 한다.

## K. Failure isolation

Predictor/receipt/outcome에 각각 queue/drop/write/error/duplicate/conflict counter와 별도 lock/storage budget이 있다. 한 stream 실패 뒤 나머지 stream이 저장되는 테스트가 있다. Health는 bounded monitor/risk-manager 파일로 분리하고 중첩 counter snapshot을 lock 안에서 복사한다. Health write 자체가 실패하면 memory degraded와 stale heartbeat로 드러난다.

Missing directory·permission·serialization·malformed JSON/seal·atomic replace·queue full·storage cap·writer exception을 임시 경로에서 검증한다. 재시작은 실제 Windows subprocess/session/OS lock으로 검사한다. JSONL partial-line 복구는 구조상 해당하지 않으며 partial final JSON 보존/drop 및 stranded temp/atomic replace 경계를 검증한다. Startup storage scan은 개수/bytes를 재구성하고 모든 seal을 proactively 확인하지는 않는다. Acceptance가 전체 stream seal을 검사한다.

## L. Performance

2026-10-03 23:31:46 KST Windows 임시 저장소 측정. Candidate source는 수정 중 HEAD7fad7db였으므로 값은 final source 실전 SLO가 아니다. 합성 1000건 mean/p95(µs): predictor finish/enqueue1.494/2.4; receipt queue put0.935/1.3; actual outcome capture/enqueue11.315/14.8; 전체 normal fixture capture/mark/projection/enqueue103.276/132.2. Writer serializer/hash는 predictor622.783/1025.9, outcome35.468/42.0으로 critical path 밖이다. 실제 split BUY 두 파일+health fsync100건 mean34.517ms/p9542.0ms, 약28.97 candidate/s. Producer64 burst에서 depth16 유지,17 처리/47drop이었다.

Worker는 predictor를 최대0.25초 기다린 뒤 receipt/outcome 각1개를 소비한다. Predictor가 없는 outcome-only rate는 디스크 비용 제외 최대약4/s라서 fsync-only throughput을 전체 서비스율로 간주하지 않는다. Queue drain timeout은 실패하면 ack하지 않는다. CPU/GIL/disk 경합에 따른 실전 Control latency regression은 current runtime에 instrumentation을 넣지 않아 미검증이며 activation 후 자연 수집으로 확인한다.

Fixture disk bytes: minimal7638, normal predictor8550+receipt1536, wallet-rich10442+1538, reject8574, bounded stress46523+1544. 합성 fixture upper를 production p95로 사용하지 않는다. 과거 완전24hour proxy 평균90.4167/h·최소21/h·최대271/h이며 실제 new telemetry rate와 같지 않을 수 있다. Normal predictor+BUY receipt를 모든2170/day에 가정하면 약21.89MB/day(0.657GB/30days), completed outcome bytes는 별도 추가다. 초단위 burst는 관측 근거가 없다.

4096행/stream/epoch 제한이면 현재 proxy에서 약1.89일에 predictor cap이 올 수 있다. 자동 archive/새 무한 epoch 생성을 하지 않는다. 장기 수집은 중지·봉인·검증된 archive 및 새 명시적 epoch 계획이 필요하다. 삭제 없이 연구 기간 전체 원본을 보존한다. 이 retention 한계와 null sub-second flow/전체 whale population/wallet performance/pool metadata는 READY_WITH_LIMITATIONS의 이유다.

## M. N3 isolation

Cohort cba13fbb-c702-4b18-bcf7-20b78a8e252f, start UTC2026-10-03T01:04:59.425384+00:00, seq10901, rule hash b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165 그대로다. Frozen dual inclusive eligibility와 legacy9583/9661 제외를 유지한다. N3 module/evaluation/contract는 변경하지 않았다. Monitor prepare/submit 인자도 유지한다.

현재 live HEAD/source/foreign hash, PID/start time와 observer 상태를 작업 전후 비교한다. Candidate 추가로 N3 build digest가 바뀌므로 현재 cohort 중에는 checkout/activation 절대 금지. Telemetry writes는 다른 namespace이며 N3 sentinel/Control 원장 불변을 테스트했다. Old observer를 새 build에서 다시 시작하지 않는다.

## N. Tests / first runtime acceptance

실제 serializer fixtures9개, leakage scalar/nested/unknown/future-wallet/future-history, 세 파일 불변·duplicate·offline join·missing identity·epoch mismatch·Windows path/reopen/process restart/lock·stream 장애·Control BUY/SELL 의미·clean-stop drain을 검증했다. `python -m compileall -q src tests scripts` PASS, `python -m unittest discover -s tests -v`는 **864 tests OK / skipped17**, 36.001초다. 기존 baseline745/17skip 회귀를 유지했다. Log: `C:\Users\user\AppData\Local\Temp\aibot-split-telemetry-full.log`. 최초 전체 실행에서 Windows feeder의 기존 Linux PM2 기대값과 새 pointer의 lock 진단 operation 누락을 확인하고 수정한 뒤 전체 재실행했다. Linux 전용15개/Windows symlink 권한2개 skip는 성공으로 바꾸지 않는다.

`python scripts/entry_telemetry_acceptance.py --root <runtime-root> --output <report.json>`은 활성화 후 읽기 전용 확인용이다. 첫 predictor/rejected/RPC-skipped/BUY receipt/completed outcome 다섯 case를 독립 검사한다. 미발생 자연 case는 PENDING이다. 올바른 epoch/schema/build/config/session, hash, nested allowlist, duplicate, 분리 파일, actual Control BUY/최종 SELL identity를 확인한다. 두 role health stale90초 또는 drop/error/conflict/duplicate/degraded는 FAIL이다. 모든 case와 offline join이 맞아야 전체 PASS다. 결과 파일만 state_store API로 저장하며 전략/Alpha 평가를 하지 않는다.

## O. Changed files

Core: entry_telemetry.py, new entry_predictor_schema.py / entry_telemetry_epoch.py / entry_telemetry_join.py. Minimal integration: monitor.py, risk_manager.py. Tools: new entry_telemetry_cutover.py / entry_telemetry_acceptance.py, reconciled local_paper_runner.py. 관련 recorder/fixture/failure/monitor/isolation tests와 new epoch/join/outcome/clean-stop/acceptance/inventory/Windows runner tests, measurement helper 및 이 문서/초기 구현 문서 안내만 변경한다. Strategy/threshold/sizing/SL/TP, shared ledger schema, dependency/deploy/N3 rules/AGENTS는 변경하지 않는다.

## P. Git state

관련 code/tests/docs만 commit/push한다. Live foreign 파일은 변경·stage하지 않는다. Main은 fast-forward이며 history rewrite 없음. Remote workflow3개 disabled_manually 확인 후 그대로 둔다. 현재 live checkout HEAD8f0는 main보다 뒤에 남는 것이 의도된 상태다. 최종 SHA/origin/ahead/status와 전체 검증 결과는 완료 응답에 기록한다. Runtime/서버 배포는 하지 않는다.

## Q. Final readiness verdict / abort

**TELEMETRY_ACTIVATION_READY_WITH_LIMITATIONS**: predictor/receipt/outcome 물리적 분리, recursive leakage guard, current/future wallet 차단, immutable epoch+restart association, Windows cutover tools, stream/Control failure isolation, N3 isolation을 코드·테스트로 확인했다. 미발생 자연 runtime acceptance, 초기 oldbuild 수동 owned-stop precondition, retention cap와 unavailable input, production/VPS latency 검증은 명시적으로 남는다.

Abort: N3 closure/finalreport 미완료, PID 살아 있음/소유 불명, state 또는 byte backup 불일치, foreign patch 미이관, 실패한 tests, unknown/mismatched epoch/session/build/config/schema, corrupt stream, stale/degraded health, 저장 한도면 activation 금지. Activation 후 실패하면 새 서비스/row/log/current ledger 증거를 보존하고 검증된 baseline source로 재시작하더라도 **최신 거래 원장**을 쓴다. Pre-cutover snapshot으로 이후 거래를 지우지 않는다. N3 closure 삭제/재등록·강제 청산·전략 변경은 rollback에 포함하지 않는다.

```text
N3_SHADOW_STATUS: RUNNING 유지
PROJECT_STATUS: PAUSE_STRATEGY_REVIEW
CURRENT_RUNTIME: frozen 8f0b942 유지
TELEMETRY_RUNTIME: NOT_STARTED
```
