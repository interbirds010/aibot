# Entry telemetry: offline readiness와 N3 종료 후 cutover

2026-10-03 KST audit. **TELEMETRY_NOT_READY**. 이 문서는 활성화 명령 실행 승인이 아니다. 현재 N3/Control을 재시작하거나 현재 checkout을 업데이트하지 않는다. 이번 변경은 테스트·측정 도구·문서뿐이다.

## A. Preflight

| 구분 | checkout / identity |
|---|---|
| 실제 실행 | `C:\Users\user\Documents\aibot-local-paper`, branch `codex/memory-failure-diagnostics`, HEAD `8f0b9429352c9961b895a9f9c0e2d1f3e6b19d05` |
| Telemetry 구현 | `C:\Users\user\Documents\aibot-entry-telemetry`, branch `codex/entry-telemetry`, base HEAD `1c06e60642ab84837835d96e074a46be55e1db84` |
| 원격 시작 상태 | origin/main = telemetry base; isolated ahead/behind 0/0, live 0/1 |

일반 `git fetch origin`은 설정된 삭제된 remote branch ref 때문에 실패했다. 설정을 변경하지 않고 `git fetch origin main`으로 main을 확인했다. 완료 commit은 이 문서/관련 tests만 추가하며 telemetry 구현 commit과 별개다. 최종 HEAD/origin/status는 완료 보고에서 확인한다.

Live foreign 변경: tracked `src/monitor.py`, `tests/test_self_recovery_filters.py`; untracked `.deployed-sha`, local-paper 문서 2개, `scripts/local_paper_runner.py`, Windows runner/feeder tests, `research_tools/`, first-BUY acceptance test. 수정/stage/commit하지 않았다. `legacy_carry_in.json`의 foreign hash inventory와 manifest의 실제 source identity가 일치했다.

Audit 중 actual PIDs monitor 7876, risk 4556, dashboard 3624, N3 43084 및 시작 시각은 기존과 같았다. N3 RUNNING/recorder OK, source identity 일치, WSS SUBSCRIBED, backlog 0. 첫 fresh BUY watcher는 이미 PASS 후 정상 종료했으며 N3 observer는 계속 실행 중이다. 현재 telemetry process는 시작하지 않았다. GitHub Actions 세 workflow는 disabled_manually이며 변경하지 않았다.

## B. Implementation review

실제 코드 `src/research/entry_telemetry.py` 기준 schema 1, queue 16, daemon writer. Import만으로 writer를 시작하지 않지만 candidate `monitor.main()`은 worker를 무조건 시작한다. 현재는 live checkout에 해당 코드가 없어서 disabled다. Candidate monitor를 시험 삼아 시작하는 것도 activation이다.

저장: `data/research/entry_telemetry/rows/<signal_id>.json`, `storage.json`, `health.json`. `state_store.exclusive_file_lock`과 `atomic_write_json`을 사용한다. I/O/직렬화/HEAD+src fingerprint는 writer에 있고 후보 경로는 bounded projection과 `put_nowait`다. Queue/row/storage 제한은 각각 16 / 64 KiB / 4096행 또는 128 MiB. 동일 identity/trade/event/outcome 재시도는 첫 파일을 보존하고 duplicate count를 올린다. 충돌/손상은 보존하고 degrade/drop한다.

Writer 오류 행은 retry하지 않는다. startup 오류 후에도 다음 row를 시도하며 health를 게시한다. 모든 쓰기 자체가 불가능하면 degraded는 메모리에만 있고 health 파일은 갱신되지 못한다. 운영자는 heartbeat stale도 검사해야 한다. Health counter는 session별이고 고정 health 파일은 최신 publisher 상태다. `degraded_event_count`는 첫 오류부터 생성된다. 영구 cumulative drop counter가 아니다.

Coverage는 BUY, analyzer/risk reject, quote fail, RPC skip, OTHER 및 B confirmation 실패다. 모든 원시 discovery/feed event를 기록한다는 보장은 없다. 성공 B confirmation의 세부값은 후속 BUY row에 연결되지 않는다. Provenance는 SHA, src digest, allowlisted config/hash, OS/Python/PID/session/module 초기화 UTC다. 전체 effective config나 OS process 생성 시각은 아니다.

## C. Sample fixtures

`tests/test_entry_telemetry_readiness_fixtures.py`의 실제 capture/finish/_row/_persist를 사용해 정상 BUY, analyzer reject, risk reject, quote fail, RPC skip, optional missing, partial wallet/score를 만들었다. 추가 wallet-rich/bounded stress까지 9개이며 worker/runtime/network는 시작하지 않고 TemporaryDirectory만 쓴다.

Identity/schema/outcome, hash 봉인, timestamps/monotonic order, cap 이전 momentum operand와 할당 점수, retry/wait, quote, wallet lower-bound, 실제 provenance, null/reason 및 Windows 저장 bytes를 검증한다. BUY event_seq는 기존 함수가 반환하지 않아 null이며 receipt trade ID로 이후 원장 join해야 한다. missing 값은 0으로 대체하지 않는다. 합성 fixture는 자연 거래 표본이 아니다.

## D. Leakage audit — 활성화 차단

1. `SECTION_FIELDS["wallet_performance"]`는 `realized_pnl`, `roi`, `wins`, `losses`를 허용한다. `_forbidden`은 realized_pnl/SELL을 거르지 않는다. `scores.raw_components` 내부 synthetic `SELL: {realized_pnl, roi, status}`가 살아남는다.
2. `wallet_performance.snapshot_at`에 signal 이후 시각을 공급해도 비교/거절하지 않는다. 현재 monitor에서는 이 section 값이 null이므로 **현재 실제 미래값 누출을 관찰했다는 주장은 아니다**. Serializer가 잘못된 입력의 누출을 막지 못한다는 offline 재현이다.
3. `_row`와 `_persist`는 predictors/decision/execution_receipt를 같은 물리적 JSON 파일에 저장한다. Top-level key 분리만으로 요청한 물리적 파일 분리를 충족하지 못한다.

winner/loser/exit_reason/MFE/MAE/future/post_entry/final_pnl처럼 알려진 key는 재귀적으로 제외되고 decision freeze 이후 predictor 수정은 차단된다. 그러나 이 부분적 보호로 위 위험이 해소되지 않는다. Entry 이전 5분 시장 `sell_count`는 미래 Control SELL/청산 결과와 다르다. 금지해야 할 미래 exit/settlement payload가 중첩 input으로 들어갈 수 있다는 것이 차단 사유다.

새 tests의 `characterize_*` PASS는 취약 동작 재현 성공이며 안전 통과가 아니다. 기존 `entry-telemetry-2026-10-03.md`의 leakage 설명은 이 추가 audit로 제한된다. 물리적 분리, 엄격한 nested schema, label 차단, 시점 검증을 별도 구현·검증하기 전 activation 금지.

## E. Failure isolation

16개 새 offline tests: directory 자동 생성, permission/row write/Windows atomic replace 실패, malformed final JSON 및 invalid seal, malformed storage, writer exception, serialization TypeError, full queue, health write 실패, stranded partial temp를 주입했다. Drop/degraded/write/conflict counter와 보존 동작을 확인했다. Queue-full에서 실제 monitor wrapper가 Control BUY/REJECT stub을 그대로 실행한다. 기존 monitor/execution tests는 실제 Control 본문과 거래 함수 인자 보존도 검증한다.

ACL을 변경한 실환경 permission 시험이 아닌 fault injection이다. 실패 writer가 정상 Control 거래에 연결되지 않는 구조를 검사한 것이며 실제 실행 latency/네트워크 실패 확률 불변을 증명한 것은 아니다.

## F. Queue / backpressure

2026-10-03 23:01:10 KST 측정: 지난 저장된 **24개 완전하고 overflow/saturation 없는 시간 bucket**의 `analyzer_started + rpc_confirmation_failed` 합은 2170, 평균 90.4167/h (0.0251/s), 최소 21/h, 최대 271/h. UTC 10-02 04:00부터 10-03 13:00 시작 hour까지 비연속 구간이다. 결측 시간은 제외했다. 최근 24개 900초 bucket의 최대는 56건/15분 (0.0622/s). 이는 후보 row 빈도의 proxy이며 중복·미계측 초기 reject·새 envelope 범위 때문에 정확한 telemetry rate는 아니다. `candidate_considered`/시장 poll 횟수 전체를 telemetry row 수로 간주하지 않는다.

TemporaryDirectory에서 실제 `_persist + _publish_health` 100건 평균으로 writer drain 약 **51.82행/s**. 순간 64건 producer 시험은 16건 저장/48건 drop, max depth 16이었다. Full 정책은 기존 queue 유지 + 새 row 즉시 drop + `queue_full` reason/counter이며 거래를 기다리게 하지 않는다.

이론상 빈 queue는 consumer 정지 시 16건을 받는다. 순간 burst에서는 서비스율 보장이 없다. 일정 입력률 λ가 실측 μ=51.82/s보다 크다면 빈 큐 포화 시간은 약 `16/(λ-μ)`초, 입력 중단 뒤 16건 drain은 약 0.309초다. 예시 λ=100/s면 약 0.332초. 이 예시는 설계 계산이며 관측 1초 burst가 아니다. Windows local disk 결과를 VPS/실거래 처리 보장으로 사용하지 않는다.

## G. Row sizes

`tests/entry_telemetry_readiness_measurements.py`가 실제 serializer와 atomic writer를 호출한다. 아래 UTF-8 sealed JSON 크기에 Windows CRLF 2바이트를 포함한 disk bytes를 별도 표기했다.

| Fixture | raw canonical bytes | disk bytes |
|---|---:|---:|
| minimal fixed-schema row | 7611 | 7613 |
| normal BUY | 8569 | 8571 |
| wallet-rich 20-wallet | 10462 | 10464 |
| analyzer reject | 8458 | 8460 |
| bounded stress 128 long wallet IDs | 28957 | 28959 |

5개 designed fixture 평균 disk 12813.4 bytes, 상단 28959 bytes. 실거래 p95는 없다. Wallet-rich는 풍부한 합성 입력, bounded stress는 예산 소진된 truncated 입력이며 실제 최대 시장 population을 뜻하지 않는다. 전체 실제 시장에서의 최대 realistic row는 아직 관측 불가다. Schema 자체 null keys 때문에 minimal도 약 7.6KB다. 긴 identity/문자열/노드 수는 기존 제한을 따르며 hard max는 64KiB다.

## H. Disk growth

십진 MB/GB, row 파일만 계산; fixed health/storage/lock와 파일시스템 allocation/백업 overhead 제외. 아래는 완전한 hour의 관측 빈도를 24시간으로 외삽한 **가정 시나리오**이며 생산 telemetry 예측의 확정치가 아니다.

| Scenario / 근거 | rows/day | bytes/row 가정 | MB/day | GB/30days | 4096행 도달 |
|---|---:|---:|---:|---:|---:|
| low: 최소 21/h, normal BUY size | 504 | 8571 | 4.320 | 0.130 | 8.13일 |
| observed: 90.4167/h, fixture 평균 | 2170 | 12813.4 | 27.805 | 0.834 | 1.89일 |
| high: 최대 271/h, fixture upper | 6504 | 28959 | 188.349 | 5.650 | 0.63일 |

Fixture minimal~upper 범위로 observed는 16.52~62.84MB/day, 0.50~1.89GB/30days다. High는 관측 hour 최대를 하루 지속시키는 보수적 가정이며 수초 burst 상한은 알 수 없다. 4096행 한도가 표의 row size에서는 byte 한도보다 먼저 온다. 64KiB에 근접하는 행이면 128MiB가 먼저 올 수 있다. 현재 구현은 한도 뒤 기록을 중단/drop하므로 계속 이 속도로 30일 저장하지 않는다.

## I. Rotation / retention

현재 rotation/archive 없음. 무한 증가는 4096/128MiB cap으로 막지만 약 이틀 만에 수집이 멈출 수 있어 장기 coverage 계획이 부족하다. 기존 rows를 삭제하거나 budget을 임의 초기화해 회피하지 않는다.

추천은 **epoch별 일별 partition + immutable archive**, partition당 byte/row 한도와 활성 partition만 writer가 사용하도록 별도 구현하는 것이다. 월별 manifest로 각 partition hash/count/time/session 범위와 archive 경로를 연결하고, 중복 검색/epoch join/장애복구를 검증한다. 최소 하나의 완결된 prospective 수집 기간 전체와 이후 연구 재현에 필요한 원본을 보존하며 임의 자동 삭제 기한을 만들지 않는다. Hot disk 압박 시 검증된 다른 저장소 archive로 이동하고 양쪽 hash 검증 전 원본을 지우지 않는다. 현재 파일의 Windows 열린 handle/lock을 확인한 뒤 quiescent partition만 archive한다. 이번에 runtime rotation을 적용하지 않았다.

## J. Restart continuity

실제 별도 Python process 2개를 순차 실행: session UUID 변경, schema 1 유지, 동일 identity duplicate의 기존 bytes 유지, 새 identity append와 두 session provenance를 확인했다. Windows process를 OS exit로 종료한 후 file lock을 재획득하고 stale lock 파일이 ownership이 아님을 검증했다.

JSONL이 아닌 개별 atomic JSON이므로 partial last line truncate recovery는 해당하지 않는다. Partial final JSON은 보존/drop; atomic replace 실패 시 final 미생성/temp 정리; 최근 stranded `.tmp`는 row로 계산하지 않는다. 같은 filename의 오래된 temp만 기존 state_store 정책으로 정리한다. Startup은 실제 파일 개수/크기를 재계산하고 실패한 예약을 회복하지만 **모든 row JSON/hash를 검증하지 않는다**. 일치 identity 재기록 전 다른 손상 row를 놓칠 수 있다. Cutover/restart preflight에 offline 전체 seal/inventory 검증이 필요하다.

Queue는 메모리/daemon이며 process 종료 시 미기록 row가 유실될 수 있다. 기존 Windows runner stop은 TerminateProcess 기반이며 graceful writer drain API가 아니다. Clean stop은 별도 reviewed quiescence/drain 절차가 필요하다. 예전 ledger를 복원해 유실을 보완하지 않는다.

## K. N3 isolation

Frozen cohort `cba13fbb-c702-4b18-bcf7-20b78a8e252f`, start UTC `2026-10-03T01:04:59.425384+00:00`, event_seq 10901, rule hash `b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165` 그대로다. BUY는 양쪽 inclusive 경계를 충족해야 한다. Legacy 9583/9661 제외를 그대로 검증했다.

Telemetry path는 N3 `data/n3_shadow`와 독립이다. 보호 sentinel 테스트에서 N3 manifest/closure/entry와 Control ledger bytes는 변하지 않았다. `src/research/n3_shadow.py`, evaluation 코드와 기존 tests는 frozen base와 동일하다. Monitor의 telemetry context wrapper는 기존 Control 함수 전체(N3 prepare/submit 호출 포함)를 감싸지만 N3 recorder 함수를 교체/wrap하지 않으며 기존 호출 인자를 보존한다. ContextVar 기반 RPC/analyzer 계측도 N3 rule 입력을 재계산하지 않는다.

그러나 **N3 build identity는 HEAD와 src/scripts/requirements/ecosystem 전체**다. Telemetry 추가만으로 frozen source digest가 달라지므로 현재 cohort 동안 activation 절대 금지. Telemetry 자체 src digest는 이보다 좁다. 새 epoch manifest에는 전체 실제 runtime inventory를 별도로 고정해야 한다.

진짜 `closure.json`이 있으면 `persist_capture`는 manifest/build 검사 전에 return한다. Old observer의 `observe_once`는 closure보다 build를 먼저 검사한다. 따라서 종료된 old observer를 새 checkout에서 실행하지 않는다. 기존 observer는 CLOSED 시 자연 종료한다.

## L. Cutover plan — 후속 구현 및 활성화 승인 이후에만

1. 현재 frozen 정책의 자연 hard-cap(28 active KST days 또는 400 completed)에 의해 immutable `data/n3_shadow/closure.json` 및 `final_report.json`이 작성될 때까지 기다린다. 이번 audit에서 final 평가를 수행하거나 종료 marker를 만들지 않는다. 기존 규칙의 최소 gate 도달을 조기 종료로 사용하지 않는다.
2. Closure/final_report content seal, cohort/build/rule/contract/end_event_seq를 검증하고 old observer가 CLOSED 후 자연 종료한 것을 확인한다. N3 전체 artifact와 runtime_sources inventory 보존. 새 build에서 old observer를 재시작하지 않는다.
3. 아래 S의 blocker를 **별도 candidate**에서 수정·검증한다. 현재 candidate의 PM2 feeder에는 live Windows sys/runner 분기가 없다. Foreign monitor patch, runner와 Windows tests를 명시적으로 reconciliation/review한 다음 최종 commit/source identity를 고정한다. Live에 blind git pull/checkout하지 않는다. 새 SHA는 현재 1c06을 그대로 사용할 수 없다.
4. 서비스 ownership registry/creation time을 확인한다. 현재 runner의 `status all`로 unmanaged/duplicate 확인 후, 승인된 quiescence/Control clean-stop 절차로 monitor/risk/dashboard/feeder를 정지한다. 기존 runner `stop all --root ... --python ...`은 강제 종료를 하므로 writer drain 보장으로 해석하지 않는다. 중간 writer/ledger 작업이 없고 OS lock이 해제됨을 확인해야 한다.
5. 모두 정지한 상태에서 state continuity snapshot을 별도 경로에 만들고 SHA-256 manifest를 남긴다. 대상: paper_trades, wallets, wallet_performance, global_metrics, 기존 연구 observation/shadow/coverage/archive 상태, N3 전체 산출물, runner registry. JSON은 읽기 검증, ledger next_event_seq와 position/event ID 보존 확인. 비밀 파일 내용을 로그/manifest에 출력하지 않는다. `.env`를 Git에 넣지 않는다.
6. 최종 승인 candidate를 intended runtime으로 사용한다. Local 실제 foreign runtime source를 보존한 checkout을 사용하고 data 경로 연결/소유권을 사전에 검증한다. Snapshot을 읽는 dashboard가 별도 ledger를 만들지 않게 한다. Compile와 full suite, Windows runner/feeder tests를 해당 최종 build에서 실행한다.
7. 아래 M의 immutable epoch marker를 **첫 monitor 실행 전** state_store lock/atomic API로 작성하고 모든 startup session을 epoch와 연결한다. 이 기능은 현재 구현에 없으므로 추가 구현이 완료되어야 한다. Ledger next_event_seq와 snapshot hash를 다시 확인한다.
8. 기존 안전 Windows runner의 reviewed 최종 버전으로 Control을 한 번 시작한다. Registry/실제 PID creation/executable/arguments/session을 비교하고 duplicate process가 없어야 한다. 기존 config/threshold/sizing/SL/TP 유지.
9. WSS SUBSCRIBED, monitor/risk heartbeat/freshness PASS, dashboard HTTP 200, backlog와 pending task bound 확인. 새 recorder health heartbeat/session/source/config/schema/epoch binding 확인.
10. 아래 N의 자연 candidate/BUY/reject acceptance와 queue/drop/write/degraded/disk 검사를 수행한다. RPC skip은 자연 발생할 때만 검증하며 만들기 위해 장애/거래를 유발하지 않는다.

Abort: closure/final_report 미완료, blocker 남음, snapshot/next_event_seq/source/hash 불일치, unmanaged PID, failed tests, unknown session/epoch, stale health, 손상 row나 store capacity 부족이면 시작하지 않는다. 시작 후 acceptance 실패 시 새 프로세스를 안전하게 정지하고 새 row/ledger/log 증거를 보존한다. 검증한 baseline source로 되돌릴 때 **현재 최신 ledger를 사용**하며 pre-cutover snapshot으로 매매 state를 롤백하지 않는다. 기존 N3는 CLOSED 그대로이며 closure 삭제/새 cohort 등록/전략 변경을 rollback에 섞지 않는다.

## M. Telemetry epoch contract — 아직 구현되지 않은 precondition

제안 위치 `data/research/entry_telemetry/epochs/<telemetry_epoch_id>/manifest.json`와 immutable session association 파일. 현재 serializer/storage layout에는 이 기능이 없다. 실제 binding과 namespace/duplicate/retention migration을 구현하고 테스트해야 하며 문서만으로 충족했다고 판정하지 않는다.

필수: UUID telemetry_epoch_id; start KST 및 같은 순간 UTC; start_event_seq=정지 후 ledger next_event_seq; 최종 build SHA/full source inventory digest; schema version; 실제 allowlisted safe_config 및 config hash/scope; OS/platform/Python; 최초 recorder session ID 및 이후 restart session→동일 epoch 연결; 이전 N3 cohort/closure seal; continuity snapshot inventory. Epoch는 restart마다 새로 만드는 session과 다르다. 새 config/source 변경은 별도 epoch다. Signal time 경계와 session membership를 검증하고 이전 N3 sample 또는 기존 telemetry row를 새 epoch로 재분류하지 않는다.

Predictor/receipt 물리 분리 후 공통 signal_id/epoch_id로 join하고 post-entry receipt 시각/Control trade ID는 predictor 파일에 저장하지 않는다. Outcome metadata도 feature loader 입력에 들어가지 않게 별도 dataset 계약과 tests를 고정한다. Epoch marker만 만드는 것으로 live PID와 row의 join을 추정하지 않는다.

## N. First-runtime acceptance plan

후속 activation 시 별도 읽기 전용 acceptance가 다음을 고정 report로 남겨야 한다. 이번에는 실행하지 않았다.

- 첫 recorded candidate: identity/schema/source/config/epoch/session, timestamp ordering, hash/bytes, 자연 missing reason.
- 첫 BUY: Control trade ID/event_seq 원장 join, 별도 predictor/receipt 파일, entry freeze 전후 시각, duplicate 없음. N3 종료 artifact 변경 없음.
- 첫 analyzer/risk/quote reject: outcome/reason이 별도 metadata에 있고 receipt에 가짜 trade 없음. 실제 Control reject 동일.
- 첫 RPC_SKIPPED: 자연 발생 시 attempt/retry/누락/null 확인. 발생하지 않으면 pending으로 기록.
- Queue depth≤16, new dropped/write_error/conflict/degraded=0을 정상 acceptance 기준으로 삼고 nonzero면 원인을 보고해 acceptance FAIL. Health stale도 FAIL. Counter reset/session 교체를 정상화로 해석하지 않는다.
- 첫 100건 또는 충분한 자연 표본 이후 writer latency/bytes/drain/queue/drop를 offline 측정과 비교한다. 파일 fsync/health append 정상, 용량/retention 여유, restart epoch/session continuity 확인. 실제 기준 시간 예산은 승인된 runtime 요구에서 정하며 현재 synthetic p95를 거래 SLO로 만들지 않는다.

모든 case가 자연 발생할 때만 검사한다. winner/loser/수익률/Alpha/threshold 판단은 포함하지 않는다.

## O. Performance impact

Windows local offline 결과, µs. Serialization은 `_row + canonical + digest`이며 worker `_persist` 전체와 동일 시간으로 해석하지 않는다. Write는 실제 budget reservation/row fsync 및 health fsync를 포함한다.

| 작업 | n | 평균 | median | p95 | max |
|---|---:|---:|---:|---:|---:|
| serialize/hash | 1000 | 122.779 | 111.15 | 201.7 | 392.4 |
| finish/enqueue only | 1000 | 1.326 | 1.1 | 1.8 | 24.6 |
| fixture 전체 capture/marks/projection/enqueue | 1000 | 81.401 | 76.3 | 99.9 | 420.3 |
| persist + health | 100 | 19297.647 | 18880.4 | 23591.8 | 25808.4 |

추가 capture 동기 시간은 작고 디스크는 worker에 격리된다. 그러나 CPU/GIL/fsync 경쟁 및 real quote timeout/Control latency regression을 현재 frozen runtime에서 시험하지 않았다. 1GB VPS 결과도 아니다. 의미 있는 regression 없음을 확정하지 않는다. 재현: 프로젝트 root의 PYTHONPATH로 `python tests/entry_telemetry_readiness_measurements.py --live-root C:\Users\user\Documents\aibot-local-paper`; 출력은 측정 JSON이며 운영 파일은 coverage count/time만 읽는다. 재실행 시 시간/문자열 길이/관측 bucket에 따라 값은 조금 달라진다.

## P. Limitations

True sub-second flow, 전체 uncapped historical whale population, wallet performance snapshot, unavailable pool creation metadata는 null 유지. 60초 bucket/max6 과거 projection으로 10초 price/flow를 복원할 수 없다. Whale 조기 종료 lower-bound를 전체시장 population으로 해석할 수 없다. wallet historical/future performance가 없는 만큼 해당 인과 구분을 후속 Alpha 분석에서 할 수 없다. Signal 이후 데이터 보간/사후 snapshot으로 채우지 않는다. Config scope가 일부라는 사실과 RPC confirmation missing attribution도 dataset 설명에 보존한다.

## Q. Tests

새 fixture 7, failure/restart 16, N3/provenance 5 = 28 tests. 실제 소스/호출 경로와 diff를 root에서 확인했다. `python -m compileall -q src tests` PASS. `python -m unittest discover -s tests -v`는 **745 tests OK / skipped 17**, 27.267초였다. 기존 717/17skip 회귀를 유지하고 새 28개를 추가했다. Linux 전용 15개 및 Windows symlink permission 2개 skip는 성공 검증으로 바꾸지 않는다. 전체 log: `C:\Users\user\AppData\Local\Temp\aibot-entry-telemetry-readiness-full.log`. 실제 activation/실시간 Control latency/서버 테스트는 수행하지 않았다.

## R. Git state

Readiness commit 대상은 이 문서, 기존 telemetry 문서의 audit 주의 문구, 위 세 test 파일, offline measurement helper뿐이다. Production src/scripts/config/deploy/N3 tests 변경 없음. Foreign 파일/data/log/credentials는 포함하지 않는다. Fast-forward push 전에 workflow disabled 상태를 확인하고 최종 live source/foreign hash와 process continuity를 재검증한다. History rewrite 없음. Live HEAD는 계속 8f0b942이고 origin/main이 앞으로 가도 checkout을 변경하지 않는다.

## S. Final verdict

**TELEMETRY_NOT_READY**: 미래 label/raw SELL/future wallet snapshot guard 미충족과 predictor/receipt 물리 분리 부재가 직접 차단 사유다. Epoch 구현/검증, Windows foreign runtime reconciliation, 장기 archive/retention 및 안전 drain/재시작 integrity 절차도 activation 전에 해결해야 한다. 테스트 통과는 현재 동작/격리의 증거이며 readiness 승인이 아니다. 이번 테스트·문서 범위에서 production을 임의 수정하지 않았다.

```text
N3_SHADOW_STATUS: RUNNING 유지
PROJECT_STATUS: PAUSE_STRATEGY_REVIEW
CURRENT_RUNTIME: N3 frozen build 8f0b942 유지
TELEMETRY_RUNTIME: NOT_STARTED
```
