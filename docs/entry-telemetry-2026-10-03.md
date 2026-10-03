# Entry-time raw telemetry: 구현과 N3 격리

후속 offline audit: [ENTRY_TELEMETRY_ACTIVATION.md](ENTRY_TELEMETRY_ACTIVATION.md)를 우선 적용한다. 현재 판정은 **TELEMETRY_NOT_READY**다. 아래 G의 보호는 알려진 key/freeze에 한정되며, realized_pnl/중첩 SELL/future wallet snapshot guard와 predictor/receipt 물리적 파일 분리는 충족하지 않는다. 기존 구현과 runtime은 이 audit에서 변경하지 않았다.

## A. Preflight

지정 실행 checkout은 `C:\Users\user\Documents\aibot-local-paper`이다. 시작 HEAD/origin/main은 `8f0b9429352c9961b895a9f9c0e2d1f3e6b19d05`, branch `codex/memory-failure-diagnostics`, ahead/behind 0/0이었다. main ref를 명시적으로 fetch했다.

기존 foreign 변경은 monitor의 Windows feeder/sys import, test_self_recovery_filters와 기존 local-paper 문서/runner/Windows 테스트/.deployed-sha/first-BUY watcher다. 어떤 항목도 이 작업에 stage하거나 수정하지 않는다. 실행 소스 inventory와 기존 foreign 파일 해시를 확인한다.

N3는 HEAD와 src/scripts 전체 소스 해시를 검증하므로 실행 checkout에서 코드나 HEAD를 변경하면 observer가 BLOCKED된다. 따라서 같은 base commit의 별도 Git worktree `C:\Users\user\Documents\aibot-entry-telemetry`, branch `codex/entry-telemetry`에서만 구현한다. 기존 Control 프로세스, N3 marker/observer/rule/contract와 상태 원장은 변경하지 않는다.

## B. Existing data gaps confirmed

코드의 현재 수집·저장 경로를 확인했으며 거래 성과를 분석하지 않았다.

- Safety 점수는 실제 35/30/35 배점 합으로 최대 100이다. 별도 cap 함수가 있는 것이 아니다. 기존 공개 report는 개발자 보유율·LP·유동성 값을 소수 둘째 자리로 표시하며 실제 배점 구성과 반올림 전 입력을 별도 저장하지 않았다.
- Momentum은 volume operand를 60점, net-buy operand를 40점으로 제한한다. 최종 점수만으로는 포화 전 operand를 구분할 수 없다.
- B whale 확인은 이미 얻은 transaction에서 지갑별 최대 기여액을 모으고 3개 이상이면 탐색을 조기 종료한다. `len(whales)`는 관찰된 count이며 전체 시장 whale count가 아니다.
- 기존 pre-signal history는 최대 6개, 60초 bucket, 900초 TTL이다. 정확한 5/10/20/30초 flow나 10/30초 price를 복원할 수 없다. 기존 5분 거래량도 buy/sell volume으로 분해할 근거가 없다.
- entry 경로는 wallet performance snapshot이나 내부 원장 admission guard의 전체 상태를 이미 제공하지 않는다. 새 파일 조회나 사후 wallet 값으로 보완하지 않는다.

## C. Telemetry architecture

`src/research/entry_telemetry.py`는 ContextVar로 후보별 작은 capture를 연결한다. `src/monitor.py`가 시작할 때만 daemon writer를 만든다. 후보 처리 경로는 작은 in-memory projection과 `put_nowait`만 수행한다. 직렬화, Git/source fingerprint, 파일 락과 디스크 쓰기는 background writer가 담당한다.

대상 경로는 `data/research/entry_telemetry/`이며 N3 sidecar와 분리한다.

- `rows/<signal_id>.json`: content hash를 봉인한 append-only 개별 기록
- `storage.json`: 공유 락 안에서 version을 증가시키는 저장 용량 예약 상태
- `health.json`: 공유 락 안에서 갱신하는 최근 publisher의 process/session health

일반 JSON append나 자체 임시 락을 사용하지 않고 state_store의 파일 락과 atomic_write_json을 사용한다. 이미 존재하는 row는 덮어쓰지 않는다. 동일 후보 identity/receipt/outcome 재시도는 첫 기록을 보존하고 duplicate counter를 증가시킨다. 다른 trade identity/outcome이나 손상된 hash는 conflict/degraded 처리한다.

## D. Schema v1

| 영역 | 의미 |
|---|---|
| identity | mint, route/family, upstream UTC, source wallet/signature SHA-256, deterministic signal_id |
| predictors.timestamps | 실제 wall UTC와 process monotonic ns. 이전 signal의 monotonic은 null |
| predictors.sections | raw signal, 실제 score operands, 관찰 wallet 집합, flow/trajectory/age/pressure, RPC/quote 정보 |
| predictors.counters/durations | 실제 관찰한 attempt/retry/대기/request elapsed. 미관찰 값은 null |
| decision | BUY/REJECT_ANALYZER/REJECT_RISK/QUOTE_FAILED/RPC_SKIPPED/OTHER와 수집 종료 사유. predictor 밖 envelope metadata |
| execution_receipt | BUY 함수 반환 이후 생성 확인 시각/trade ID. predictor 밖 실행 증거 |
| provenance | Git SHA/source digest, 명시적 비밀 제외 config subset/hash, platform/OS/Python/PID/session/module 초기화 시각 |
| collection_limits | prefix/list/string/nesting 수집 제한과 truncation 의미 |
| content_hash | canonical UTF-8 sorted compact JSON 내용 SHA-256 |

signal_id는 route/mint/정규화된 감지 시각/source wallet·signature hash와 optional upstream identity의 deterministic hash다. BUY 함수는 position ID만 반환하므로 event_seq는 null이며 원장을 추가 조회하지 않는다. event_seq/event_id는 후속 offline join에서 trade ID로 연결할 수 있다. rejection에는 trade ID가 없다.

Config fingerprint는 monitor가 전달하는 실제 비밀 제외 상수와 허용된 값만 다룬다. 전체 `.env` 또는 전체 effective configuration의 해시라고 해석하면 안 된다. process start timestamp는 OS 시작 조회가 아닌 모듈 초기화 UTC임을 명시한다. 재시작은 새 session ID로 구분한다.

## E. Instrumented paths / 수집 가능성

| Priority | 실제 수집 | 한계 / null 의미 |
|---|---|---|
| Timing | 실제 enqueue, semaphore 획득, analysis, 현금/크기 gate, preflight, buy/exit quote, entry decision | upstream 감지 monotonic 및 내부 원장 guard timing은 알 수 없음 |
| Retry/RPC/quote | 실제 physical attempt, local retry/failover, request elapsed, retry sleep, limiter sleep, reservation/queue wait, error category | 합산 request duration은 동시 요청 wall latency와 다름. 성공한 B 사전 confirmation은 BUY envelope에 합치지 않음 |
| Score | Safety 실제 배점/반올림 전 raw 입력; Momentum 계산에 실제 사용된 cap 이전 operands와 할당 점수 | 새 uncapped momentum total은 계산하지 않음. cache/shared-flight waiter의 과거 raw 작업은 null |
| Wallet | 이미 확인된 wallet IDs와 기여 lamports, observed count | 전체 uncapped count는 미관찰. early-exit lower bound 표시. graph/overlap 계산 없음 |
| Flow | 실제 300초 buy/sell count 및 combined volume | 초 단위 window, buy/sell volume 분해·acceleration은 null |
| Trajectory/age | 이미 보유한 현재 B 시장값, pair age, 정규화된 과거 projection | interpolation 없음. mint/pool creation/first-seen을 새로 조회하지 않음 |
| Pressure | 기존 task registry 크기, semaphore 잔여 permit/기존 concurrency | open positions, pending quotes, 전체 candidate/analysis count는 따로 조회·계산하지 않음 |
| Wallet performance | schema만 제공하며 현재 entry 경로에 없으면 null/missing reason | 사후 조회·새 수익률 계산 없음 |

RPC retry count는 같은 logical call의 추가 physical attempt이며 provider failover도 포함한다. local retry와 failover attempt도 별도 counter다. Limiter duration은 실제 sleep elapsed이고 reservation duration은 lock/thread/scheduling/저장 작업도 포함한다. Risk timing scope는 `cash_balance_sizing_gate`이며 내부 admission 검증 전체의 시간으로 해석하지 않는다.

Quote timestamp는 provider 생성 시각이 아닌 로컬 응답 JSON 수신 완료 시각이다. BUY quote age는 실제 수신 monotonic과 decision monotonic 차이다. Entry BUY와 exit preflight는 별도 section이다. DEX identifiers는 최대 32 leg이며 hash도 해당 보존 projection을 대상으로 한다. route 수와 truncation flag를 함께 해석한다. 자연스럽게 반복된 동일 notional 견적이 없으면 quote delta를 만들지 않는다.

## F. BUY / reject coverage

기존 process_paper_signal을 context envelope로 감싸고 Control 본문·함수 인자·거절 분기를 유지한다. BUY, analyzer/risk 거절, quote 오류, RPC skip, observation 미승격/기타 결과를 같은 schema로 남긴다. 승인 신호 생성 전에 발생한 B confirmation 실패도 별도 RPC_SKIPPED envelope로 남긴다. 성공한 confirmation 자체는 별도 row로 쓰지 않아 승인 BUY row를 중복 생성하지 않는다.

완전히 후보를 형성하기 전 discovery/parser에서 제외된 모든 feed event를 보장하는 것은 아니다. 기존 shadow backlog 제한으로 후보 자체가 만들어지지 않은 경우에도 새 queue나 추가 탐색을 만들지 않는다.

## G. Leakage audit

entry_decision_at을 기록하면 predictor 수정을 닫는다. BUY 생성 시각과 trade ID는 execution_receipt에만 들어간다. BUY 후 후속 기록 실패가 실제 BUY outcome을 다른 outcome으로 바꾸지 않도록 보호한다. decision/outcome metadata는 predictor 사전에 포함하지 않는다.

종료된 capture에 cache/shared flight가 늦게 값을 넣는 것도 무시한다. pre-signal history는 기존 normalizer로 감지 이전 시간만 보존한다. final PnL, winner/loser, exit reason, MFE/MAE, 미래 수익률·최대 가격·사후 wallet 성과는 수집하지 않는다. 결과 label dataset은 별도 offline 연구에서만 결합한다.

## H. Failure isolation

Context/hook/projection 실패도 기존 Control 실행을 계속한다. queue overflow, 큰 row, 저장 오류·용량 한도·conflict는 TELEMETRY_DEGRADED와 drop/write-error counter 및 고정 reason code로 남긴다. 원문 예외/URL/인증정보를 건강 상태에 복사하지 않는다.

용량은 queue 16, row 64KiB, 전체 4096 rows/128MiB로 제한한다. 중첩 값은 depth 5, mapping/list 128개, 문자열 256자 및 후보 전체 node/string budget로 제한한다. 용량을 먼저 예약하고 row를 쓰므로 budget 쓰기 실패 뒤 무제한 row 생성이 가능하지 않다. 실패한 row의 보수적 용량 예약은 다음 startup의 bounded 실제 파일 scan으로 조정한다. 한도에 도달하면 telemetry만 drop하며 기존 연구 row를 삭제하지 않는다.

Health counter는 해당 publisher process/session 단위이며 지속 저장 count는 storage.json 기준이다. 필드 일부의 수집 실패/health 오류는 degraded_event_count로 구분하고, 실제 수집 단위가 저장되지 못한 경우에만 dropped_row_count를 증가시킨다. health는 고정 파일 하나로 유지하여 재시작으로 health 파일이 무제한 생기지 않는다.

## I. Performance impact

추가 네트워크 요청, 새로운 sleep, 고빈도 동기 디스크 작업, 전체 history 복사, 큰 cache를 추가하지 않는다. 실제 limiter/retry의 기존 동작은 유지한다. Writer 종료 전 프로세스가 비정상 종료되면 메모리 queue에 있던 row는 유실될 수 있으며 거래 원장은 영향을 받지 않는다.

최소 hot-path microbenchmark는 테스트 환경의 수집 비용을 확인하는 용도다. Root의 독립 Python 프로세스에서 10,000번 begin/bind/두 timestamp mark/작은 score projection/finish를 측정했다. median 29.9µs, p95 38.6µs, mean 32.025µs였다. writer를 시작하지 않은 saturation 측정에서 queue depth는 16을 넘지 않았고 9,984건을 즉시 drop했다. 네트워크/디스크는 시작하지 않았다. 실전 BUY latency 무영향이나 timeout 확률 불변을 증명하지 않는다. 현재 실행 중인 Control에 instrumentation을 넣어 latency를 측정하지 않는다.

## J. N3 isolation

N3 cohort `cba13fbb-c702-4b18-bcf7-20b78a8e252f`, rule hash `b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165`, start 10901/2026-10-03T01:04:59.425384+00:00, frozen build `8f0b9429352c9961b895a9f9c0e2d1f3e6b19d05`를 바꾸지 않는다.

src/research/n3_shadow.py와 evaluation/기존 N3 tests는 수정하지 않는다. Control의 기존 N3 prepare/submit 인자도 유지한다. Active checkout 소스와 HEAD는 그대로 둔다. 원격 main이 새로운 commit으로 진행돼도 active checkout의 HEAD/source manifest는 그대로이며 실행 identity 비교는 origin/main을 사용하지 않는다.

## K. Tests

필수 명령은 `python -m compileall -q src tests`와 `python -m unittest discover -s tests -v`다. 새 테스트는 실제 capture/row를 확인하며 BUY 인자 동일성, 거절 coverage, 미래 값 제외, hook/write/queue 실패 격리, deterministic identity/duplicate, null, score operands, lower-bound wallet count, timestamp ordering, 재시작 provenance/health, Windows 다중 프로세스 원자 쓰기, 큰 row와 저장 한도를 검증한다. 기존 N3 테스트도 전체 회귀에 포함한다.

최종 compile PASS, 전체 **717 tests OK / skipped 17**(Linux 전용 15개, Windows symlink 권한 2개). 새 recorder/monitor/execution 테스트는 각각 33/16/11개다. 최초 전체 실행은 메모리 해제 검사 1개가 wrapper 추가 후 이전 함수 본문만 검색해서 실패했다. wrapper가 실제 memory helper를 호출하는지와 helper 내부의 raw 해제→allocator trim→return 순서를 함께 검증하도록 해당 관련 테스트를 갱신한 뒤 단독 및 전체 재실행에서 통과했다. N3 테스트와 fixture는 수정하지 않았다. Windows 4-process 원자 중복 경쟁도 통과했다.

검증 log: `C:\Users\user\AppData\Local\Temp\aibot-entry-telemetry-tests-20261003-final.log`. Linux 실프로세스/락 환경은 이 Windows 실행으로 확인한 것이 아니며 해당 skip를 성공으로 해석하지 않는다.

## L. Changed files

- src/research/entry_telemetry.py
- src/monitor.py
- src/analyzer.py
- src/executor.py
- src/solana_rpc.py
- tests/test_entry_telemetry.py
- tests/test_entry_monitor_telemetry.py
- tests/test_entry_execution_telemetry.py
- tests/test_runtime_memory.py (wrapper 호출 경로에 맞춘 기존 순서 검증 유지)
- docs/entry-telemetry-2026-10-03.md

AGENTS.md, 거래 원장 schema, 전략/threshold/sizing/SL/TP, 의존성, deploy workflow와 ecosystem 설정은 수정하지 않는다.

## M. Git status

위 연구 계측 파일만 별도 worktree에서 commit/push한다. 실제 실행 checkout의 foreign 변경을 포함하지 않는다. reset/rebase/amend/squash/force push/history rewrite 없이 main fast-forward 가능성을 확인한다.

commit 전 `git diff --check` PASS, 변경 파일 10개만 확인했다. 변경에 credential/data/log/런타임 state/기존 foreign 파일은 포함하지 않는다. 최종 commit SHA와 push 이후 원격 일치 여부는 완료 응답에 보고한다.

## N. Deployment / runtime start recommendation

이 작업은 구현·테스트·문서·Git 반영까지이며 runtime은 시작하지 않는다. GitHub Actions 세 workflow가 disabled_manually인 상태를 확인하고 그대로 유지한다. 서버 수동 배포나 원격 코드 편집을 하지 않는다.

Active N3 동안 이 구현을 현재 checkout에 적용하거나 HEAD를 갱신하면 frozen source identity가 달라진다. 따라서 현재 코호트의 종료 및 별도 runtime epoch 적용 작업이 허용되기 전에는 새 recorder를 활성화하지 않는다. N3 rule/contract를 완화하거나 cohort를 교체해서 우회하지 않는다.

2026-10-03 20:12:34 KST 실측에서 실행 checkout의 전체 frozen build/source identity와 foreign 파일 해시가 일치했다. N3 RUNNING/health OK, Control RUNNING_HEALTHY/collection freshness PASS, WSS SUBSCRIBED, backlog 0이었다. 이번 worktree에는 운영 .env를 복사하거나 runtime을 시작하지 않았다.

Verdict: **TELEMETRY_PARTIAL** — 안전한 기존 값 수집 구현을 준비하지만 초 단위 flow·전체 whale 수·wallet performance 등 기존에 없는 데이터는 만들지 않으며, active N3 동안 runtime 적용은 보류한다.

N3_SHADOW_STATUS: **기존 RUNNING 유지**
PROJECT_STATUS: **PAUSE_STRATEGY_REVIEW**
