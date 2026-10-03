# Entry telemetry activation 전 offline hardening

2026-10-04 KST. 판정: **TELEMETRY_ACTIVATION_READY_WITH_RUNTIME_VALIDATION_PENDING**. 현재 telemetry를 활성화하지 않았다. 남은 검증은 자연 runtime acceptance와 실제 Control latency다.

## A. Preflight

후보 checkout은 `C:/Users/user/Documents/aibot-entry-telemetry`, 시작 HEAD/origin/main은 `52aa17f3583ee9127741e2687414dc1b4b9d81cf`, ahead/behind 0/0, clean이었다. Main ref를 fetch했다. Live `C:/Users/user/Documents/aibot-local-paper`는 frozen `8f0b9429352c9961b895a9f9c0e2d1f3e6b19d05`이며 기존 tracked/untracked foreign 변경을 그대로 보존한다. Live checkout/state/process를 수정하거나 재시작하지 않는다.

## B. Meaning of old 4096 limit

52aa17f의 `entry_telemetry.MAX_ROWS=4096`과 `MAX_STORAGE_BYTES=128MiB`는 epoch 전체의 **각 persistent stream 누적 쓰기 admission cap**이었다. `storage.json` 예약 수/bytes가 cap에 도달하면 새 row가 drop됐다. 저장된 과거 row를 삭제하거나 덮어쓰는 retention 로직은 없었다. Queue capacity는 별도로 각16개였다. Offline reader/join에도4096행 ceiling이 있었다. 이번에 writer/reader의 누적 cap과 startup 전체 이력 scan을 제거했다. 남은4096은 identity index 파일의 **byte bound**이며 행 수 제한이 아니다. Test에서4100행은 과거 cap을 넘는 검증 표본이다.

## C. New persistent storage architecture

```text
data/research/entry_telemetry/
  active.json
  epochs/<telemetry_epoch_id>/
    epoch.json
    sessions/<session_hash>.json
    predictors/YYYY-MM-DD/<signal_id>.json
    receipts/YYYY-MM-DD/<signal_id>.json
    outcomes/YYYY-MM-DD/<trade_id>.json
    <stream>/_identity/<hash-prefix>/<identity-hash>.json
    health/monitor.json
    health/risk-manager.json
```

세 stream의 physical separation, predictor schema2 / receipt1 / outcome1을 유지한다. Row/index는 state_store OS lock과 원자 교체로 최초 생성하고 봉인한다. 기존 파일을 update/delete하지 않는다. Predictor는 decision-time에 freeze하며 receipt/outcome으로 갱신하지 않는다. 기존 obsolete `storage.json`은 읽거나 수정하지 않는다. 실제 telemetry가 미시작이므로 production migration은 없다. 과거 mixed/unindexed runtime 데이터 발견 시 자동 변환하지 않고 별도 검토한다.

## D. Partition / rotation

Partition은 기록 시계가 아니라 원래 signal UTC를 KST로 변환한 날짜다. BUY/completed outcome도 동일 signal 날짜에 연결되므로 늦은 completion은 원래 partition에 새 파일로 추가된다. 날짜 이동에 open file handle/JSONL rotation은 필요 없다. Signal/trade ID별 hash shard index를 O(1)로 조회하고 stream lock으로 예약/행 commit을 직렬화한다. 같은 identity를 다른 날짜로 재시도하면 conflict로 거절한다. Row당64KiB, index당4KiB, queue각16, capture projection budget은 유지한다. Partition/파일 쓰기와 fsync는 background writer이며 Control critical path에는 없다. Size rollover는 개별 행 파일이므로 추가하지 않는다.

## E. Retention / storage estimate

기본 local research 정책은 **자동 삭제 없이 날짜 partition 전체 보존**이다. 최소90일, 현재 disk estimate로365일까지 보존 가능하도록 계획한다. 날짜/epoch당 행 수나 총 bytes cap, 자동 archive/자동 새 epoch/expiry delete는 없다. Disk free 확인은 operator의 운영 점검이다. 실제 disk-full은 telemetry drop/degraded를 보고하며 과거 원본을 삭제해 공간을 만들지 않는다.

2026-10-04 00:13 KST C: free 약418.26GB, NTFS cluster4096bytes를 확인했다. 140개 effective config를 포함한 합성 normal predictor14032bytes+index667, receipt7018+index687, completed outcome7040+index555다. 최근 완전24hour coverage proxy는2135 events/day(평균88.958/h, 최대271/h)이며 새 telemetry row rate나 burst를 보장하지 않는다. 모든 proxy event마다 BUY와completed outcome까지 생긴다는 보수적 가정에서 logical 약64.05MB/day,90일5.76GB다. 각 파일을 cluster로 올림하면 약96.19MB/day,90일8.66GB,365일35.11GB다. Bounded-stress predictor52002bytes를 전부 적용한 cluster estimate는174.90MB/day,90일15.74GB,365일63.84GB다.

365일 최대 약467만 row/index 파일이므로 directory/MFT metadata와 다른 연구 데이터 성장도 고려해야 한다. 위 수치는 filesystem overhead까지 측정한 실사용량/production p95가 아니다. 현재 free에 비해 여유가 있지만 실측 성장률을 정기 대조한다. 원본을 삭제하지 않는 archive는 필요 시 별도 검토한다. Queue overflow/OS write failure에 따른 새 기록 손실 가능성은 실패 격리 counter로 드러나며 과거4096 admission drop과 구분한다.

## F. Config fingerprint

`entry_telemetry_config.py`의 명시적 typed allowlist로 **140개 실제 effective 값**을 추출한다. Canonical sorted key/value JSON의 SHA-256을 epoch/session/각 row provenance에 연결한다. 전체 environment/secret를 hash하지 않는다. Config contract version2이며 recorder와 epoch가 동일 projection을 사용한다. `_safe`의128-item 제한으로140필드를 자르지 않는다.

| Category | 실제 source / 기록 내용 |
| --- | --- |
| Strategy / modes | 현재 A/B 경로 selection, observation/approved Paper flags, trading mode |
| Thresholds / sizing | monitor 상수, analyzer dataclass 기본값, executor 진입 비율/route B multiplier/fee reserve |
| SL/TP / capacity | risk-manager ratios/sell fraction, approved_signal_max_open_positions 실제1..20 설정 |
| RPC / WSS | provider enabled/RPS/light-heavy priority/attempt/circuit, WSS configured/resolved 여부 |
| Scheduler | reload/poll/cooldown/discovery/queue/concurrency 관련 실제 상수 |
| Quote / execution | slippage 기본값, impact/fee/attempt/reset-wait 제한 |
| Wallet source | feeder 환경 설정/clamps/defaults, wallet performance cooldown 등 상수 |
| Telemetry / features | stream schema2/1/1, prospective collector schema와 snapshot 제한 |

누락 environment는 실제 적용 default로 처리해 explicit 동일 default와 hash가 같다. Invalid mode/nonfinite/타입 오류는 deterministic fail이다. Low-level fixture용 partial config는 누락 key를 생략하지만 실제 runtime/tool projection은140필드를 모두 추출한다. API key, private key, URL, wallet address, auth token은 저장/hash하지 않는다. Endpoint secret 회전은 fingerprint를 바꾸지 않는다. Provider availability/order/RPS 변경은 바꾼다. 같은 provider의 endpoint byte 자체는 의도적으로 비교하지 않으며 내부 literal 정책도 build/source digest로 식별한다. 현재값을 strategy/threshold 변경으로 맞추는 작업은 하지 않는다.

Cutover/acceptance 도구는 미래 runner 환경 override → dotenv override=False 및 변수 치환 → effective config 순서를 재현한다. 호출자의 `os.environ`을 바꾸지 않는다. 실제 runner의 Paper/observation/8-position override가 `.env`보다 우선하는 기존 동작을 변경하지 않는다. Typed safe_config와 그 hash도 acceptance에서 재검증하여 재봉인된 payload 변조를 거절한다. SL/TP 설정값은 pre-entry provenance이며 realized outcome feature가 아니다.

## G. Epoch continuity

UUID immutable marker의 build/source inventory, config fingerprint, stream schemas, startUTC/KST/event_seq, platform 및 immutable PID/session association을 유지한다. Partition 변화나 restart가 epoch를 바꾸지 않는다. 새 activation만 명시적으로 새 UUID를 만든다. Restart 시 다른 build/config/schema는 telemetry를 disable하고 Control은 계속한다. 새 config contract/build는 이전 epoch로 몰래 이어 붙이지 않는다. 모든 partition row는 epoch/build/schema/session/config fingerprint를 보존한다.

## H. Windows / restart / future cutover

개별 atomic JSON 구조이므로 JSONL partial-last-line 복구는 적용되지 않는다. Partial final JSON/index는 원래 bytes를 보존하고 fail-closed한다. Stranded temporary file은 row로 취급하지 않는다. 예약 후 crash는 최초 payload hash와 같은 재시도만 복구한다. 실제 subprocess restart/OS lock 해제/reopen/동일 identity duplicate, KST midnight 직전/직후 partition을 검증한다.

실제 activation은 이번에 수행하지 않는다. 미래 절차는 다음 순서다.

1. N3의 matching final_report/closure/CLOSED와 observer 자연 종료를 확인한다.
2. Frozen8f0에는 cooperative stop handler가 없으므로 기존 owned Windows runner의 별도 승인된 operator 종료를 수행한다. PID/creation ownership과 원장 정상성을 확인한다. Tool은 강제 fallback하지 않는다.
3. `python scripts/entry_telemetry_cutover.py snapshot --root <runtime-root> --execute`: 모든 writer/PID 종료 후 byte backup/hash를 만든다. 256MiB/file·512MiB total·20000-file snapshot 상한은 거래 상태 snapshot 안전 상한이며 telemetry retention cap과 별개다.
4. Foreign backup/이관을 확인한 뒤 reviewed telemetry commit으로 checkout한다. Live dirty checkout에 blind pull을 하지 않는다. Compile와 전체 tests를 수행한다.
5. `create-epoch --execute` 후 `validate`: N3 종료/state continuity/build/config/schema/platform을 검증한다.
6. `resume --python <verified-python> --execute`: hidden owned runner로 Control을 한 번 시작한다. Ambiguous/duplicate PID면 거절한다. Health/WSS/freshness/backlog/dashboard를 확인한다.
7. `python scripts/entry_telemetry_acceptance.py --root <runtime-root> --output <report.json>`: 첫 predictor/reject/RPC skip/BUY receipt/completed outcome을 독립 확인한다. 자연 미발생은PENDING이다.
8. 후속 clean-stop은 등록된 기존 신호/진행 중 청산과 queue를 drain한 ack 및 실제 PID 종료를 요구한다. Timeout이면 강제 종료하지 않는다.

Windows case-insensitive path/PID creation/session을 비교하고 cutover 자기 PID와 검증된 동일 argv venv launcher만 inventory에서 제외한다. Abort/rollback 때는 최신 거래 원장과 원본 telemetry를 보존한다. 과거 snapshot으로 이후 거래를 지우지 않는다.

## I. Failure isolation

Stream별 독립 queue/counter/index/lock을 유지한다. Directory/partition/index/row 생성, disk-full/permission/replace 오류는 해당 telemetry drop/write-error/degraded로 기록하고 Control BUY/SELL을 막지 않는다. Health는 각 role/session의 성공 row commit 수이며 persistent 총수를 주장하지 않는다. Writer restart는 과거 전체 행을 scan/cache하지 않는다. Existing row/index hash linkage와 손상은 덮어쓰지 않고 거절한다.

## J. Offline join

Reader/index의4096 ceiling을 제거했다. Lazy scandir로 일별 row를 읽고 seal/schema/epoch/index-path/hash linkage를 검증한다. `_identity`는 row로 읽지 않는다. Predictor+receipt는 epoch+signal ID, completed outcome은 epoch+trade ID로 연결하며 build/config/mint/route/family/signalUTC/BUYseq가 일치해야 한다. Reject/RPC predictor-only는 정상이다. Duplicate/mismatch는 fail이고 파일은 변경하지 않는다. `iter_stream`은 행당 bounded read이며 `load_stream`/전체 join/전체 acceptance는 offline 메모리에 epoch를 모으므로 대형 연구는 날짜별 subset 계획을 사용한다. Runtime writer에는 offline join lookup이 없다.

## K. Offline performance

2026-10-04 00:13:05 KST, Windows temporary storage, modified candidate HEAD52aa17f에서140필드 config와 실제 serializer/writer를 사용했다. Full normal capture/mark/projection/enqueue1000건 mean102.625µs / p95138µs, 이전 p95132.2µs보다약4.4% 높다. Partition/index/disk I/O는 이 critical path에 없다. Predictor finish/enqueue p951.7µs, receipt put3.0µs, outcome capture/enqueue19.4µs다. 실제 Control latency나 production SLO로 주장하지 않는다.

Predictor+receipt+index+health fsync100건 mean16.756ms / p9519.139ms였다. Windows disk/cache 조건이 이전 측정과 달라 향상을 보장하지 않는다. Producer64 burst에서 queue16 유지,16처리/48drop으로 bounded queue 특성을 확인했다. Worker idle predictor wait0.25초와 receipt/outcome 각1개 처리 정책은 기존과 같다. Real compact fixture12300행+12300index 쓰기 검증도 별도로 수행했다. 측정 JSON: `C:/Users/user/AppData/Local/Temp/telemetry-hardening-measurements.json`.

## L. Tests

검증 범위: stream별4100행 보존(총12300 real atomic row +12300index), 과거 bytes 불변, daily rollover, partial JSON/temp/index, reservation crash/retry, process restart/OS lock, duplicate/day conflict, reader 실제4097파일 및 각4100행 join, fingerprint 결정성/키순서/default/secret 제외/관련 설정 변경,140필드 보존, dotenv 우선순위/환경 불변, typed config payload/hash 변조 거절, leakage/N3/Control 회귀.

`python -m compileall -q src tests scripts` PASS. `python -m unittest discover -s tests -v`: **897 tests OK / skipped17**,146.315초. Full log: `C:/Users/user/AppData/Local/Temp/telemetry-hardening-full.log`. 최초 전체 검증은 config 조회가 호출자 환경을 바꾸어 기존 관찰/쿨다운 테스트2개를 오염시킨 문제를 발견했다. Pure projection으로 수정하고 전체를 재실행했다. 기존 Linux 전용/Windows symlink 권한 skip을 성공으로 바꾸지 않는다.

## M. N3 isolation

2026-10-04 00:27:54 KST 최종 대조: Live frozen HEAD/source/foreign hashes 일치, monitor7876/risk4556/dashboard3624/observer43084 및 시작 시각 그대로, N3 RUNNING/recorder OK, WSS SUBSCRIBED, collection freshness PASS, backlog0, dashboard HTTP200, next_event_seq10956, telemetry directory 없음. Cohort `cba13fbb-c702-4b18-bcf7-20b78a8e252f`, startUTC `2026-10-03T01:04:59.425384+00:00`, seq10901, rule hash `b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165`를 유지한다. Inclusive BUYseq+signalUTC와 legacy9583/9661 제외 규칙을 변경하지 않는다. Telemetry runtime directory는 미생성이다. N3/PnL/Alpha 평가는 수행하지 않는다.

## N. Git state / changed files

관련 recorder/config/epoch/join/cutover/acceptance, measurement helper, regression/new tests와 이 문서만 commit/push한다. Strategy 실행 파일 monitor/risk/executor/analyzer와 thresholds/sizing/SLTP/원장 schema를 변경하지 않는다. Live foreign 변경은 stage하지 않는다. Main은 fast-forward이며 history rewrite 없음. Workflow3개 disabled_manually 상태를 그대로 유지해 배포/activation은 하지 않는다. 최종 SHA/remote/ahead/status는 완료 응답에 기록한다.

## O. Final verdict / abort

**TELEMETRY_ACTIVATION_READY_WITH_RUNTIME_VALIDATION_PENDING**. 누적 저장 cap과 좁은 config fingerprint라는 이번 offline blocker를 제거했다. 최초 frozen process owned-stop, N3 closure, 충분한 disk free와 state continuity는 미래 activation의 필수 운영 precondition이다. 누락/불일치한 epoch/build/config/session/schema, corrupt row/index, stale/degraded health, failed tests 또는 살아 있는 writer/PID이면 activation을 중단한다. 자연 acceptance와 실제 Control latency는 아직 검증하지 않았다.

```text
N3_SHADOW_STATUS: RUNNING
PROJECT_STATUS: PAUSE_STRATEGY_REVIEW
CURRENT_RUNTIME: frozen 8f0b942 유지
TELEMETRY_RUNTIME: NOT_STARTED
```
