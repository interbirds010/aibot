# N3 prospective shadow 구현·시작 보고

PROJECT_STATUS: **PAUSE_STRATEGY_REVIEW**. Phase: **PAPER_MVP_RUNNING**. RPC_OPTIMIZATION_COMPLETE 유지.
N3는 고정 negative-filter 후보이며 수익 Alpha를 뜻하지 않는다. 실거래·전략·threshold·sizing·SL/TP 변경, N5, 과거 결과 재튜닝을 수행하지 않았다.

## A. Preflight

요청에 지정된 `C:\Users\user\Documents\aibot-local-paper`에서 작업했다. HEAD는 `c066afcd03d7d838e7e85a0c11c0598b60e3ab64`, branch는 `codex/memory-failure-diagnostics`이다. 기본 `git fetch origin`은 삭제된 tracking branch ref 때문에 실패했다. 이후 `git fetch origin refs/heads/main:refs/remotes/origin/main` 성공, HEAD 대 origin/main ahead/behind **0/0**을 확인했다. reset/rebase/amend/commit/push는 없다.

기존 monitor의 Windows feeder 변경(+25/-10), self_recovery 테스트(+1), `.deployed-sha`, local runner·운영 문서·관련 테스트를 보존했다. 별도 solana-ai-bot 개발 checkout의 dirty 파일도 수정하지 않았다. PROJECT_DIRECTION.md와 PAPER_MVP.md를 읽었다. 문서와 실제 거래 수치의 차이를 이번 작업에서 맞추지 않았다.

작업 전·검증 후 local runner는 monitor/risk-manager/dashboard를 모두 `finished_or_stopped`, wallet-feeder를 `not_started`로 보고했다. Control 프로세스 시작·중지·재시작을 수행하지 않았다. 기존 Control position 2개는 유지됐다.

## B. Changed files

- `src/monitor.py`: 실제 단계 시각과 진입 전 연구 snapshot, BUY 성공 후 bounded queue 전달, 서비스 시작 시 연구 writer 준비. 기존 feeder 수정 보존.
- `src/research/n3_shadow.py`: 동결 규칙·계약·명시적 등록, immutable sidecar와 별도 observer.
- `src/research/n3_shadow_evaluation.py`: 종료 코호트 평가. 실행 전략에서 호출하지 않는다.
- `tests/test_n3_shadow.py`, `tests/test_n3_shadow_evaluation.py`, `tests/fixtures/n3_historical.json`: 경계·실패·원장·재시작·다중 프로세스·통계 검증.
- 본 보고서. AGENTS.md, 의존성, 배포 정의와 기존 연구 산출물을 변경하지 않았다.

## C. Exact rule/hash

원본: `C:\Users\user\Documents\aibot-alpha-discovery\20261002\locked-discovery-rules.json`의 N3만 사용한다. 원본 파일 SHA-256: `65babf0b1a60b8174ce8f8032952f3e7769b8f6e27640edb269099ddaade886e`.

```text
(quote_preflight_duration_sec <= 4.165494775772094
 OR quote_preflight_duration_sec > 4.808962464332581)
AND analysis_duration_sec > 1.497460412979126
```

하단은 **<=**, 상단과 analysis는 **>**다. 원본 outside 조건을 그대로 재현했다. `historical_n3_20261002_v1`, 실험 `n3_prospective_shadow_v1`.
Bare rule SHA-256: **b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165**.
UTF-8 / sorted keys / compact JSON / no newline / nonfinite 거부로 serialize한다. rule 자체, 계약, 전체 marker 및 각 immutable 기록의 내용 해시를 각각 보존한다.

## D. Decision-time availability proof

`src/monitor.py:1145` 실제 analyzer 시작, `:1150` analyzer 완료, `:1226` 실제 preflight 시작, `:1238` entry quote의 로컬 수신 완료, `:1284` entry·exit preflight 완료를 캡처한다. `:1301` 연구 판정을 고정하며, observation 결정 저장은 `:1314`, 실제 Paper BUY는 `:1378`이다. N3 판정은 BUY 전이다.

| Predictor | 판정 | 그대로 유지한 historical 의미 |
|---|---|---|
| analysis_duration_sec | AVAILABLE_BEFORE_ENTRY_DECISION | analysis 완료 epoch − signal epoch; semaphore·discovery·초기 gate 포함 |
| quote_preflight_duration_sec | AVAILABLE_BEFORE_ENTRY_DECISION | preflight 완료 epoch − analysis 완료 epoch; cash/size·setup·entry/exit quote·limiter/retry 포함 |
| entry_latency_ms | AVAILABLE_BEFORE_ENTRY_DECISION | 기존 signal→post-preflight 경과시간; N3 predictor에는 사용하지 않음 |

순수 analyzer/network 실행시간으로 바꾸지 않았다. 두 predictor 합이 기존 entry_latency와 약 1ms 이내인 historical 결과는 같은 elapsed 구간을 나누기 때문이며 독립적인 predictor 증거가 아니다. 기존 discovery `started_at`과 document `updated_at`을 결정 시각으로 사용하지 않는다.
`entry_decision_timestamp`는 observation 저장과 BUY 이전의 **post-preflight 연구 판정 시각**이다. 최종 원장 락 또는 실제 체결 시각이라는 뜻이 아니다. quote_timestamp는 provider 생성 시각이 아닌 **로컬 수신 시각**이며 quote_age는 그 시각부터 연구 판정까지의 경과시간이다. 없는 provider 시각·retry 값은 만들지 않는다.

## E. Sidecar schema/health

`data/n3_shadow/manifest.json`: cohort ID, UTC start, start event_seq, 계약·규칙·해시, Git SHA와 dirty source digest/파일별 hash. `.env`와 비밀값은 읽거나 hash 대상에 넣지 않는다.

`snapshots/`, `entries/`, `sells/`, `completed/`는 identity hash 파일명의 **immutable JSON 기록 컬렉션**이다. JSONL 대신 파일 단위 append-only를 사용한다. 각 기록은 content_hash를 가지며 state_store의 OS lock·atomic write를 통해서만 저장한다. 기존 기록을 수정하거나 복구를 위해 덮어쓰지 않는다. 동일 재시도는 no-op, 다른 내용·손상·삭제된 committed 기록은 연구 observer만 실패시킨다.

기록 내용: cohort/experiment/rule identity, source/build, platform/OS, Control PID/session, signal·단계별 시각·판정·quote 시각/age, raw inputs/would_skip, missing flags/reason, family/mint, Control BUY event/position IDs, 수량/원가/가격. SELL은 event별로 기록하며 부분 TP와 최종 SELL의 합산 원가 불변식이 맞을 때 completed PnL/normalized return/delta를 기록한다. source wallet/전체 signature/API key는 추가 수집하지 않는다.

`observer_state.json`은 cursor·version·heartbeat·observer PID/session·admitted/completed/open 수·missing field 수·건강만 표시한다. 중간 평가나 중간 PnL로 결정을 변경하지 않는다. `capture_health.json`/`health.json`은 실패 범주만 보존한다. writer queue는 16개, entries 상한 408개, SELL 기록 상한 1632개다. recorder는 background thread, observer는 별도 프로세스다. 외부 API 호출·RPC 변경은 없다.

## F. Cohort inclusion

원장 락 안에서 start_utc와 당시 `next_event_seq`를 한 번 등록한다. **signal_timestamp >= start_utc AND BUY event_seq >= start_event_seq**인 실제 Control BUY만 포함한다. marker 이전에 시작하여 이후 BUY한 signal도 제외한다. 기존 147건, 구현 중 거래, 기존 open position은 fresh 평가에서 제외한다. marker는 재등록/재시작 시 같은 identity를 유지하고 source build가 달라지면 거부한다.

연구 snapshot은 진입 전 메모리에 고정하고 BUY 성공 후 저장한다. observer는 snapshot을 최대 30초 기다린다. 여전히 없으면 UNKNOWN/would_skip=null로 최초 admission을 고정한다. 늦게 도착한 snapshot으로 재분류하지 않는다. missing/nonfinite/잘못된 timestamp 순서는 UNKNOWN이며 Control은 그대로 수행한다. UNKNOWN은 효과 대조에서 따로 표시하고 hypothetical 잔존 전략에는 유지한다.

## G. Sample gates/hard cap

정식 판정은 completed Control >=200, valid hit >=40, valid non-hit >=40, Control winners >=20, active KST days >=14를 **모두** 요구한다. 최소 day gate는 completed trade가 대표하는 signal/entry 일자를 세는 보수적 기준이다. hard cap day는 open을 포함한 모든 eligible BUY의 KST signal/entry 일자를 센다. 이는 명시적으로 다른 분모이며 close-date는 사용하지 않는다.

최소 gate 달성만으로 종료하지 않는다. 첫 **28 active entry days 또는 400 completed Control trades** event에서 admissions와 최종 평가를 종료한다. 결과나 승률에 따라 종료·연장하지 않는다. 당시 open trade는 censored로 보고하고 강제 청산하지 않는다. cap 이후 SELL로 primary cohort를 늘리지 않는다. gate 미달이면 INSUFFICIENT_FRESH_SAMPLE이다. duplicate 재시도/장애 복구 시 cap event_seq가 바뀌지 않도록 cursor 시점 이하 기록만 사용한다.

## H. Evaluation/verdict contract

Primary delta = hit이면 `-realized_pnl/original_entry_cost`, non-hit이면 0. Secondary actual delta = hit이면 `-realized_pnl`, non-hit이면 0. equal 1SOL은 장부 정규화이며 실제 1SOL quote나 cash/slot recycling 시뮬레이션이 아니다.

최종 보고: 적중·비적중·승패·coverage·loser/winner exclusion, avoided/missed/net 실제 SOL과 정규화, 잔존 trade count/win rate/expectancy/PF/PnL/SELL 시간순 drawdown, UNKNOWN stratum. KST entry-day 전체 cluster를 paired resample하는 percentile 95% bootstrap, seed **20261003**, **5000회**를 고정했다. 종료 후 chronological first/second half와 이익을 준 tail 1/2/3개 제거를 actual/norm 각각 계산한다. route별로 같은 sample gates를 독립 적용하며 부족하면 INSUFFICIENT_ROUTE_SAMPLE이다.

고정 판정은 다음과 같다. 질적 표현을 보수적인 결정 규칙으로 사전 구체화했다.

- INSUFFICIENT_FRESH_SAMPLE: 최소 gate 하나라도 미달.
- NEGATIVE_FILTER_FAILED: normalized net <=0, 회피 손실 <= 포기 이익, temporal half 하나라도 net<0, 또는 상위 normalized benefit 2개 제거 후 net<=0.
- ROBUST_NEGATIVE_FILTER: normalized CI 하단>0, actual net>=0, 두 half 양수, 상위 benefit 3개 제거 후 양수, 고정 비용 시나리오 모두 normalized effect 양수, UNKNOWN 없음.
- PROMISING_NEGATIVE_FILTER: positive point estimate이나 위 robust evidence 부족.

STRATEGY_ALPHA_STATUS는 별도다. 잔존 normalized expectancy CI 하단>0, PF>1, total>0일 때 POSITIVE_EXPECTANCY_EVIDENCE; 표본 gate 미달은 INSUFFICIENT_SAMPLE, 나머지는 NO_POSITIVE_EXPECTANCY_EVIDENCE. N3 성공으로 strategy Alpha를 선언하지 않는다.

추가 비용 민감도는 recorded Paper, round-trip 추가 50bps, 추가 100bps+10000lamports로 시작 전에 고정한다. 관측된 live fee라고 주장하지 않는다. Paper quote 내재 비용은 원장 그대로, 누락된 network/Jito 비용은 unmodeled다. 추가 비용은 skip의 benefit을 기계적으로 늘릴 수 있으므로 그것만으로 robustness를 주장하지 않는다. 각 scenario의 remaining expectancy/PF/total/DD도 별도 출력한다.

## I. Tests

기존 Python 3.12 venv에서 `python -m compileall -q src tests` PASS.
`python -m unittest discover -s tests -v`: **679 tests, OK, skipped=17**. Linux 전용 15개와 Windows symlink 권한 제한 2개는 실행하지 못했다. Linux runtime 검증이나 실제 future fill 검증을 했다는 뜻이 아니다.

신규 49개: 동결147건 exact hit(D56/V17, L/W46/10·16/1), rule hash·boundary·missing·timing, start 경계, Control BUY 동일성, recorder 실패, partial TP/final SELL 원가·PnL·delta, 재시작 identity/cursor, Unicode Windows 파일, parseable corruption, committed 기록 삭제, 늦은 snapshot, hard cap 장애 재실행, 4개 Windows spawn 프로세스 동시 저장, normalized 경제 효과·bootstrap·route·tail·cost·Alpha 분리. 4-process race는 신규1회/중복3회/exit0을 확인했다. `git diff --check` PASS.

## J. Control-isolation proof

N3 판정값은 진입 허용, 금액, SL/TP, paper BUY 인자 또는 return 조건에 연결하지 않았다. snapshot 예외와 submit 예외를 별도 잡고 기존 BUY를 수행한다. 실제 저장은 BUY 뒤 queue에 전달하며 동기 파일·Git·네트워크 작업을 추가하지 않았다. 신규 테스트는 hit/nonhit/UNKNOWN/prepare 실패/submit 실패 모두 **동일한 BUY call**을 확인한다.
진입 전 pure snapshot 함수 10000회 로컬 microbenchmark는 median 4.0µs, p99 4.7µs였다. 실제 거래 경로 전체의 latency를 측정한 결과는 아니며 실제 future signal 성능은 아직 확인되지 않았다.

## K. Git/deployment

우리 범위의 변경은 B 목록뿐이다. 기존 foreign 변경을 그대로 보존했다. `.env`, 거래 history/state, outputs, RPC 설정, 의존성, AGENTS.md를 수정하지 않았다. 원장 초기화·강제 청산·실거래·commit·push·서버 배포는 없다. 신규 연구 데이터와 observer 전용 로그만 별도 생성할 수 있다.

## L. Ready-to-start

코드/동결 규칙/테스트/격리 검증은 완료했다. 별도 safety review의 cursor·late snapshot·기록 삭제 결함을 회귀 테스트와 함께 수정했다. 이 보고를 먼저 작성한 뒤 안전성이 확인되면 명시적 register로 marker를 생성하고 별도 observer만 시작한다. **Control 세 서비스가 현재 중지 상태이므로 실제 신규 Paper 표본은 Control 실행이 재개되기 전까지 생기지 않는다.** Control을 이번 연구 때문에 자동 시작/재시작하지 않는다.

운영 명령(기존 interpreter):

```powershell
$python = 'C:\Users\user\Documents\solana-ai-bot\.venv\Scripts\python.exe'
Set-Location -LiteralPath 'C:\Users\user\Documents\aibot-local-paper'
& $python -m src.research.n3_shadow status
& $python -m src.research.n3_shadow watch
```

watch는 독립 OS lock으로 중복 observer를 거부하며 hard cap에서 종료한다. PC 재부팅 뒤 자동 재시작은 설정하지 않았다. 다시 watch를 실행하면 기존 marker/cursor를 이어간다. 프로세스 종료/파일 손상/원장 event gap은 연구만 실패시키며 Control은 건드리지 않는다. source build 변경 후 기존 실험을 조용히 재개하지 않는다.

## 시작 후 확인

**N3_SHADOW_STATUS: RUNNING** — 독립 observer만 실행 중이며 중지된 Control의 미래 eligible 거래를 대기한다. 실제 prospective 거래 수집은 아직 시작되지 않았다.
**PROJECT_STATUS: PAUSE_STRATEGY_REVIEW**. Phase PAPER_MVP_RUNNING, RPC_OPTIMIZATION_COMPLETE 유지.

| 항목 | 확인값 |
|---|---|
| cohort_id | 607c372a-1fd5-4d48-9af3-00d9d90fcd2f |
| start KST | 2026-10-03T09:46:16.129564+09:00 |
| start UTC | 2026-10-03T00:46:16.129564+00:00 |
| start event_seq | 10901 |
| N3 rule hash | b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165 |
| build Git SHA | c066afcd03d7d838e7e85a0c11c0598b60e3ab64 |
| dirty source digest | f048db0dd0799fbfb8e38ecd00e69ae2b9c5813ee2ab3ac0ced56790c83fab5e |
| contract hash | 4e9242c5eb5133a2983e9523fd48f17cfbf6a4b473bfff515f5cbb7dc735d562 |
| observer PID / launcher PID | 42988 / 40612 |
| observer session | d15c44cd-51e3-442b-b767-b8518f2a4baf |
| observer state / recorder health | RUNNING / OK |
| admitted / completed / fresh open | 0 / 0 / 0 |
| initial cursor | 10900 |
| Control monitor / risk / dashboard | finished_or_stopped / finished_or_stopped / finished_or_stopped |
| existing Control positions | 2（fresh 제외） |

Control position `522501ab-010b-4e45-9c9e-15c8731fa159` / mint `DbaSVQ87gQumixFumMHCrMKFpq5dSYNtKiVNDw3Vpump`: A, 수량96993489559, remaining cost23248993lamports, 저장 상태NORMAL.
Control position `4c163918-c8b2-49f4-8c96-2cc7d96703c6` / mint `Gf79Rm3ffe58uZyRWBRv7YUB6ETUnHCt81WVMHckpump`: A, 수량871246046860, remaining cost23190871lamports, 저장 상태NORMAL. 현재 risk-manager가 꺼져 있으므로 이 상태값은 실시간 청산 감시의 증거가 아니다.

등록 전후 `paper_trades`, `wallet_performance`, `global_metrics`, `signal_observations`, `shadow_trades`, `wallets` **6개 state 파일 SHA-256 모두 동일**. Control ledger version2098759 / next_event_seq10901 유지. 기존 position을 강제 청산하거나 history를 재작성하지 않았다. Observer heartbeat/version 진행, stderr 0bytes 확인. 로그는 `logs/n3_shadow_607c372a.stdout.log` / `.stderr.log`의 새 전용 파일이다. marker/연구 데이터와 로그는 Git ignore 대상이며 commit하지 않았다.

최종 Git status는 새 연구 파일/본 보고서와 기존 dirty 목록만 포함했다. 의도하지 않은 코드 변경 없음. compile/full tests/diff check 이후 source 수정은 없으며 시작 후에는 본 보고서만 갱신했다. **미커밋·미push·서버 미배포**. Linux-specific runtime·실제 미래 거래 recorder flush·실제 자연 청산 경로는 아직 fresh 데이터로 검증하지 못했으며 단위/통합 mocked 테스트로 확인했다.

## 첫 표본 이전 code freeze 전환

위 시작 후 표는 초기 zero-sample 단계의 역사적 snapshot이다. 사용자 후속 요청에 따라 `607c372a-1fd5-4d48-9af3-00d9d90fcd2f`는 **ABANDONED_BEFORE_FIRST_SAMPLE**로 기록했다. admitted/completed/fresh open 모두 0임을 재확인한 뒤 observer만 종료했다. 기존 marker·기록을 재작성하지 않고 `data/n3_shadow_abandoned_607c372a`에 보존했다.

N3 instrumentation·관련 테스트·본 문서만 커밋하고, foreign Windows feeder/runtime 변경은 별도 hunk로 남긴다. 새 정식 코호트는 commit/push 완료 후 다른 ID와 새로운 시각·event_seq로 등록하며, rule hash와 evaluation contract hash는 기존과 같아야 한다. start_event_seq는 등록 당시 next_event_seq이므로 `>=` 계약을 바꾸지 않는다.

새 Git SHA는 N3 구현의 재현 기준이다. 보존한 foreign Windows feeder·local runner가 실제 runtime에 존재하므로 전체 실행 소스는 Git SHA에 더해 새 코호트의 source digest와 별도 동결 소스 사본으로 식별한다. foreign 파일은 stage/commit하지 않는다. 중간 성과 검토 없이 프로세스·기록 correctness만 확인한다.

후속 legacy regression 2개는 원래 BUY가 premarker인 포지션 2개의 postmarker SELL을 fresh 표본에서 제외하며, seq가 start_event_seq와 같은 새 BUY는 포함함을 고정했다. 실제 최종 commit SHA·새 marker·Control 재개·검증 결과는 새 코호트의 `launch_report.md`에 기록한다.
