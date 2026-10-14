# N3 fresh coverage feasibility amendment

이 amendment는 2026-10-07에 사용자가 **이미 완료 17건, hit 0건, non-hit 17건, active KST days 4일을 관찰한 뒤** 추가하도록 지시했다. 최초 사전 등록 조건이 아니며, 과거 protocol을 재작성하지 않는다. 채택 시각과 당시 실제 누적치는 외부 봉인 artifact에 별도로 기록한다.

대상은 frozen `8f0b9429352c9961b895a9f9c0e2d1f3e6b19d05`, cohort `cba13fbb-c702-4b18-bcf7-20b78a8e252f`, rule hash `b219661c31c132e1b2f11901664981482a9e42dc847ab300f590072eb3271165`다. 원래 `200 completed / 40 hit / 40 non-hit / 20 winner / 14 active days` gate와 `400 completed / 28 active days` hard cap은 원본 manifest에 그대로 남는다.

## 새 checkpoint와 허용 판단

기존 cohort의 누적 completed Control이 **30 이상 OR active KST days가 7 이상**인 첫 검사에서 `N3_FEASIBILITY_CHECKPOINT_REACHED`를 외부 기록한다. 여기서 active day는 기존 정의대로 완료 Control 거래의 KST signal/entry 날짜가 존재하는 날이다. 달력상 경과 일수나 open-only 날짜가 아니다. 실제 임계값 통과 시각과 검사 시각을 혼동하지 않는다.

보고 항목은 completed, days, valid hit, valid non-hit, unknown/missing coverage, recording correctness/health뿐이다. PnL, PF, expectancy, winner/loser 경제 지표를 계산하거나 판정에 사용하지 않는다. 파일의 immutable seal 검증은 원본 bytes의 정합성 확인이며 경제 평가가 아니다.

- 정상 recording에서 hit <= 1: `N3_LOW_FRESH_COVERAGE`; 해석은 `LOW_COVERAGE_IN_FRESH_REGIME`.
- 정상 recording에서 hit >= 2: `N3_CONTINUE_PROSPECTIVE_VALIDATION`.
- Identity, source, seal, pairing, count 정합성 또는 recording coverage에 이상이 있으면 먼저 데이터 문제를 보고하고 feasibility 판정을 차단한다. Unknown을 non-hit로 바꾸지 않는다.

Low coverage는 `FAILED_ALPHA` 또는 `FAILED_FILTER`가 아니다. Continue도 수익성 검증 성공이 아니다. 어느 결과도 실제 Control 전략 변경이나 telemetry activation을 자동 승인하지 않는다.

## 보존과 실행

Frozen source, 기존 manifest/contract, observer cursor, cohort, 과거 entry/completed/result를 변경하지 않는다. 도구는 외부 경로에 amendment와 첫 checkpoint를 봉인하며 기존 checkpoint를 최신 결과로 덮어쓰지 않는다. 원래 N3 observer의 hard-cap closure나 final report를 위조하지 않는다.

```powershell
$repo = 'C:\Users\user\.codex\worktrees\accelerated-alpha\solana-ai-bot'
$python = 'C:\Users\user\Documents\solana-ai-bot\.venv\Scripts\python.exe'
$runtime = 'C:\Users\user\Documents\aibot-local-paper'
$out = 'C:\Users\user\Documents\aibot-alpha-research\n3-feasibility'
& $python "$repo\scripts\n3_feasibility_checkpoint.py" --help
& $python "$repo\scripts\n3_feasibility_checkpoint.py" --runtime $runtime --amendment "$out\amendment.json"
& $python "$repo\scripts\n3_feasibility_checkpoint.py" --runtime $runtime --amendment "$out\amendment.json" --record --output "$out\checkpoint.json"
```

최초 등록은 `--register --output <외부 amendment.json>`이다. 이번 작업에서 2026-10-07 10:29:43 KST에 이미 등록했으므로 동일 경로를 다시 등록하지 않는다. 명시적 register/record만 외부 artifact를 쓰며 기본 status는 읽기 전용이다. Checkpoint 검사와 accelerated batch 경제 분석은 서로 다른 코드 경로다.

현재 채팅에 한 시간 간격의 checkpoint heartbeat를 등록했다. 대기 상태가 같으면 조용히 종료하고 최초 도달, recording/BLOCKED 또는 실행 실패에만 알린다. 도달 시 경제 분석·batch forensic·전환을 실행하지 않는다. 로컬 PC/앱을 이용할 수 없으면 즉시 검사/알림을 보장하지 않는다. 이 도구를 보관한 managed worktree를 삭제하기 전에 자동 점검의 경로도 검토한 새 위치로 갱신해야 한다.

## Telemetry 전환 결정

Low coverage이면 200건을 무조건 기다리는 대신 checkpoint 결과 보고 후 별도 operator 결정을 받는다. 현재 `86d7ddc` cutover 도구는 원래 N3 `CLOSED` + matching closure/final report를 요구한다. Amendment에 의한 조기 종료는 이 조건을 만족하지 않으므로 그대로 실행하면 거절되는 것이 정상이다.

다음 전환 작업에는 별도 amendment-resolution artifact와 matching 종료 경계, observer의 안전한 종료, 기존 원장/foreign 변경 백업, supervisor maintenance, 그리고 cutover guard의 명시적 amendment 종료 지원이 필요하다. 원래 manifest를 고쳐 기존 hard-cap 종료처럼 보이게 하지 않는다. Open position은 강제 청산하지 않고 평가 censor/유지 정책을 전환 전에 고정한다. 실제 checkout 변경·프로세스 종료·telemetry epoch 시작은 이번 작업에서 수행하지 않는다.
