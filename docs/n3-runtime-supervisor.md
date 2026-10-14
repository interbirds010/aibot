# N3 frozen runtime 외부 복구 harness

## 2026-10-13 파일 접근 복구 정책

외부 harness만 갱신하며 frozen application 소스와 cohort는 그대로 둔다.
검증, 초기화, 제어 파일 읽기와 최종 상태 저장의 `PermissionError`, Windows
sharing violation 32/33/5는 1초·5초·15초 뒤 재시도한다. 최초 시도 포함 최대
4회이며, 검증 재시도는 guard와 프로세스 출처 검사를 처음부터 수행한다.
지속 실패는 `BLOCKED`로 남긴다. build/source/cohort/rule/manifest 불일치,
원장 parse 실패, 중복 프로세스는 재시도 없이 즉시 차단한다.

최종 상태 쓰기 실패 시 같은 대상에 대한 재시도는 4회로 제한한다. 다른
상태 파일에 BLOCKED 기록을 한 번 시도하고, 두 파일 모두 쓰지 못하면
로그와 현재 Supervisor 메모리에 차단 상태를 보존한다. 이 경우 durable
상태 보존은 보장할 수 없으므로 파일 접근 원인을 사람이 확인해야 한다.
기존 재시작 예산과 수동 disabled marker는 유지한다.

`RUNNING_HEALTHY`는 프로세스 존재, Observer 기록 시각/PID, Monitor heartbeat,
WSS `SUBSCRIBED`와 기존 `collection_metrics_are_fresh` 검증을 모두 요구한다.
`RECOVERING`, `DEGRADED`, `BLOCKED`에는 `NEEDS_ATTENTION=true`와 사유를 남긴다.
`last_successful_validation`은 전체 검증 통과 시각이며 서비스 정상 판정과
별개다. Task Scheduler의 Running이나 오래된 Observer/WSS 상태 문자열을
운영 정상 근거로 사용하지 않는다.

진단에는 접근 대상의 절대 파일 경로, read/hash/replace 단계, errno/winerror,
파일명·행·함수로 구성한 traceback을 기록한다. 예외 원문, locals, 소스 줄은
기록하지 않는다. 과거 generic PermissionError 로그만 남은 장애는 정확한
접근 파일과 stack을 복원할 수 없으며 Observer 오류와 동일 원인으로 단정하지
않는다. 기존 outage 기록과 frozen manifest를 다시 쓰지 않는다.

이 harness는 `C:\Users\user\Documents\aibot-local-paper`의 frozen
`8f0b9429352c9961b895a9f9c0e2d1f3e6b19d05`를 그대로 감시한다.
monitor, risk-manager, dashboard, N3 observer의 availability만 복구한다.
N3 rule, cohort, threshold, sizing, SL/TP, classification, persistent cursor,
과거 결과와 telemetry activation을 변경하지 않는다.

## 설치 위치와 권한

내구성 있는 외부 경로 `C:\Users\user\Documents\aibot-n3-runtime-harness`에
검토한 `n3_runtime_supervisor.py`를 복사한다. 삭제될 수 있는 Git worktree나
현재 main을 예약 작업의 실행 경로로 지정하지 않는다. frozen runtime 내부에
새 코드를 설치하지 않는다. 운영 health/disabled marker만 별도 sidecar로 둔다.
Python 경로는 frozen runtime의
기존 venv `Scripts\python.exe`를 사용한다. 예약 작업 자체는 동일 venv의
`pythonw.exe`로 실행하여 로그온 때 콘솔 창이 나타나지 않게 한다.

현재 사용자 SID의 `AtLogon`, `InteractiveToken`, `LeastPrivilege`를 사용한다.
사용자 암호를 저장하지 않고 SYSTEM이나 관리자 권한으로 실행하지 않는다.
Windows가 부팅돼도 현재 사용자가 로그인하기 전에는 실행되지 않는다.
로그온 후 30초 지연으로 사용자 환경과 네트워크 초기화 시간을 준다.
지연은 네트워크 성공을 보장하지 않으며 실제 시작 검증은 supervisor가 한다.
`.env`는 기존 frozen 애플리케이션의 로딩 경로를 유지한다. 스케줄러 정의에
비밀값을 넣거나 `.env`를 출력하지 않는다.

## 검토, 등록, 현재 PID 인계

PowerShell에서 다음과 같이 설치 정의를 생성한다. 기본 동작은 DryRun이며
XML만 출력한다. `-XmlPath`를 지정하면 검토용 UTF-16 XML을 저장한다.

```powershell
$runtime = 'C:\Users\user\Documents\aibot-local-paper'
$python = 'C:\Users\user\Documents\solana-ai-bot\.venv\Scripts\python.exe'
$harness = 'C:\Users\user\Documents\aibot-n3-runtime-harness'
& .\scripts\install_n3_runtime_task.ps1 -RuntimePath $runtime -PythonPath $python -HarnessHome $harness -XmlPath (Join-Path $harness 'task.xml')
```

XML의 trigger SID, Python executable, 전체 arguments, working directory를
검토한다. 절대 경로와 Windows 명령줄 인용을 사용하며 공백·한글·`&`를
포함하는 경로도 하나의 인자로 전달된다. 상대 경로와 제어 문자는 거부한다.

```powershell
& $python (Join-Path $harness 'n3_runtime_supervisor.py') status --runtime $runtime --python $python --home $harness
& .\scripts\install_n3_runtime_task.ps1 -RuntimePath $runtime -PythonPath $python -HarnessHome $harness -Apply
Start-ScheduledTask -TaskName 'SolanaAIBot-N3-Frozen-Runtime'
Get-ScheduledTask -TaskName 'SolanaAIBot-N3-Frozen-Runtime'
Get-ScheduledTaskInfo -TaskName 'SolanaAIBot-N3-Frozen-Runtime'
```

등록 직전 `status` 읽기 전용 검증을 다시 실행한다. 실패하면 등록하지 않는다.
동일 이름의 기존 작업은 덮어쓰지 않는다. 검토 후 제거/재등록해야 한다.
등록과 실행은 분리돼 있으며 실행 시 기존 live PID를 발견하면 그대로 인계한다.
중복 프로세스나 출처가 모호한 프로세스는 추가 실행 대신 BLOCKED 처리한다.
현재 Control이나 observer를 종료해서 복구 시험을 하지 않는다.

작업은 `IgnoreNew`, 실행 시간 제한 `PT0S`, `StartWhenAvailable=true`,
배터리 실행 제한 해제로 구성한다. Task Scheduler의 `RestartOnFailure`는
설정하지 않는다. 재시도 예산은 supervisor의 영속 상태가 담당한다.
작업 상태 `Running`만으로 application health를 판단하지 말고 별도 health와
live PID, dashboard HTTP, heartbeat, observer backlog를 함께 확인한다.

## 안전 검증과 실패 분류

supervisor는 시작/재시작 전 frozen build, 소스 해시, cohort ID, N3 rule hash,
거래 원장의 JSON 및 불변식, cursor와 이벤트 순서, 프로세스 실행 출처를 검증한다.
원본 상태가 없거나 손상됐을 때 빈 원장으로 초기화하지 않는다.
오래된 PID는 실행 중 PID의 실제 command와 출처를 확인하고 판단한다.
실행 중 동일 서비스가 2개 이상이면 재시작하지 않는다.

단순 process absence는 기존 명령과 기존 persistent cursor로 복구할 수 있다.
PermissionError로 observer가 종료됐으면 소스를 수정하지 않고 외부 backoff 후
같은 observer를 실행한다. 로그에서 확인되는 PermissionError만 원인으로 기록하며,
핸들 소유자나 파일 잠금 원인을 추측하지 않는다. stale observer health는
원본을 지우지 않고 현재 PID/최근 로그/새 health 증거와 구분한다.

state parse failure, ledger corruption, cohort/rule/build/source hash mismatch,
duplicate process는 BLOCKED다. 재실행으로 해소하지 않는다. 원인 검토 없이
expected identity나 원장을 새 값으로 맞추지 않는다. `clear-blocked`는 원인 수정
이후에도 전체 provenance 검증을 통과해야 하며 자동 초기화 수단이 아니다.

별도 harness home의 `supervisor_control.json`은 restart budget과 BLOCKED 상태를
보존하고 `supervisor_owner.lock`은 supervisor 중복을 막는다.
`supervisor_operation.lock`은 매 검사/maintenance 작업을 직렬화한다.
`n3_runtime_supervisor.log`는 1 MB마다 회전하며 현재 파일과 백업 3개로 제한한다.
availability health는 runtime의 `data/n3_shadow/runtime_supervisor_health.json`에
별도 원자 API로 기록한다. 기존 observer health/PnL 원장은 변경하지 않는다.
disabled marker는 runtime의 `data/n3_shadow/runtime_supervisor.disabled`이며
실험 원장과 별개다.
재시도 예산은 supervisor 재시작에도 이어지고 최초 시작 여부나 부재를
기존 source/state를 바꾸는 신호로 사용하지 않는다. 로그와 health에는 운영 상태만
기록하며 성과/PnL 원장을 만들거나 복사하지 않는다.

## Restart 예산

검사 간격은 30초다. 서비스별 900초 창 안에서 시작 시도는 최대 3회이며
실패 후 backoff는 15/30/60초다. 다음 검사 때 backoff가 충족되어야 시도하므로
실제 재시도 간격은 검사 주기에 따라 더 길 수 있다. 시도/다음 허용 시각은
wall clock 기준으로 영속화한다. 연속 launch 실패 3회는 BLOCKED로 고정하고
사람의 원인 확인과 `clear-blocked`가 필요하다. 재시작 직후 다시 종료되는 등
창 내 시도 예산을 소진하면 cooldown으로 실행을 제한한다. 재시작 후 빠르게
다시 종료되는 일이 3회 이어지면 BLOCKED로 고정한다. 정상 실행이 15분 유지되어야
연속 실패 수가 해제되며, 사람이 검증 후 clear-blocked를 실행할 수도 있다.
Task Scheduler 자체의 무한 재시도와 supervisor 프로세스 재시작으로 예산을
초기화하지 않는다. `clear-blocked` 역시 기존 restart budget을 초기화하지 않는다.

## 의도한 수동 중지와 제거

사용자가 Paper를 의도적으로 끄려면 먼저 자동 복구를 disable 한다.
maintenance CLI가 supervisor와 같은 제어 락에서 marker를 변경하여 재시작과
경쟁하지 않게 한다. marker 변경 명령이 성공한 뒤 기존 수동 종료 절차를 사용한다.

```powershell
& .\scripts\set_n3_runtime_maintenance.ps1 -Mode disable -RuntimePath $runtime -PythonPath $python -HarnessHome $harness
```

disable은 이미 실행 중인 애플리케이션을 종료하지 않는다. enable도 애플리케이션을
즉시 실행하지 않으며 다음 supervisor 검사에서 정상 검증 후 복구할 수 있게 한다.

```powershell
& .\scripts\set_n3_runtime_maintenance.ps1 -Mode enable -RuntimePath $runtime -PythonPath $python -HarnessHome $harness
```

작업 제거도 기본 DryRun이다. `-Apply`는 action이 예상 외부 supervisor와 정확히
일치하는지 검사하고 먼저 disabled marker를 둔 뒤 작업 등록만 제거한다.
`Stop-ScheduledTask`나 process kill을 실행하지 않는다. 실행 중 supervisor는
Windows가 종료될 때까지 DISABLED로 남을 수 있다. 등록 삭제 시 실행 중 작업의
동작은 이 환경에서 검증하지 않았으며 애플리케이션 종료를 요청하지 않는다.
원장, observer cursor, health와 marker는 보존한다.
다른 command를 가진 동명 작업의 등록 제거는 거부한다.

Windows에서는 자식 프로세스가 부모의 job을 상속할 수 있으므로 supervisor
중지가 애플리케이션에 미치는 영향을 격리 검증 없이 안전하다고 가정하지 않는다.
이것이 이 환경에서 Task Scheduler가 항상 자식을 종료한다는 뜻은 아니다.
[Microsoft Job Objects](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects)와
[AllowHardTerminate](https://learn.microsoft.com/en-us/windows/win32/taskschd/tasksettings-allowhardterminate)의
동작을 고려하여 강제 종료를 허용하지 않고 명시적 task stop도 생략한다.

```powershell
& .\scripts\remove_n3_runtime_task.ps1 -RuntimePath $runtime -PythonPath $python -HarnessHome $harness
& .\scripts\remove_n3_runtime_task.ps1 -RuntimePath $runtime -PythonPath $python -HarnessHome $harness -Apply
```

## 검증과 다음 자연 재부팅 acceptance

Task 테스트는 실제 PowerShell로 XML을 생성하고 XML parser와 Windows
`CommandLineToArgvW`로 실행 인자를 검증한다. frozen 프로세스를 종료하거나
스케줄러에 fixture 작업을 등록하지 않는다.

```powershell
python -m compileall -q src tests
python -m unittest discover -s tests -p test_n3_runtime_task.py -v
python -m unittest discover -s tests -v
```

이번 작업에서는 Windows를 실제 재부팅하지 않는다. 다음 자연 재부팅 뒤 현재
사용자 로그온 및 30초 경과 후 예약 작업 상태/실행 시각, supervisor session과
최근 검사 시각, 각 서비스 정확히 1개 PID, frozen identity, dashboard HTTP 200,
fresh heartbeat/WSS, observer cursor continuity/backlog를 확인한다.
부팅·로그온·supervisor·애플리케이션 시각을 구분하고 실행되지 않았다면 Task
Scheduler Operational 이벤트와 harness 로그를 대조한다. 필요 시 수동으로
작업을 시작해도 중복 방지와 provenance 검증을 생략하지 않는다.

자동 시작은 로그온이 필요하며 PC 전원 차단 중에는 감시할 수 없다. 자연
재부팅 acceptance와 실제 transient crash 복구는 아직 발생하지 않았다면
미검증으로 보고한다. 전략 검토는 계속 `PAUSE_STRATEGY_REVIEW`, telemetry는
`NOT_STARTED`이며 frozen cohort의 outage gap annotation을 유지한다.
