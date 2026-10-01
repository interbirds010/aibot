# Production memory failure 진단 계측

## 범위와 기준

- 기준 소스 및 운영 deployed SHA: `909a99e3dc3592af70cff154ad68f9c61b7c3679`.
- 기존 34시간 21분 관찰에서 메모리 초과 재시작 54회, allocator trim 성공 821회.
- `PAPER_MVP_RUNNING`, `RPC_OPTIMIZATION_COMPLETE`, Paper-only 운영을 유지한다.
- 이번 변경은 진단만 추가한다. 전략, 후보 수, RPC 요청 순서·동시성, sizing,
  SL/TP, 260 MiB PM2 제한, 200 MiB trim 임계값, 60초 trim 간격을 바꾸지 않는다.
- 코드 변경은 `codex/memory-failure-diagnostics` 분리 worktree에만 존재한다.
  원래 checkout의 외부 수정과 미추적 파일은 보존했다. commit/push/배포 없음.

## 기존 계측의 공백

`runtime_memory`의 phase count/maxima는 프로세스 재시작으로 초기화된다.
60초 heartbeat는 짧은 피크를 놓친다. `monitor_memory_phases.json`의 상세 이벤트
200개와 sampler 이벤트 96개는 모든 프로세스가 같은 최근 ring을 사용하므로,
이후 정상 작업만으로 지난 failure 직전 증거가 밀려난다. 누적 maxima는 배포 전
기록도 포함한다. trim 로그는 실제 시도만 기록하며 cooldown으로 건너뛴 release
point와 raw 객체의 생존 상태를 연결하지 못했다.

## 확인한 객체 수명

### Candidate

검색, profile/boost, token batch HTTP 요청은 순차적이다. 전체 raw 응답들을
`gather` 결과 목록으로 유지하는 구조는 없다. `response.json()` 내부에서 body,
decode 입력 문자열, JSON 그래프가 겹칠 수 있다. HTTP helper 반환 후 projection
단계에는 raw JSON과 compact dataclass가 함께 있지만, helper의 HTTP body 소유
수명은 이미 끝난다. 외부 라이브러리 참조까지 모두 해제됐다는 뜻은 아니다.

`payload[:limit]`는 일시적인 참조 목록을 만든다. 이후 filtering/dedup은 compact
pair 목록과 두 mint dict를 유지하고, downstream 참조 목록, sorted 결과, key tuple,
결과 slicing도 겹친다. 이 시점에 원본 raw JSON을 보유하는 코드 근거는 없다.
어느 부분이 운영 피크의 몇 bytes를 차지했는지는 아직 확정되지 않았다.

### Whale / transaction

원본 signatures 목록과 projected signature 목록은 confirmation 함수가 끝날 때까지
유지된다. transaction은 하나씩 조회하며 matching 직후 `None`으로 바뀐다.
matching 동안 message/meta/account/token-balance 파생 구조와 compact 결과가 겹친다.
완료된 raw transaction을 전체 fan-out의 gather 결과로 유지하는 구조는 없다.
Standard websocket 경로는 복원 transaction을 `print_buys`가 소비한 뒤 해제한다.

### Shadow 원장

2026-10-01 21:55 KST 읽기 전용 stat 기준 파일은 47,190,003 bytes였다.
파일 크기는 Python 객체 그래프 크기가 아니다. `read_json`의
`json.loads(path.read_text(...))`는 decode 중 전체 입력 문자열과 생성되는 그래프를
동시에 유지한다. loaded 경계는 decode 반환 후 측정이므로 순간 피크와 구분한다.

Atomic writer는 `json.dump`로 조각을 바로 파일에 쓴다. 전체 원장을 한 번 더
`json.dumps`/`.encode`하는 경로는 없다. Serialization과 tmp write는 interleave되며
독립된 전체 serialized buffer가 없다. closed 파일의 buffer release와 원장 root
reference 해제는 다른 경계다. 기존 schema/version/lock/fsync/rename/cleanup는 유지한다.

Shadow 목록 slicing은 참조 배열을 복사하며, 새 trade의 일부 metadata는 observation
객체와 공유된다. Sample dict는 얕게 복사한다. Startup maintenance의 observation
graph와 shadow load/backfill, 분석의 observation·shadow graph가 겹칠 수 있다.
Migration이 반환한 원장은 caller가 보유하므로 scope 종료를 객체 해제로 해석하지 않는다.
ID 조회의 migration/외부 scope 중첩도 별개의 두 원장 그래프를 뜻하지 않는다.

## 기록과 상한

`src/failure_memory_diagnostics.py`가 `data/failure_memory_snapshots.json`에 기록한다.
공유 JSON 쓰기는 `state_store.update_json`의 파일 락과 atomic replacement를 사용한다.
이 파일은 실행 시 생성되는 진단 원장이며 이번 작업에서 운영에 생성하지 않았다.

- Native sampler: 1초. 파일 I/O는 sampler thread에서만 수행한다.
- Event-loop counter task: 1초, task·collection의 scalar 수량과 freshness만 전달한다.
- RSS threshold: 220/240/250 MiB. 260 MiB 제한까지 각각 40/20/10 MiB 여유.
- 반복 high-RSS 표본은 10초, baseline은 60초 간격. 고RSS 해제 이후 10초 동안
  낮아진 RSS의 release/exit 경계도 보존한다.
- 고RSS에서 phase before/stage RSS, VmHWM, elapsed, stage, input/projected/live 수량,
  task/history/cooldown/momentum 수량, trim age/eligible/in-progress를 함께 기록한다.
- 대상 lifecycle 4종과 기존 coverage/archive/observation 등 finite active phase 수량을
  함께 기록한다. 기존 phase 락이 바쁘면 unavailable로 표시하며 기다리지 않는다.
- Active registry 16개, phase scalar 8개(최신 갱신 우선), pending queue 64개.
- 최근 process generation 최대 8개, 세대별 incident 최대 48개,
  처음 baseline과 최근 baseline을 합해 최대 8개.
- 세대별 250 MiB 이상 마지막 critical 표본은 별도 보존하며 queue overflow에서도
  별도 pending slot으로 보호한다. VmHWM이 threshold를 넘거나 고수위에서 5 MiB
  이상 증가하면 낮은 현재 RSS에서도 `hwm_peak`를 저장한다. 첫 관측과 실제 증가를
  구분하고 별도 HWM slot을 보존한다. 과거 HWM을 현재 RSS 초과로 해석하지 않는다.
  전체 문서 최대 1 MiB, 크기 상한에 도달하면
  incident 수를 추가로 줄인다. 모든 과거 54회 failure의 영구 보존을 보장하지 않는다.
- Raw JSON, mint/wallet/signature 목록, URL, 인증 정보, 객체 graph deepcopy/순회는
  진단 데이터에 넣지 않는다. scalar 값·키 길이·배열 수량은 제한된다.
- 기존 trim 실행 로그는 유지한다. 새 기록에는 cooldown skip과 numeric phase/reason
  code가 포함된다. phase code 1=candidate, 2=confirmation, 3=transaction,
  4=shadow, 0=unspecified. reason code 1/2/3은 각 raw release 사유, 0=unspecified.

## 다음 관찰에서 가설을 구분하는 법

| 가설 | 필요한 증거 |
|---|---|
| A: trim cooldown | RSS>250, raw release stage/flag, cooldown remaining>0, 실제 cooldown skip을 같은 process_start_id로 연결 |
| B: live payload | decode/materialized/matching stage에서 live flags와 item count가 유지되는 고RSS; release 전후·trim 전후 RSS를 비교 |
| C: overlap | 같은 표본에 candidate/confirmation/transaction/shadow와 기존 heavy phase가 동시에 active; nested scope는 별개 graph로 세지 않음 |
| D: shadow | loaded→mutated→serialization/write→flush/close→rename→ledger release 경계의 RSS 및 VmHWM 변화와 file/trade count |
| E: baseline drift | 같은 프로세스의 처음/최근 낮은 RSS baseline, lifetime, active phases, task/collection 수량, freshness를 함께 비교 |

PM2 daemon의 memory restart timestamp·이전 PID와 이 파일의 process_start_id를
대조한다. `trim_eligible`은 시간·RSS·플랫폼 조건이며, 살아있는 객체를 해제할 수
있다는 의미가 아니다. Trim success도 allocator 반환값 기준이므로 RSS 감소와 다르다.

## 검증과 한계

Threshold/cooldown, thread overlap, exception/cancellation cleanup, bounded queue 및
generation/문서 크기, corruption 보존, weakref 기반 raw release, transaction retry,
atomic writer 실패·observer 실패 격리, 기존 후보/원장 회귀 테스트를 수행한다.
기존 프로젝트 `.venv`의 Python 3.12를 사용하며 Python 3.10 문법도 compileall로 확인한다.

Scalar lifecycle 계측은 코드가 소유하는 참조의 수명에 대한 증거이며 객체별 allocator
bytes를 직접 측정하지 않는다. GIL-bound decode는 sampler를 지연시킬 수 있고,
SIGKILL 전 마지막 미저장 표본도 유실될 수 있다. sampler gap, counter freshness,
VmHWM, queue drop/persistence failure 및 unavailable 필드를 먼저 확인한다.
Windows 테스트는 Linux procfs·glibc 실제 동작과 운영 1GB 호스트 overhead를 보증하지
않는다. 배포 후 관찰이 필요하지만 이번 작업에서는 운영 배포·재시작을 하지 않는다.
