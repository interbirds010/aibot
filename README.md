# Solana AI Trading Bot

Python 3.10+ 기반의 비동기 Solana DEX 자동매매 시스템 골격입니다. DexScreener 같은 집계 API를 핵심 데이터 소스로 사용하지 않고 Helius RPC의 원시 온체인 이벤트를 직접 구독하도록 설계했습니다.

## 아키텍처

```text
Helius WebSocket / gRPC
          │
          ▼
  collectors (수집·재연결)
          │ asyncio.Queue (bounded)
          ▼
  parsers (트랜잭션/로그 정규화)
          │
          ▼
  strategy (특징·AI 신호·리스크 판단)
          │
          ▼
  execution (paper/live 주문 및 확인)
          │
          ▼
  storage/monitoring (비동기 기록·지표)
```

- 모든 장기 작업은 `asyncio.TaskGroup`과 bounded `asyncio.Queue`로 연결합니다.
- WebSocket 수집기는 지수 백오프로 재연결하고, 소비자가 느릴 때 큐가 무한히 커지지 않게 합니다.
- 전략 판단과 주문 실행을 분리해 실거래 키가 데이터 수집 계층으로 새지 않게 합니다.
- 기본값은 `paper`이며 실거래 실행기는 별도 구현·검증 후 활성화합니다.
- `asyncio`는 Python 표준 라이브러리이므로 `requirements.txt`에는 설명 주석만 있습니다.

## 설정

PowerShell:

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

`.env`에 Helius 키를 입력합니다. 개인키는 가능하면 `.env` 대신 OS 키 저장소나 외부 secret manager에서 런타임에 주입하세요. `.env`와 개인키는 절대 커밋하지 않습니다.

실행:

```powershell
python -m src.main
```

기본 예제는 Solana 로그를 구독해 내부 큐로 전달하고 수신 사실만 출력합니다. 매매 주문은 발생시키지 않습니다.

스마트 머니 모니터 실행:

```powershell
python -m src.monitor
```

모니터는 `data/wallets.json`을 읽으며 `.env`에서 지갑 주소를 받지 않습니다. 파일 변경 여부를 기본 5초마다 확인하고 새 목록이 원자적으로 저장되면 파일을 다시 읽어 WebSocket 구독을 자동으로 구성합니다. 따라서 프로세스를 재시작하지 않아도 감시 대상 최대 20개가 동적으로 교체됩니다. Helius의 확장 `transactionSubscribe`를 사용해 감시 지갑 중 하나와 Pump.fun/PumpSwap/Raydium 프로그램 하나가 동시에 포함된 성공 트랜잭션만 RPC 단계에서 받습니다. 출력 금액은 지갑의 확정된 pre/post 잔액 기준 순변화입니다. `SOL net outflow`는 네트워크 수수료를 제외하지만, 새 토큰 계정 생성이 동반된 거래라면 계정 rent가 포함될 수 있습니다.

스마트 머니 지갑 자동 갱신은 유료 데이터 API나 수동 파일 없이 Helius 표준 Solana JSON-RPC만 사용합니다. 주요 DEX 5개의 최근 서명을 프로그램당 50건씩 조회해 최대 250개 서명자 후보를 만들며, 동시 RPC 요청은 3개·요청 시작 간격은 최소 0.3초로 제한합니다. 이후 0.1 SOL 미만 또는 최근 24시간 거래가 300건을 초과한 지갑을 제외합니다. 감시 중 관찰된 실제 스왑 가격은 `data/wallet_performance.json`에 저장되며, 1시간 뒤 새 온체인 가격 표본과 비교해 반복적으로 부진하거나 위험 토큰을 매수하는 지갑은 후보 풀에서 즉시 교체됩니다.

```powershell
# 1시간 주기 상시 실행 (WALLET_REFRESH_HOURS=2로 설정하면 2시간)
python -m src.wallet_feeder

# 즉시 한 번만 실행
python -m src.wallet_feeder --once
```

결과는 `data/wallets.json`에 원자적으로 저장됩니다. 성과 기준을 통과해도 Helius 이력이 20건 미만이거나, 같은 초 거래 비율·1분 이내 왕복거래 비율·동일 상대 계정 집중도가 임계값을 넘으면 제외됩니다. 블록 타임만으로 0.0001초 반응 시간을 증명할 수 없으므로 이를 주장하지 않고 같은 초 단위의 자동화 패턴을 보수적으로 대체 탐지합니다.

토큰 사기/러그 위험 분석:

```powershell
python -m src.analyzer <TOKEN_MINT_CA>
```

분석기는 finalized Helius RPC와 Rugcheck를 교차 사용합니다. 개발자 직접 보유량 10% 미만은 30점, Mint 권한 포기는 35점, LP 95% 이상 락업/소각은 35점으로 총 100점입니다. 세 조건을 모두 충족하고 85점 이상일 때만 `should_enter_token(mint)`가 `True`를 반환합니다. 데이터 누락이나 API 오류는 `False`로 처리합니다. CLI는 진입 가능 시 종료 코드 `0`, 그 외에는 `2`를 반환합니다. 이 판정도 안전을 보증하지 않으며 연결 지갑·내부자 그래프와 실제 매도 가능성 검사는 후속 단계에서 추가해야 합니다.

Jupiter/Jito 실행기:

```powershell
python -m src.executor <TOKEN_MINT_CA>
```

실행기는 분석 통과 후 현재 SOL 잔액의 정확히 1%를 입력 금액으로 사용합니다. 실거래 주문은 Jito Tip Floor의 최근 분포를 조회해 일반 매수·익절에는 50th~75th 구간의 중간값(62.5th), 긴급 손절·수동 종료에는 75th~90th 구간의 중간값(82.5th)을 적용합니다. Jito 응답에 90th가 없으므로 제공되는 75th와 95th 사이를 선형 보간합니다. 팁은 주문 SOL 명목가의 200bps와 0.03 SOL 중 작은 값으로 제한되며, API 장애 시 프로세스 내 최근 성공값 또는 0.002 SOL을 사용한 뒤 동일한 상한을 적용합니다. 기본 `TRADING_MODE=paper`에서는 서명하거나 전송하지 않습니다. 실거래에는 `JUPITER_API_KEY`, `SOLANA_PRIVATE_KEY_ENCRYPTED`가 필요하고 Fernet 복호화 키 `SOLANA_KEY_ENCRYPTION_KEY`는 `.env`가 아닌 OS/서비스 secret store에서 주입해야 합니다.

암호화 키와 지갑 암호문 생성 예시:

```powershell
$env:SOLANA_KEY_ENCRYPTION_KEY = python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python scripts/encrypt_private_key.py
```

출력된 암호문만 `.env`의 `SOLANA_PRIVATE_KEY_ENCRYPTED`에 저장합니다. `SOLANA_KEY_ENCRYPTION_KEY`는 재시작 후에도 안전하게 주입할 수 있도록 OS 자격 증명 저장소나 배포 환경의 secret manager에 보관해야 합니다.

모의투자와 리스크 관리의 최종 스위치는 `src/main.py`의 `PAPER_TRADING = True`입니다. 모의 매수·절반 익절·전량 손절 기록은 `data/paper_trades.json`에 원자적으로 저장됩니다. 초기 가상 시드는 10 SOL이며 매수마다 현재 가상 현금의 1%만 사용합니다. Jupiter의 실제 SOL 환산 견적을 API 기본 한도 내에서 순차 확인해 +50%에서 최초 1회 절반 익절하고 -15%에서 잔량 전부를 청산합니다. 포지션이 여러 개면 각 포지션 갱신 간격은 그 수에 비례해 늘어납니다. Helius DAS 가격은 최대 600초 캐시될 수 있어 초단기 손절 기준으로 사용하지 않습니다.

실시간 운영 대시보드:

```powershell
streamlit run src/dashboard.py --server.address localhost --server.port 8501
```

브라우저에서 `http://localhost:8501`을 열면 스마트 머니 지갑, 가상 SOL 자산, 포지션, 매매 이력과 누적 손익을 확인할 수 있습니다. 화면은 2초마다 JSON 원장을 다시 읽습니다.

`src/monitor.py`의 `TEST_ALLOW_UNVERIFIED_WALLETS = True`는 대시보드 검증을 위한 임시 Paper 전용 스위치입니다. `verified=false` 지갑의 신호도 분석 대상으로 받지만 Rugcheck/Helius 안전 점수 85점 방어막은 우회하지 않습니다. 실거래 전에는 반드시 `False`로 되돌려야 합니다.

Live 모드는 평문 개인키를 거부합니다. 암호화 키의 공개주소가 `EXPECTED_LIVE_WALLET_ADDRESS`와 일치하고 `LIVE_TRADING_ACK=I_UNDERSTAND_LIVE_TRADING_RISK`가 명시되어야 하며, RPC/Jito URL도 HTTPS mainnet인지 검사합니다. 거래 생성 후 Helius `simulateTransaction`의 `unitsConsumed`에 10% 버퍼를 더해 `SetComputeUnitLimit`을 다시 기록하고 재서명합니다. 실행 상태는 `BUILT → SIMULATED → SUBMITTED → LANDED/FAILED/UNKNOWN`으로 로그와 live 장부 이벤트에 기록됩니다.

## 권장 확장 순서

1. 관심 DEX 프로그램 ID와 이벤트 파서를 `src/parsers/`에 추가
2. 슬롯 지연, 중복 signature, rollback/finality 처리
3. 저장소와 replay 기반 전략 백테스트
4. 포지션 한도, 슬리피지, 일일 손실 한도, circuit breaker 구현
5. paper trading 및 devnet 검증 후 서명기를 격리한 live executor 추가
