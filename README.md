# KRX Feed Pipeline

이 프로젝트는 07:30/17:00 KST 예약 파이프라인에서 공개 시장자료를 수집합니다. KIS 조회 자격증명이 실행 환경에 있을 때만 KIS를 우선하고, 없으면 설정된 공개 원천을 사용합니다. 증권사 주문을 실행하는 파이프라인이 아닙니다. 확보한 원시 시계열을 `raw/`에 저장하고 계산 결과를 `out/latest.csv`와 `out/daily/*.csv`에 반영합니다.

## 요구 사항 요약

- KIS 앱키/시크릿(`KIS_APPKEY`, `KIS_APPSECRET`)을 GitHub Secrets에 등록해야 실제 API를 호출할 수 있습니다.
- 모든 항목은 “KIS(1차) → Yahoo Finance 또는 지정 웹페이지(③~⑤)” 순으로 값을 조회하며, 모든 소스가 실패하면 값을 비워 두고 `notes="parse_failed:<url>,<reason>"` 형식으로 실패 이유를 남깁니다. 국채 수익률(KR3Y/KR10Y)은 “KRX → KOFIA → 한국은행 ECOS → KIS → Naver → Investing.com” 순으로만 폴백하며, fixture/합성값은 사용하지 않습니다.
- 합성 시계열은 생성하지 않으며, 확보된 값만 `out/latest.csv`, `out/history.csv`, `out/daily/*.csv`에 반영합니다.
- 원시 시계열은 `raw/<asset>/<YYYYMMDD>.parquet` 또는 CSV로 보관합니다(파케이 엔진 미설치 시 CSV 폴백).

## 설치 및 로컬 실행

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python pipeline.py --phase 0730 --tz Asia/Seoul
python pipeline.py --phase 1700 --tz Asia/Seoul --reconcile
```

실행 결과는 `out/latest.csv`, `out/daily/<날짜>.csv`, `out/logs/runner_<날짜>.json`에 기록됩니다. 파이프라인 실패의 안전한 원인은 `debug/<phase>/pipeline_failure.json`, 목표 거래일과 핵심지수 원천일은 `history_inputs.json` 및 `core_sources.json`에서 확인합니다. 실패 artifact의 `out/latest.csv`는 이전 성공본일 수 있으므로 새 수집물로 해석하지 않습니다.

### 완료장 지수와 원천 지연

- 수집시각과 원천일을 구분합니다. `SOURCE_DATE_NOT_READY`는 필요한 거래일 종가가 없다는 뜻이며, 과거 숫자의 날짜를 오늘로 바꿔 해결하지 않습니다.
- KOSPI/KOSDAQ의 기존 KIS/Yahoo 응답에 완료장 목표일의 정상 종가가 없을 때만 공개 Naver 일봉 XML을 지수별 최대 1회 확인합니다. symbol·단위·정밀도·원천일·OHLC·중복을 검사하고, 기존 시계열의 누락된 목표일만 보완합니다. 목표일이 있으면 추가 조회하지 않습니다. 장중 관측을 종가로 복사하지 않습니다.
- 보조 원천도 부족하면 기존 엄격한 종가 검사가 계속 실패하며 정본을 보존합니다. 오전에는 기존 `WAITING_SOURCE` 부분 결과와 다음 예약의 완료장 보완을 유지합니다. 인증 차단·429를 우회하지 않습니다.
- 콘솔의 `[pipeline-status]`와 Actions 요약은 대상일·관측시각·결과를 표시합니다. 비밀정보·원시 예외본문은 넣지 않습니다. 요약 표시 실패가 정상 자료 발행을 실패로 뒤집지 않습니다.
- `0730`과 `1700` 예약은 GitHub에서 지연될 수 있습니다. 실제 관측시각으로 완료 거래일을 판단하며 미래·휴장 종가를 합성하지 않습니다. 이전 실패는 새 성공으로 고치지 않고 다음 실행 결과와 구분합니다.

## GitHub Actions

`.github/workflows/data-pipeline.yml`은 월~금 07:30/17:00 KST에 파이프라인을 구동합니다. 충돌 방지를 위해 `fetch-depth: 0`, `git pull --rebase`를 포함하고 있으며, `permissions: { contents: write }`가 지정되어 자동 커밋이 가능합니다.

## 구성 파일

- `conf.yml`: KIS 코드 매핑과 폴백 심볼을 정의합니다. `kis.mode`가 `auto`이면 앱키/시크릿이 존재할 때 실거래 API를 호출하며, `series` 섹션에서 지수·선물·환율·금리별 경로와 TR-ID, 파라미터를 조정할 수 있습니다.
- `src/universe.py`: KRX 종목 우주를 로드합니다.
- `src/kis/*`: KIS 토큰/마켓 래퍼. 앱키가 없으면 빈 데이터프레임을 반환하고, 파이프라인이 자동으로 2차·3차 소스로 폴백합니다.
- `src/compute.py`: HV, 상관, TRIN, 베이시스 등 파생 계산을 수행합니다.
- `src/storage.py`: 원시/결과 저장, 보존 정책(180일)을 관리합니다.
- `src/reconcile.py`: 17:00 리컨실 로직.

## 테스트

```bash
pytest
```

`tests/test_derive.py`는 HV30, 20일 상관, TRIN, 베이시스 계산과 원자재/크립토 폴백 파서를 검증합니다. 라이브 호출 실패 시 `out/debug/`에 원본 응답(JSON/HTML)을 저장하므로, `notes`에 남은 `parse_failed` 메시지를 참고해 파서를 조정하거나 소스를 교체할 수 있습니다.

`out/logs/runner_YYYYMMDD.json`에는 단계별 이벤트가 append 되며, 국채 심볼을 찾지 못한 경우 `event="monitor"` 레코드에 `symbol_not_found` 배열이 기록됩니다.
