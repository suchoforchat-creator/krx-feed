# KRX Feed Pipeline

이 프로젝트는 한국투자증권(KIS) API를 1차 소스로 사용해 07:30/17:00 KST 파이프라인을 실행하고, 원시 데이터를 `raw/`에 적재한 뒤 파생 지표만 `out/latest.csv` 및 `out/daily/*.csv`에 저장합니다. 17:00 실행 시에는 1차 소스 기준으로 리컨실을 수행합니다.

## 요구 사항 요약

- KIS 앱키/시크릿(`KIS_APPKEY`, `KIS_APPSECRET`)을 GitHub Secrets에 등록해야 실제 API를 호출할 수 있습니다.
- KIS 장애 시 원유·브렌트·금·구리·BTC는 Yahoo Finance로 폴백하며 `quality=secondary`로 표기합니다.
- DXY와 미 국채 수익률(2/5/10년)은 선물 가격 변화를 이용해 프록시로 산출합니다.
- 모든 원시 시계열은 `raw/<asset>/<YYYYMMDD>.parquet`에 저장하고, 파생 지표만 결과 CSV에 반영합니다.

## 설치 및 로컬 실행

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python pipeline.py --phase 0730 --tz Asia/Seoul
python pipeline.py --phase 1700 --tz Asia/Seoul --reconcile
```

실행 결과는 `out/latest.csv`, `out/daily/<날짜>.csv`, `out/logs/runner_<날짜>.json`에 기록됩니다. 실패 시 `out/debug/` 하위에 HTML 요약이 남습니다.

## GitHub Actions

`.github/workflows/data-pipeline.yml`은 월~금 07:30/17:00 KST에 파이프라인을 구동합니다. 충돌 방지를 위해 `fetch-depth: 0`, `git pull --rebase`를 포함하고 있으며, `permissions: { contents: write }`가 지정되어 자동 커밋이 가능합니다.

## 구성 파일

- `conf.yml`: KIS 코드 매핑과 시뮬레이션 기본값을 정의합니다. `kis.mode`가 `auto`이면 앱키/시크릿이 존재할 때 자동으로 실거래 API를 호출하며, `series` 섹션에서 지수·선물·환율·금리별 경로와 TR-ID, 파라미터를 수정해 확장할 수 있습니다.
- `src/universe.py`: KRX 종목 우주를 로드합니다.
- `src/kis/*`: KIS 토큰/마켓 래퍼. 시뮬레이션 모드에서 합성 데이터를 제공합니다.
- `src/compute.py`: HV, 상관, TRIN, 베이시스 등 파생 계산을 수행합니다.
- `src/storage.py`: 원시/결과 저장, 보존 정책(180일)을 관리합니다.
- `src/reconcile.py`: 17:00 리컨실 로직.

## 테스트

```bash
pytest
```

`tests/test_derive.py`는 HV30, 20일 상관, TRIN, 베이시스 계산이 정확한지 검증합니다. 라이브 API에서 이상치가 관측되면 `out/debug/`에 원시 응답이 JSON으로 저장되므로, 해당 파일을 기반으로 파라미터(예: `conf.yml`의 KIS 시리즈 설정)를 보정할 수 있습니다.

