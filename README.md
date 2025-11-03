# KRX Feed Pipeline

이 프로젝트는 한국투자증권(KIS) API를 1차 소스로 사용해 07:30/17:00 KST 파이프라인을 실행하고, 확보한 원시 시계열을 `raw/`에 저장한 뒤 파생 지표만 `out/latest.csv`와 `out/daily/*.csv`에 반영합니다. 17:00 실행 시에는 1차 소스 기준으로 리컨실을 수행합니다.

## 요구 사항 요약

- KIS 앱키/시크릿(`KIS_APPKEY`, `KIS_APPSECRET`)을 GitHub Secrets에 등록해야 실제 API를 호출할 수 있습니다.
- 모든 항목은 “KIS → Yahoo Finance → 지정 웹페이지(③~⑤)” 순서로 데이터를 시도하며, 모든 소스가 실패하면 값은 비워 두고 `notes="parse_failed:<url>,<reason>"` 형식으로 실패 이유를 남깁니다.
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

실행 결과는 `out/latest.csv`, `out/daily/<날짜>.csv`, `out/logs/runner_<날짜>.json`에 기록됩니다. 실패 시 `out/debug/` 하위에 HTML 요약이 남습니다.

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

