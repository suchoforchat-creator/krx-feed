# KRX Feed Synthetic Pipeline

이 저장소는 07:30/17:00 KST 두 단계로 시장 지표 스냅샷을 생성하는 데이터 파이프라인 예제입니다. 모든 출력은 `out/` 디렉터리에 기록되며 Asia/Seoul 타임존 기준 `ts_kst` 타임스탬프를 따릅니다.

## 설치

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 실행 방법

07:30 스냅샷:

```bash
python pipeline.py --phase 0730 --tz Asia/Seoul
```

17:00 리컨실:

```bash
python pipeline.py --phase 1700 --tz Asia/Seoul --reconcile
```

각 실행은 `out/daily/YYYYMMDD.csv`에 표준 스키마(`ts_kst,asset,key,...`)로 스냅샷을 저장합니다.
17:00 실행 직후에는 `update_history.py`가 호출되어 확정본을 다음과 같이 변환합니다.

* `out/latest.csv`: 17:00 확정본 1행만 포함. 스키마는 `time_kst,...,quality`(quality=`final`).
* `out/history.csv`: 최근 90일 확정본 누적. 날짜 중복 시 덮어쓰기 후 오름차순 정렬.
* `out/daily/index.csv`: (선택) 최근 180개 `out/daily/*.csv`의 Raw URL 색인.

## 수동 재실행

1. 최신 데이터를 삭제하거나 별도 백업.
2. 원하는 `--phase`와 `--reconcile` 조합으로 커맨드 실행.
3. GitHub Actions 워크플로에서 `workflow_dispatch`를 이용해도 동일하게 작동합니다.

## 품질 플래그 규칙

* `primary`: 1차 소스(공식) 또는 기본 생성 데이터.
* `secondary`: 2차 소스로 대체하거나 오류 처리 시 기록.
* `final`: 17:00 리컨실 이후 확정치.

리컨실 시 동일 `(asset, key, window)` 조합의 최신 레코드가 `final`로 승격되며 값이 임계 이상 변하면 `notes="revised"`로 표기합니다.

## 임계치 조정

`conf.yml`의 `thresholds` 섹션에서 자산군별 임계값을 수정할 수 있습니다.

```yaml
thresholds:
  index: 0.3
  rates: 0.01
  fx: 0.05
  commodity: 0.1
  hv_corr: 0.01
```

필요 시 `assets` 맵을 수정하여 자산-카테고리 매핑을 재정의하면 리컨실 임계치에 반영됩니다.

## 17:00 확정 히스토리 & 공개 URL

수동으로 히스토리를 재생성하려면 17:00 파이프라인을 실행한 뒤 다음 명령을 호출합니다.

```bash
python update_history.py --latest out/latest.csv --history out/history.csv --days 90
```

필요하면 `--index out/daily/index.csv` 옵션을 명시적으로 지정할 수 있습니다. 히스토리 스키마는 아래와 같으며 모든 값은 숫자(공백=결측)입니다.

```
time_kst,kospi,kosdaq,kospi_adv,kospi_dec,kospi_unch,kosdaq_adv,kosdaq_dec,kosdaq_unch,
usdkrw,dxy,ust2y,ust10y,kr3y,kr10y,tips10y,wti,brent,gold,copper,btc,k200_hv30,src_tag,quality
```

`out/history.csv`는 항상 90일 이내(헤더 제외 최대 90행)만 유지하며, Raw URL은 `https://raw.githubusercontent.com/<USER>/<REPO>/main/out/history.csv` 형식으로 접근할 수 있습니다.

## 로그 및 디버그

* 실행 로그: `out/logs/runner_YYYYMMDD.json`
* 데이터 스냅샷: `out/daily/`
* 디버그 HTML: `out/debug/asset_phase.html`

에러 시 핵심 키 충족률이 80% 미만이면 종료 코드 2를 반환합니다. 기타 예외는 종료 코드 1입니다.

## GitHub 연동 및 자동화 배포

1. **저장소 초기화 및 원격 연결**

   ```bash
   git init
   git remote add origin https://github.com/<계정>/<저장소>.git
   git add .
   git commit -m "Initial commit"
   git push -u origin main
   ```

   이미 저장소가 있다면 `git remote -v`로 연결 상태를 확인하고, 필요한 경우 `git remote set-url origin <url>`로 갱신합니다.

2. **GitHub Actions 활성화 확인**

   `.github/workflows/data-pipeline.yml`이 포함되어 있으므로 첫 푸시 이후 Actions 탭에서 워크플로가 표시됩니다. 월~금 07:30/17:00 KST(UTC 22:30/08:00)에 자동 실행되며 `workflow_dispatch`로 수동 실행이 가능합니다.

3. **필요 시 시크릿 설정**

   Cloudflare R2 업로드나 프록시가 필요하다면 GitHub 저장소의 `Settings > Secrets and variables > Actions`에서 `R2_ACCESS_KEY`, `R2_SECRET_KEY`, `R2_BUCKET`, `PROXY_URL` 등을 추가합니다.

4. **워크플로 권한 설정**

   Actions가 변경 사항을 푸시하려면 저장소 **Settings > Actions > General > Workflow permissions**에서 `Read and write permissions`를 선택하고 `Allow GitHub Actions to create and approve pull requests`를 필요 시 활성화합니다. 또는 `repo` 권한이 있는 개인 접근 토큰(PAT)을 시크릿으로 등록한 뒤 `actions/checkout`에 `token`을 지정해도 됩니다.

5. **자동 커밋 모니터링 및 수동 테스트**

   워크플로는 실행 후 `out/` 디렉터리 변경을 자동 커밋합니다. 17:00 잡은 `update_history.py`를 호출한 뒤 `final: update history (YYYY-MM-DD HH:MM:SS KST)` 형식으로 커밋을 생성합니다.
   `workflow_dispatch`로 수동 실행하면 07:30/17:00 단계가 모두 실행되어 즉시 동작을 검증할 수 있습니다.

6. **푸시 거부(업데이트 필요) 대응**

   다른 실행이나 수동 커밋이 선행되어 원격 브랜치가 앞서 있는 경우 워크플로가 자동으로 `git fetch` 후 `git rebase`를 수행해 최신 내역을 반영합니다.
   그래도 충돌이 발생하면 Actions 로그에서 충돌 파일을 확인한 뒤, 로컬에서 동일한 리베이스를 수행해 해결하고 다시 푸시해야 합니다.
