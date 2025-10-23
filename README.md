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

각 실행은 `out/latest.csv`를 갱신하고, `out/daily/YYYYMMDD.csv`에 시계열을 보존합니다.

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

   워크플로는 실행 후 `out/` 디렉터리 변경을 자동 커밋합니다. GitHub Actions 로그와 저장소 커밋 내역에서 성공 여부를 점검하세요.
   `workflow_dispatch`로 수동 실행하면 07:30/17:00 단계가 모두 실행되어 즉시 동작을 검증할 수 있습니다.

6. **푸시 거부(업데이트 필요) 대응**

   다른 실행이나 수동 커밋이 선행되어 원격 브랜치가 앞서 있는 경우 워크플로가 자동으로 `git fetch` 후 `git rebase`를 수행해 최신 내역을 반영합니다.
   그래도 충돌이 발생하면 Actions 로그에서 충돌 파일을 확인한 뒤, 로컬에서 동일한 리베이스를 수행해 해결하고 다시 푸시해야 합니다.
