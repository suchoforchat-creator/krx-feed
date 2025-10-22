# krx_scrape.py  (교체본 v3)
from playwright.sync_api import sync_playwright
import pandas as pd, re, os
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
def now_kst(): return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
os.makedirs("out", exist_ok=True)

def to_float(s):
    try: return float(re.sub(r"[^0-9.]", "", s))
    except: return None

def to_int(s):
    try: return int(re.sub(r"[^0-9]", "", s))
    except: return None

def first_number_in(locator, low=None, high=None):
    n = locator.count()
    for i in range(min(n, 50)):              # 최대 50개 탐색
        txt = locator.nth(i).inner_text().strip()
        v = to_float(txt)
        if v is None: 
            continue
        if low is not None and v < low: 
            continue
        if high is not None and v > high: 
            continue
        return v
    return None

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context()
    page = ctx.new_page()

    # 메인 진입 및 로딩 대기
    page.goto("https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd?locale=ko",
              timeout=120_000, wait_until="networkidle")
    page.wait_for_timeout(4000)  # 숫자 스팬 로딩 여유

    # 1) 코스피/코스닥: 라벨 포함 블록(ancestor) 범위 내 숫자만, 합리적 범위 필터
    def grab_index(label, low, high):
        # 라벨 텍스트 노드
        lab = page.locator(f"xpath=//*[contains(normalize-space(.), '{label}')]").first
        if lab.count()==0:
            return None
        # 가장 가까운 상위 컨테이너 내에서 숫자 후보 수집
        anc = lab.locator("xpath=ancestor::*[position()<=3]").first
        cand = anc.locator("xpath=.//span|.//strong|.//em")
        return first_number_in(cand, low, high)

    kospi  = grab_index("코스피", 100.0, 5000.0) or grab_index("KOSPI", 100.0, 5000.0)
    kosdaq = grab_index("코스닥", 100.0, 2000.0) or grab_index("KOSDAQ", 100.0, 2000.0)

    # 2) 상승/하락/보합: 라벨 인접 숫자만 추출(숫자 없으면 None)
    def grab_breadth(word):
        w = page.locator(f"xpath=//*[contains(normalize-space(.), '{word}')]").first
        if w.count()==0:
            return None
        # 인접 텍스트들 중 숫자 포함 요소 탐색
        near = w.locator("xpath=following::*[self::span or self::strong or self::em][position()<=5]")
        n = near.count()
        for i in range(n):
            s = near.nth(i).inner_text()
            if re.search(r"\d", s):
                v = to_int(s)
                if v is not None and v < 10000:  # 비정상 큰 값 필터
                    return v
        return None

    adv  = grab_breadth("상승")
    dec  = grab_breadth("하락")
    unch = grab_breadth("보합")

    row = {
        "time_kst": now_kst(),
        "source": "KRX_DOM",
        "kospi": kospi, "kosdaq": kosdaq,
        "adv": adv, "dec": dec, "unch": unch
    }

    # 디버그 스냅샷(필요 시 확인)
    page.content()  # 렌더 강제
    with open("out/main_snippet.html", "w", encoding="utf-8") as f:
        f.write(page.content())

    df = pd.DataFrame([row])
    # 최신 파일명은 latest.csv로 고정
    df.to_csv("out/latest.csv", index=False, encoding="utf-8-sig")
    # 일자별 백업
    df.to_csv(f"out/krx_{datetime.now(KST).strftime('%Y%m%d')}.csv", index=False, encoding="utf-8-sig")
    print(df)
