# krx_scrape.py  (교체본)
from playwright.sync_api import sync_playwright
import pandas as pd, re, os
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
def now_kst(): return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
os.makedirs("out", exist_ok=True)

def to_num(s):
    try: return float(re.sub(r"[^0-9.]", "", s))
    except: return None

def pick_number_near(page, label, rng=None):
    # label이 포함된 노드를 찾고, 같은 블록/인접 노드에서 숫자만 추출
    el = page.locator(f"xpath=//*[contains(normalize-space(text()), '{label}')]").first
    if el.count()==0: return None
    # 근처의 후보 6개만 조사
    for i in range(1,7):
        cand = el.locator(f"xpath=following::*[self::span or self::strong or self::em][{i}]")
        if cand.count()==0: continue
        v = to_num(cand.first.inner_text())
        if v is None: continue
        if rng and not (rng[0] <= v <= rng[1]):  # 비정상값(광고·메뉴 숫자 등) 필터
            continue
        return v
    return None

with sync_playwright() as p:
    b = p.chromium.launch(headless=True); c = b.new_context(); page = c.new_page()
    page.goto("https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd?locale=ko",
              timeout=120000, wait_until="networkidle")
    page.wait_for_timeout(2500)

    # 지수: 합리적 범위 필터 포함(오검출 방지)
    kospi  = pick_number_near(page, "코스피",  (100.0, 5000.0)) or pick_number_near(page,"KOSPI",(100.0,5000.0))
    kosdaq = pick_number_near(page, "코스닥", (100.0, 2000.0)) or pick_number_near(page,"KOSDAQ",(100.0,2000.0))

    # 브레드스: '상승/하락/보합' 숫자만 추출, 숫자 아니면 None
    def breadth_word(w):
        el = page.locator(f"xpath=//*[contains(normalize-space(text()), '{w}')]").first
        if el.count()==0: return None
        cand = el.locator("xpath=following::*[self::span or self::strong][1]")
        if cand.count()==0: return None
        s = cand.first.inner_text()
        if not re.search(r"\d", s): return None
        try: return int(re.sub(r"[^0-9]", "", s))
        except: return None

    adv  = breadth_word("상승")
    dec  = breadth_word("하락")
    unch = breadth_word("보합")

    row = {
        "time_kst": now_kst(), "source": "KRX_DOM",
        "kospi": kospi, "kosdaq": kosdaq, "adv": adv, "dec": dec, "unch": unch
    }
    df = pd.DataFrame([row])
    df.to_csv("out/최신.csv", index=False, encoding="utf-8-sig")
    df.to_csv(f"out/krx_{datetime.now(KST).strftime('%Y%m%d')}.csv", index=False, encoding="utf-8-sig")
    print(df)
