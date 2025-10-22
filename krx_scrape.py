# krx_scrape.py  v4 (frames 지원)
from playwright.sync_api import sync_playwright
import pandas as pd, re, os
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
def now_kst(): return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
os.makedirs("out", exist_ok=True)

def to_float(s):
    m = re.findall(r"[0-9]+(?:\.[0-9]+)?", s.replace(",", ""))
    return float(m[0]) if m else None

def to_int(s):
    m = re.findall(r"[0-9]+", s.replace(",", ""))
    return int(m[0]) if m else None

def first_number_in(locator, low=None, high=None):
    n = locator.count()
    for i in range(min(n, 80)):
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

def grab_index_in_frame(fr, label, low, high):
    lab = fr.locator(f"xpath=//*[contains(normalize-space(.), '{label}')]").first
    if lab.count()==0:
        return None
    anc = lab.locator("xpath=ancestor::*[position()<=4]").first
    cand = anc.locator("xpath=.//span|.//strong|.//em|.//*[contains(@class,'num')]")
    return first_number_in(cand, low, high)

def grab_breadth_in_frame(fr, word):
    w = fr.locator(f"xpath=//*[contains(normalize-space(.), '{word}')]").first
    if w.count()==0:
        return None
    near = w.locator("xpath=following::*[self::span or self::strong or self::em or contains(@class,'num')][position()<=8]")
    n = near.count()
    for i in range(n):
        s = near.nth(i).inner_text()
        if re.search(r"\d", s):
            v = to_int(s)
            if v is not None and v < 20000:
                return v
    return None

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context()
    page = ctx.new_page()
    page.goto("https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd?locale=ko",
              timeout=120_000, wait_until="networkidle")
    page.wait_for_timeout(6000)  # 로딩 여유

    # 탐색 대상: 메인 + 모든 iframe
    frames = [page] + page.frames

    kospi = kosdaq = adv = dec = unch = None

    # 1) 지수
    for fr in frames:
        kospi  = kospi  or grab_index_in_frame(fr, "코스피", 100.0, 5000.0) or grab_index_in_frame(fr, "KOSPI", 100.0, 5000.0)
        kosdaq = kosdaq or grab_index_in_frame(fr, "코스닥", 100.0, 2000.0) or grab_index_in_frame(fr, "KOSDAQ", 100.0, 2000.0)
    # 2) 시장 폭
    for fr in frames:
        adv  = adv  or grab_breadth_in_frame(fr, "상승")
        dec  = dec  or grab_breadth_in_frame(fr, "하락")
        unch = unch or grab_breadth_in_frame(fr, "보합")

    row = {
        "time_kst": now_kst(), "source": "KRX_DOM",
        "kospi": kospi, "kosdaq": kosdaq, "adv": adv, "dec": dec, "unch": unch
    }
    df = pd.DataFrame([row])

    # 디버그: 각 프레임 HTML 저장(문제시 확인)
    for i, fr in enumerate(frames[:6]):  # 최대 6개만
        try:
            html = fr.content()
            with open(f"out/frame_{i}.html", "w", encoding="utf-8") as f:
                f.write(html)
        except:
            pass

    # 산출물
    df.to_csv("out/latest.csv", index=False, encoding="utf-8-sig")
    df.to_csv(f"out/krx_{datetime.now(KST).strftime('%Y%m%d')}.csv", index=False, encoding="utf-8-sig")
    print(df)

