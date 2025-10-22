# krx_scrape.py  v5
from playwright.sync_api import sync_playwright
import pandas as pd, re, os, json
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
    for i in range(min(n, 120)):
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

def grab_index(fr, label, low, high):
    lab = fr.locator(f"xpath=//*[contains(normalize-space(.), '{label}')]").first
    if lab.count()==0:
        return None
    anc = lab.locator("xpath=ancestor::*[position()<=5]").first
    cand = anc.locator("xpath=.//span|.//strong|.//em|.//*[contains(@class,'num') or contains(@class,'point')]")
    return first_number_in(cand, low, high)

def grab_breadth(fr, word):
    w = fr.locator(f"xpath=//*[contains(normalize-space(.), '{word}')]").first
    if w.count()==0:
        return None
    near = w.locator("xpath=following::*[self::span or self::strong or self::em or contains(@class,'num')][position()<=12]")
    n = near.count()
    for i in range(n):
        s = near.nth(i).inner_text()
        if re.search(r"\d", s):
            v = to_int(s)
            if v is not None and 0 < v < 20000:
                return v
    return None

import re, requests
from bs4 import BeautifulSoup

def parse_naver_index(url):
    r = requests.get(url, timeout=20, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    # 지수
    idx_txt = s.select_one(".no_today .blind")
    idx = float(idx_txt.text.replace(",","")) if idx_txt else None
    # 등락 종목수(상승/보합/하락)
    html = s.get_text(" ")
    m_up   = re.search(r"상승\s*([0-9,]+)\s*종목", html)
    m_flat = re.search(r"보합\s*([0-9,]+)\s*종목", html)
    m_dn   = re.search(r"하락\s*([0-9,]+)\s*종목", html)
    up   = int(m_up.group(1).replace(",",""))   if m_up else None
    flat = int(m_flat.group(1).replace(",","")) if m_flat else None
    dn   = int(m_dn.group(1).replace(",",""))   if m_dn else None
    return idx, up, dn, flat

def fallback_naver():
    k_idx, k_up, k_dn, k_flat = parse_naver_index("https://finance.naver.com/sise/sise_index.nhn?code=KOSPI")
    q_idx, q_up, q_dn, q_flat = parse_naver_index("https://finance.naver.com/sise/sise_index.nhn?code=KOSDAQ")
    return {
        "source":"NAVER_SEC 잠정(secondary)",
        "kospi":k_idx,"kosdaq":q_idx,
        "adv":k_up if k_up is not None else None,
        "dec":k_dn if k_dn is not None else None,
        "unch":k_flat if k_flat is not None else None
    }


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context()
    page = ctx.new_page()
    page.goto("https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd?locale=ko",
              timeout=120_000, wait_until="networkidle")
    page.wait_for_timeout(7000)  # 로딩 여유

    # 디버그: 페이지 스크린샷/프레임 목록
    try:
        page.screenshot(path="out/page.png", full_page=True)
    except: pass
    frames = [page] + page.frames
    with open("out/frames.txt", "w", encoding="utf-8") as f:
        for i, fr in enumerate(frames):
            try:
                f.write(f"[{i}] {getattr(fr, 'url', 'page')}\n")
            except:
                pass

    # 각 프레임 HTML 저장(확인용)
    for i, fr in enumerate(frames[:8]):
        try:
            html = fr.content()
            with open(f"out/frame_{i}.html", "w", encoding="utf-8") as fh:
                fh.write(html)
        except:
            continue

    kospi = kosdaq = adv = dec = unch = None
    for fr in frames:
        kospi  = kospi  or grab_index(fr, "코스피", 100.0, 5000.0) or grab_index(fr, "KOSPI", 100.0, 5000.0)
        kosdaq = kosdaq or grab_index(fr, "코스닥", 100.0, 2000.0) or grab_index(fr, "KOSDAQ", 100.0, 2000.0)
    for fr in frames:
        adv  = adv  or grab_breadth(fr, "상승")
        dec  = dec  or grab_breadth(fr, "하락")
        unch = unch or grab_breadth(fr, "보합")

    try:
        row = {"time_kst": now_kst(), "source": "KRX_DOM",
               "kospi": kospi, "kosdaq": kosdaq, "adv": adv, "dec": dec, "unch": unch}
    except exception:
        row = {"time_kst": now_kst(), **fallback_naver()}
    df = pd.DataFrame([row])
    df.to_csv("out/latest.csv", index=False, encoding="utf-8-sig")
    df.to_csv(f"out/krx_{datetime.now(KST).strftime('%Y%m%d')}.csv", index=False, encoding="utf-8-sig")
    print(df)
