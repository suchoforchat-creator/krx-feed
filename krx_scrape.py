# krx_scrape.py  v6 (KRX 우선, 실패시 NAVER 폴백+스크린샷, 값검증)
from playwright.sync_api import sync_playwright
import pandas as pd, re, os, time
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
def now_kst(): return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
def ymd(): return datetime.now(KST).strftime("%Y%m%d")
os.makedirs("out", exist_ok=True)

def to_float(s):
    m = re.findall(r"[0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?", s)
    return float(m[0].replace(",","")) if m else None
def to_int(s):
    m = re.findall(r"[0-9]{1,3}(?:,[0-9]{3})*", s)
    return int(m[0].replace(",","")) if m else None
def valid(row):
    return isinstance(row.get("kospi"), (int,float)) and isinstance(row.get("kosdaq"), (int,float))

# ---------- NAVER 폴백(Playwright 사용) ----------
def parse_naver_with_pw(ctx):
    row = {"time_kst": now_kst(), "source": "NAVER_SEC 잠정(secondary)",
           "kospi": None, "kosdaq": None, "adv": None, "dec": None, "unch": None}
    p1 = ctx.new_page()
    p1.goto("https://finance.naver.com/sise/sise_index.nhn?code=KOSPI",
            timeout=120_000, wait_until="domcontentloaded")
    p1.wait_for_timeout(2500)
    p1.screenshot(path="out/naver_kospi.png", full_page=True)
    # 지수
    try:
        v = p1.locator(".no_today .blind").first.inner_text().strip()
        row["kospi"] = to_float(v)
    except: pass
    # 폭(상승/보합/하락) – 텍스트에서 정규식
    body_txt = p1.locator("body").inner_text()
    up  = re.search(r"상승\s*([0-9,]+)\s*종목", body_txt)
    flt = re.search(r"보합\s*([0-9,]+)\s*종목", body_txt)
    dn  = re.search(r"하락\s*([0-9,]+)\s*종목", body_txt)
    row["adv"]  = int(up.group(1).replace(",",""))  if up  else None
    row["unch"] = int(flt.group(1).replace(",","")) if flt else None
    row["dec"]  = int(dn.group(1).replace(",",""))  if dn  else None
    p1.close()

    p2 = ctx.new_page()
    p2.goto("https://finance.naver.com/sise/sise_index.nhn?code=KOSDAQ",
            timeout=120_000, wait_until="domcontentloaded")
    p2.wait_for_timeout(2500)
    p2.screenshot(path="out/naver_kosdaq.png", full_page=True)
    try:
        v = p2.locator(".no_today .blind").first.inner_text().strip()
        row["kosdaq"] = to_float(v)
    except: pass
    p2.close()
    return row

# ---------- KRX ----------
def first_num(locator, low=None, high=None, limit=120):
    n = locator.count()
    for i in range(min(n, limit)):
        try:
            v = to_float(locator.nth(i).inner_text().strip())
        except:
            v = None
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
    return first_num(cand, low, high)

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
            if v and 0 < v < 20000:
                return v
    return None

def scrape_krx(ctx):
    page = ctx.new_page()
    page.goto("https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd?locale=ko",
              timeout=120_000, wait_until="networkidle")
    page.wait_for_timeout(6000)
    page.screenshot(path="out/krx_page.png", full_page=True)
    if "Service unavailable" in page.locator("body").inner_text():
        raise RuntimeError("KRX service unavailable")
    frames = [page] + page.frames
    kospi = kosdaq = adv = dec = unch = None
    for fr in frames:
        kospi  = kospi  or grab_index(fr, "코스피", 100.0, 5000.0) or grab_index(fr, "KOSPI", 100.0, 5000.0)
        kosdaq = kosdaq or grab_index(fr, "코스닥", 100.0, 2000.0) or grab_index(fr, "KOSDAQ", 100.0, 2000.0)
    for fr in frames:
        adv  = adv  or grab_breadth(fr, "상승")
        dec  = dec  or grab_breadth(fr, "하락")
        unch = unch or grab_breadth(fr, "보합")
    page.close()
    return {"time_kst": now_kst(), "source": "KRX_DOM",
            "kospi": kospi, "kosdaq": kosdaq, "adv": adv, "dec": dec, "unch": unch}

# ---------- MAIN ----------
with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context()

    row = None
    # KRX 2회 재시도
    for i in range(2):
        try:
            row = scrape_krx(ctx)
            if valid(row):
                break
            else:
                raise ValueError("KRX values empty")
        except Exception:
            time.sleep(60)
    # 폴백
    if row is None or not valid(row):
        row = parse_naver_with_pw(ctx)

    df = pd.DataFrame([row])
    df.to_csv("out/latest.csv", index=False, encoding="utf-8-sig")
    df.to_csv(f"out/krx_{ymd()}.csv", index=False, encoding="utf-8-sig")
    print(df)
