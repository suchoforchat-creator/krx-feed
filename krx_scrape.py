# krx_scrape.py v8  (KRX 우선, NAVER 폴백: 라벨근처+범위필터, 스크린샷/원문 덤프)
from playwright.sync_api import sync_playwright
import pandas as pd, re, os, time
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
def now_kst(): return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
def ymd(): return datetime.now(KST).strftime("%Y%m%d")
os.makedirs("out", exist_ok=True)

def valid(row):
    return isinstance(row.get("kospi"), (int,float)) and isinstance(row.get("kosdaq"), (int,float))

# ---------- helpers ----------
NUM_RE = re.compile(r"[0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?")

def first_in_range(text, low, high):
    for m in NUM_RE.findall(text):
        v = float(m.replace(",",""))
        if low <= v <= high:
            return v
    return None

def slice_near(label, text, span=2000):
    i = text.find(label)
    if i == -1: return text
    return text[max(0, i-100): i+span]

# ---------- NAVER fallback ----------
def parse_naver_with_pw(ctx):
    row = {"time_kst": now_kst(), "source": "NAVER_SEC 잠정(secondary)",
           "kospi": None, "kosdaq": None, "adv": None, "dec": None, "unch": None}

    def load(url, ss_path, txt_path):
        p = ctx.new_page()
        p.goto(url, timeout=120_000, wait_until="domcontentloaded")
        p.wait_for_timeout(2500)
        p.screenshot(path=ss_path, full_page=True)
        body = p.locator("body").inner_text()
        with open(txt_path, "w", encoding="utf-8") as f: f.write(body)
        p.close()
        return body

    body_k = load("https://finance.naver.com/sise/sise_index.nhn?code=KOSPI",
                  "out/naver_kospi.png",  "out/naver_kospi.txt")
    body_q = load("https://finance.naver.com/sise/sise_index.nhn?code=KOSDAQ",
                  "out/naver_kosdaq.png", "out/naver_kosdaq.txt")

    # 지수: 라벨 주변에서 합리적 범위의 첫 숫자만 채택
    seg_k = slice_near("코스피", body_k)
    seg_q = slice_near("코스닥", body_q)
    row["kospi"]  = first_in_range(seg_k, 600, 5000)   # KOSPI 필터
    row["kosdaq"] = first_in_range(seg_q, 300, 2000)   # KOSDAQ 필터

    # 등락 종목수
    def breadth(txt):
        seg = slice_near("등락", txt, span=600)
        up  = re.search(r"상승\s*([0-9,]+)", seg)
        flt = re.search(r"보합\s*([0-9,]+)", seg)
        dn  = re.search(r"하락\s*([0-9,]+)", seg)
        adv  = int(up.group(1).replace(",",""))  if up  else None
        unch = int(flt.group(1).replace(",","")) if flt else None
        dec  = int(dn.group(1).replace(",",""))  if dn  else None
        return adv, dec, unch

    row["adv"], row["dec"], row["unch"] = breadth(body_k)
    return row

# ---------- KRX ----------
def first_num(locator, low=None, high=None, limit=120):
    n = locator.count()
    for i in range(min(n, limit)):
        try:
            s = locator.nth(i).inner_text()
            m = NUM_RE.search(s)
            v = float(m.group(0).replace(",","")) if m else None
        except: v = None
        if v is None: continue
        if low is not None and v < low: continue
        if high is not None and v > high: continue
        return v
    return None

def grab_index(fr, label, low, high):
    lab = fr.locator(f"xpath=//*[contains(normalize-space(.), '{label}')]").first
    if lab.count()==0: return None
    anc = lab.locator("xpath=ancestor::*[position()<=5]").first
    cand = anc.locator("xpath=.//span|.//strong|.//em|.//*[contains(@class,'num') or contains(@class,'point')]")
    return first_num(cand, low, high)

def grab_breadth(fr, word):
    w = fr.locator(f"xpath=//*[contains(normalize-space(.), '{word}')]").first
    if w.count()==0: return None
    near = w.locator("xpath=following::*[self::span or self::strong or self::em or contains(@class,'num')][position()<=12]")
    for i in range(near.count()):
        s = near.nth(i).inner_text()
        m = re.search(r"[0-9]{1,3}(?:,[0-9]{3})*", s)
        if m:
            v = int(m.group(0).replace(",",""))
            if 0 < v < 20000: return v
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
        kospi  = kospi  or grab_index(fr, "코스피", 600, 5000) or grab_index(fr, "KOSPI", 600, 5000)
        kosdaq = kosdaq or grab_index(fr, "코스닥", 300, 2000) or grab_index(fr, "KOSDAQ", 300, 2000)
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
    for _ in range(2):
        try:
            row = scrape_krx(ctx)
            if valid(row): break
            else: raise ValueError("KRX values empty")
        except Exception:
            time.sleep(60)

    if row is None or not valid(row):
        row = parse_naver_with_pw(ctx)

    df = pd.DataFrame([row])
    df.to_csv("out/latest.csv", index=False, encoding="utf-8-sig")
    df.to_csv(f"out/krx_{ymd()}.csv", index=False, encoding="utf-8-sig")
    print(df)

