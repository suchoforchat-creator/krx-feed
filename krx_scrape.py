# krx_scrape.py  v7  (KRX 우선, NAVER 폴백: 텍스트 정규식 파싱+스크린샷)
from playwright.sync_api import sync_playwright
import pandas as pd, re, os, time
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
def now_kst(): return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
def ymd(): return datetime.now(KST).strftime("%Y%m%d")
os.makedirs("out", exist_ok=True)

def to_float_num(s):
    m = re.search(r"[0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?", s)
    return float(m.group(0).replace(",", "")) if m else None
def to_int_num(s):
    m = re.search(r"[0-9]{1,3}(?:,[0-9]{3})*", s)
    return int(m.group(0).replace(",", "")) if m else None

def valid(row):
    return isinstance(row.get("kospi"), (int,float)) and isinstance(row.get("kosdaq"), (int,float))

# ---------- NAVER 폴백(본문 텍스트 정규식) ----------
def parse_naver_with_pw(ctx):
    row = {"time_kst": now_kst(), "source": "NAVER_SEC 잠정(secondary)",
           "kospi": None, "kosdaq": None, "adv": None, "dec": None, "unch": None}

    def extract_from_page(url, ss_path):
        p = ctx.new_page()
        p.goto(url, timeout=120_000, wait_until="domcontentloaded")
        p.wait_for_timeout(2500)
        p.screenshot(path=ss_path, full_page=True)
        body = p.locator("body").inner_text()
        p.close()
        return body

    body_k = extract_from_page(
        "https://finance.naver.com/sise/sise_index.nhn?code=KOSPI",
        "out/naver_kospi.png"
    )
    body_q = extract_from_page(
        "https://finance.naver.com/sise/sise_index.nhn?code=KOSDAQ",
        "out/naver_kosdaq.png"
    )

    # 코스피/코스닥 지수: "코스피 3,883.68 ▲ ..." 형태를 직접 매칭
    m_k = re.search(r"코스피[^\d]*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)", body_k)
    m_q = re.search(r"코스닥[^\d]*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)", body_q)
    row["kospi"]  = float(m_k.group(1).replace(",", "")) if m_k else None
    row["kosdaq"] = float(m_q.group(1).replace(",", "")) if m_q else None

    # 등락 종목수: "등락 ... 상승 123 보합 45 하락 67" 근처 300자 범위에서 추출
    def breadth_from_text(txt):
        seg = ""
        pos = txt.find("등락")
        if pos != -1:
            seg = txt[pos:pos+400]
        else:
            seg = txt  # 최후 수단
        up  = re.search(r"상승\s*([0-9,]+)(?:\s*종목)?", seg)
        flt = re.search(r"보합\s*([0-9,]+)(?:\s*종목)?", seg)
        dn  = re.search(r"하락\s*([0-9,]+)(?:\s*종목)?", seg)
        adv  = int(up.group(1).replace(",", ""))  if up  else None
        unch = int(flt.group(1).replace(",", "")) if flt else None
        dec  = int(dn.group(1).replace(",", ""))  if dn  else None
        return adv, dec, unch

    adv, dec, unch = breadth_from_text(body_k)
    row["adv"], row["dec"], row["unch"] = adv, dec, unch
    return row

# ---------- KRX ----------
def first_num(locator, low=None, high=None, limit=120):
    n = locator.count()
    for i in range(min(n, limit)):
        try:
            v = to_float_num(locator.nth(i).inner_text())
        except:
            v = None
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
        v = to_int_num(near.nth(i).inner_text())
        if v and 0 < v < 20000: return v
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
