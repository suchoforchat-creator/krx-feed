# krx_scrape.py  (KRX 우선, 실패시 NAVER 폴백)
from playwright.sync_api import sync_playwright
import pandas as pd, re, os, requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta

# ---------- 공통 ----------
KST = timezone(timedelta(hours=9))
def now_kst() -> str: return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
os.makedirs("out", exist_ok=True)

def to_float(s: str):
    m = re.findall(r"[0-9]+(?:\.[0-9]+)?", s.replace(",", ""))
    return float(m[0]) if m else None

def to_int(s: str):
    m = re.findall(r"[0-9]+", s.replace(",", ""))
    return int(m[0]) if m else None

# ---------- NAVER 폴백 ----------
HEADERS = {"User-Agent":"Mozilla/5.0"}
def parse_naver_index(url: str):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    idx_el = s.select_one(".no_today .blind")
    idx = float(idx_el.text.replace(",","")) if idx_el else None
    txt = s.get_text(" ")
    up   = to_int(re.search(r"상승\s*([0-9,]+)\s*종목", txt).group(1))   if re.search(r"상승\s*([0-9,]+)\s*종목", txt) else None
    flat = to_int(re.search(r"보합\s*([0-9,]+)\s*종목", txt).group(1))   if re.search(r"보합\s*([0-9,]+)\s*종목", txt) else None
    dn   = to_int(re.search(r"하락\s*([0-9,]+)\s*종목", txt).group(1))   if re.search(r"하락\s*([0-9,]+)\s*종목", txt) else None
    return idx, up, dn, flat

def fallback_naver_row():
    k_idx, k_up, k_dn, k_flat = parse_naver_index("https://finance.naver.com/sise/sise_index.nhn?code=KOSPI")
    q_idx, q_up, q_dn, q_flat = parse_naver_index("https://finance.naver.com/sise/sise_index.nhn?code=KOSDAQ")
    return {
        "time_kst": now_kst(),
        "source": "NAVER_SEC 잠정(secondary)",
        "kospi": k_idx, "kosdaq": q_idx,
        "adv": k_up, "dec": k_dn, "unch": k_flat
    }

# ---------- KRX 시도 ----------
def first_number_in(locator, low=None, high=None):
    n = locator.count()
    for i in range(min(n, 120)):
        v = to_float(locator.nth(i).inner_text().strip())
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
    return first_number_in(cand, low, high)

def grab_breadth(fr, word):
    w = fr.locator(f"xpath=//*[contains(normalize-space(.), '{word}')]").first
    if w.count()==0: return None
    near = w.locator("xpath=following::*[self::span or self::strong or self::em or contains(@class,'num')][position()<=12]")
    for i in range(near.count()):
        s = near.nth(i).inner_text()
        if re.search(r"\d", s):
            v = to_int(s)
            if v and 0 < v < 20000: return v
    return None

def try_krx_row():
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        ctx = b.new_context()
        page = ctx.new_page()
        page.goto("https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd?locale=ko",
                  timeout=120_000, wait_until="networkidle")
        page.wait_for_timeout(7000)

        # 장애 화면 감지
        full_txt = page.content()
        if "Service unavailable" in full_txt:
            raise RuntimeError("KRX Service unavailable")

        frames = [page] + page.frames
        kospi = kosdaq = adv = dec = unch = None

        for fr in frames:
            kospi  = kospi  or grab_index(fr, "코스피", 100.0, 5000.0) or grab_index(fr, "KOSPI", 100.0, 5000.0)
            kosdaq = kosdaq or grab_index(fr, "코스닥", 100.0, 2000.0) or grab_index(fr, "KOSDAQ", 100.0, 2000.0)
        for fr in frames:
            adv  = adv  or grab_breadth(fr, "상승")
            dec  = dec  or grab_breadth(fr, "하락")
            unch = unch or grab_breadth(fr, "보합")

        # 핵심 값이 비면 실패로 간주
        if kospi is None and kosdaq is None:
            raise ValueError("KRX indices not found")

        return {
            "time_kst": now_kst(),
            "source": "KRX_DOM",
            "kospi": kospi, "kosdaq": kosdaq,
            "adv": adv, "dec": dec, "unch": unch
        }

# ---------- 메인 ----------
def save_row(row: dict):
    df = pd.DataFrame([row])
    df.to_csv("out/latest.csv", index=False, encoding="utf-8-sig")
    df.to_csv(f"out/krx_{datetime.now(KST).strftime('%Y%m%d')}.csv", index=False, encoding="utf-8-sig")
    print(df)

if __name__ == "__main__":
    try:
        row = try_krx_row()
        # 브레드스가 None이면 지수만 KRX, 폭은 NAVER로 보강
        if row.get("adv") is None or row.get("dec") is None or row.get("unch") is None:
            fb = fallback_naver_row()
            row["adv"], row["dec"], row["unch"] = fb["adv"], fb["dec"], fb["unch"]
            row["source"] += " + NAVER breadth"
    except Exception as e:
        row = fallback_naver_row()
        row["error"] = str(e)  # 디버그용
    save_row(row)
