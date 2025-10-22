# krx_scrape.py  v10
# 목적: 프리게이트/아침브리핑에 필요한 핵심 지표 수집
# 1차-잠정(네이버): KOSPI/KOSDAQ 지수+breadth, USD/KRW
# 2차-잠정(폴백): DXY, WTI, Brent, Gold, BTC, UST10Y, KR10Y
from playwright.sync_api import sync_playwright
import pandas as pd, re, os, time, json, requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
def now_kst(): return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
def ymd(): return datetime.now(KST).strftime("%Y%m%d")

os.makedirs("out", exist_ok=True)

NUM = re.compile(r"[0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?")
INT = re.compile(r"[0-9]{1,3}(?:,[0-9]{3})*")

def num_in(text, lo=None, hi=None):
    for m in NUM.findall(text):
        v = float(m.replace(",",""))
        if lo is not None and v < lo:  continue
        if hi is not None and v > hi:  continue
        return v
    return None

def int_in(text, lo=0, hi=20000):
    m = INT.search(text or "")
    if not m: return None
    v = int(m.group(0).replace(",",""))
    return v if lo <= v <= hi else None

def slice_near(label, text, span=1200):
    i = text.find(label)
    if i == -1: return text
    return text[max(0, i-120): i+span]

# -------- 네이버(1차-잠정) --------
def naver_index_and_breadth(ctx, code, label, lo, hi, ss_prefix):
    url = f"https://finance.naver.com/sise/sise_index.nhn?code={code}"
    p = ctx.new_page()
    p.goto(url, timeout=120_000, wait_until="domcontentloaded")
    p.wait_for_timeout(2500)
    body = p.locator("body").inner_text()
    html = p.content()
    p.screenshot(path=f"out/{ss_prefix}.png", full_page=True)
    with open(f"out/{ss_prefix}.txt","w",encoding="utf-8") as f: f.write(body)
    with open(f"out/{ss_prefix}.html","w",encoding="utf-8") as f: f.write(html)
    p.close()

    seg = slice_near(label, body)
    idx = num_in(seg, lo, hi)

    # 등락수: 본문/HTML에서 ‘상승/보합/하락’ 또는 ▲ ─ ▼ 기호 인식
    cand_segments = [
        slice_near("등락", body, 800), slice_near("상승", body, 800),
        slice_near("하락", body, 800), html
    ]
    adv = dec = unch = None
    for s in cand_segments:
        if adv is None:
            m = re.search(r"(?:상승)[^\d]{0,6}([0-9,]{1,6})", s) or re.search(r"▲\s*([0-9,]{1,6})", s)
            if m: adv = int(m.group(1).replace(",",""))
        if unch is None:
            m = re.search(r"(?:보합)[^\d]{0,6}([0-9,]{1,6})", s) or re.search(r"(?:■|━|─|＝)\s*([0-9,]{1,6})", s)
            if m: unch = int(m.group(1).replace(",",""))
        if dec is None:
            m = re.search(r"(?:하락)[^\d]{0,6}([0-9,]{1,6})", s) or re.search(r"▼\s*([0-9,]{1,6})", s)
            if m: dec = int(m.group(1).replace(",",""))

    return idx, adv, dec, unch

def naver_usdkrw(ctx):
    # 네이버 환율 상세
    url = "https://finance.naver.com/marketindex/exchangeDetail.nhn?marketindexCd=FX_USDKRW"
    p = ctx.new_page()
    p.goto(url, timeout=120_000, wait_until="domcontentloaded")
    p.wait_for_timeout(2000)
    p.screenshot(path="out/naver_usdkrw.png", full_page=True)
    body = p.locator("body").inner_text()
    html  = p.content()
    with open("out/naver_usdkrw.txt","w",encoding="utf-8") as f: f.write(body)
    with open("out/naver_usdkrw.html","w",encoding="utf-8") as f: f.write(html)
    p.close()
    # ".value"가 종종 비니 텍스트에서 근사 추출
    seg = slice_near("미국 USD", body) + " " + slice_near("달러", body) + " " + html
    val = num_in(seg, 500, 2000)
    return val

# -------- 2차-잠정 폴백(요약) --------
HDR = {"User-Agent":"Mozilla/5.0"}

def fetch_text(url):
    r = requests.get(url, timeout=15, headers=HDR)
    r.raise_for_status()
    return r.text

def sec_dxy():
    # MarketWatch 달러지수(지연)
    try:
        html = fetch_text("https://www.marketwatch.com/investing/index/dxy")
        v = num_in(html, 70, 120)
        return v, "잠정(secondary): MarketWatch"
    except: return None, "잠정(secondary): MarketWatch 실패"

def sec_wti():
    try:
        html = fetch_text("https://www.marketwatch.com/investing/future/crude%20oil%20-%20electronic")
        v = num_in(html, 20, 200)
        return v, "잠정(secondary): MarketWatch WTI"
    except: return None, "잠정(secondary): MW WTI 실패"

def sec_brent():
    try:
        html = fetch_text("https://www.marketwatch.com/investing/future/brent%20crude%20oil")
        v = num_in(html, 20, 200)
        return v, "잠정(secondary): MarketWatch Brent"
    except: return None, "잠정(secondary): MW Brent 실패"

def sec_gold():
    try:
        html = fetch_text("https://www.marketwatch.com/investing/future/gold")
        v = num_in(html, 500, 3000)
        return v, "잠정(secondary): MarketWatch Gold"
    except: return None, "잠정(secondary): MW Gold 실패"

def sec_btc():
    try:
        html = fetch_text("https://www.coindesk.com/price/bitcoin/")
        v = num_in(html.replace(",", ""), 100, 2000000)  # USD
        return v, "잠정(secondary): CoinDesk BTC"
    except: return None, "잠정(secondary): CoinDesk 실패"

def sec_ust10y():
    try:
        html = fetch_text("https://www.cnbc.com/quotes/US10Y")
        v = num_in(html, 0.0, 20.0)
        return v, "잠정(secondary): CNBC US10Y(%)"
    except: return None, "잠정(secondary): CNBC 실패"

def sec_kr10y():
    try:
        html = fetch_text("https://www.investing.com/rates-bonds/south-korea-10-year-bond-yield")
        v = num_in(html, 0.0, 20.0)
        return v, "잠정(secondary): Investing KR10Y(%)"
    except: return None, "잠정(secondary): Investing 실패"

# -------- 실행 --------
with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context()

    # 1) KRX 1차-잠정 by Naver
    kpi, kpi_up, kpi_dn, kpi_fl = naver_index_and_breadth(ctx, "KOSPI",  "코스피", 600, 5000, "naver_kospi")
    kqd, kqd_up, kqd_dn, kqd_fl = naver_index_and_breadth(ctx, "KOSDAQ", "코스닥", 300, 2000, "naver_kosdaq")
    usdkrw = naver_usdkrw(ctx)

    # 2) 2차-잠정 폴백들
    dxy,   src_dxy   = sec_dxy()
    wti,   src_wti   = sec_wti()
    brent, src_brent = sec_brent()
    gold,  src_gold  = sec_gold()
    btc,   src_btc   = sec_btc()
    us10y, src_us10y = sec_ust10y()
    kr10y, src_kr10y = sec_kr10y()

    row = {
        "time_kst": now_kst(),

        # KRX(네이버) 1차-잠정
        "kospi": kpi, "kospi_adv": kpi_up, "kospi_dec": kpi_dn, "kospi_unch": kpi_fl,
        "kosdaq": kqd, "kosdaq_adv": kqd_up, "kosdaq_dec": kqd_dn, "kosdaq_unch": kqd_fl,
        "usdkrw": usdkrw,

        # 2차-잠정 자산
        "dxy": dxy, "wti": wti, "brent": brent, "gold": gold, "btc_usd": btc,
        "us10y_pct": us10y, "kr10y_pct": kr10y,

        # 소스 라벨
        "source_krx": "NAVER_SEC 1차-잠정",
        "source_usdkrw": "NAVER_SEC 1차-잠정",
        "source_dxy": src_dxy, "source_wti": src_wti, "source_brent": src_brent,
        "source_gold": src_gold, "source_btc": src_btc,
        "source_us10y": src_us10y, "source_kr10y": src_kr10y
    }

    df = pd.DataFrame([row])
    df.to_csv("out/latest.csv", index=False, encoding="utf-8-sig")
    df.to_csv(f"out/brief_{ymd()}.csv", index=False, encoding="utf-8-sig")
    with open("out/latest.json","w",encoding="utf-8") as f:
        json.dump(row, f, ensure_ascii=False, indent=2)

    print(df)
