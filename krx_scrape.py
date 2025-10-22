# -*- coding: utf-8 -*-
import os, re, csv, time, datetime as dt
import requests
from bs4 import BeautifulSoup

OUTDIR = "out"
os.makedirs(OUTDIR, exist_ok=True)
HTMLDIR = os.path.join(OUTDIR, "html"); os.makedirs(HTMLDIR, exist_ok=True)

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept-Language": "ko-KR,ko;q=0.9"})

def now_kst():
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def save_html(name, text):
    path = os.path.join(HTMLDIR, name)
    with open(path, "w", encoding="utf-8") as f: f.write(text)
    return path

def fetch(url, name_hint):
    r = S.get(url, timeout=20)
    r.raise_for_status()
    save_html(f"{name_hint}.html", r.text)
    return r.text

def to_float(num_str):
    """쉼표 제거. 소수점 보존. 실패 시 None."""
    if not num_str: return None
    s = num_str.replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None

def find_first(text, patterns):
    for p in patterns:
        m = re.search(p, text, re.S)
        if m:
            return m.group(1)
    return None

# --- KOSPI/KOSDAQ + 등락/보합 ---
def parse_kospi_kosdaq():
    srcs = {
        "kospi": "https://stockpay.naver.com/domestic/index/KOSPI",
        "kosdaq": "https://stockpay.naver.com/domestic/index/KOSDAQ",
    }
    out = {}
    for key, url in srcs.items():
        raw = fetch(url, f"naver_{key}")
        # 지수값
        val = find_first(raw, [
            r"지수\s*</span>\s*<strong[^>]*>\s*([\d,]+\.\d+)",
            r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*</strong>\s*<span[^>]*>\s*전일대비"
        ])
        # 등락/보합
        adv = find_first(raw, [r"상승\s*</em>\s*<strong[^>]*>\s*([\d,]+)"])
        dec = find_first(raw, [r"하락\s*</em>\s*<strong[^>]*>\s*([\d,]+)"])
        unch = find_first(raw, [r"보합\s*</em>\s*<strong[^>]*>\s*([\d,]+)"])
        out[key] = {
            "index": to_float(val),
            "adv": int(adv.replace(",","")) if adv else None,
            "dec": int(dec.replace(",","")) if dec else None,
            "unch": int(unch.replace(",","")) if unch else None,
            "source": "NAVER_PAY"
        }
    return out

# --- USD/KRW, 금, 유가(두바이) ---
def parse_usd_krw_gold_oil():
    url = "https://stockpay.naver.com/market-index/USDKRW"
    raw = fetch(url, "naver_usdkrw")

    usdkrw = find_first(raw, [
        r"USDKRW[^<]{0,200}?([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*원",
        r"환율[^<]{0,60}?([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*원"
    ])

    gold = find_first(raw, [
        r"국제\s*금[^0-9]{0,30}([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*달러"
    ])

    dubai = find_first(raw, [
        r"두바이유[^0-9]{0,30}([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*달러"
    ])

    return {
        "usdk_rw": to_float(usdkrw),
        "gold_usd": to_float(gold),
        "dubai_usd": to_float(dubai),
        "source": "NAVER_PAY"
    }

# --- 결과 저장 ---
def write_csv(rows, fname):
    path = os.path.join(OUTDIR, fname)
    hdr = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=hdr); w.writeheader()
        for r in rows: w.writerow(r)
    return path

def main():
    t = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    kos = parse_kospi_kosdaq()
    fx = parse_usd_krw_gold_oil()

    row = {
        "time_kst": t,
        "source": "NAVER_PAY",
        "kospi": kos["kospi"]["index"],
        "kosdaq": kos["kosdaq"]["index"],
        "adv": kos["kosdaq"]["adv"],  # 프리게이트/브리핑에 breadth로 사용
        "dec": kos["kosdaq"]["dec"],
        "unch": kos["kosdaq"]["unch"],
        "usdkrw": fx["usdk_rw"],
        "gold_usd": fx["gold_usd"],
        "dubai_usd": fx["dubai_usd"]
    }

    daily = f"krx_{now_kst():%Y%m%d}.csv"
    write_csv([row], daily)
    write_csv([row], "latest.csv")
    print("OK", row)

if __name__ == "__main__":
    main()
