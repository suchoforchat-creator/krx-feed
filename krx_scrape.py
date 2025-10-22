# krx_scrape.py
# -*- coding: utf-8 -*-
import os, re, csv
import datetime as dt
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUT="out"; HTML=os.path.join(OUT,"html")
os.makedirs(OUT, exist_ok=True); os.makedirs(HTML, exist_ok=True)

def now_kst():
    return (dt.datetime.utcnow()+dt.timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")

# robust session
S=requests.Session()
S.headers.update({
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept-Language":"ko-KR,ko;q=0.9",
    "Referer":"https://m.stock.naver.com/"
})
retry = Retry(total=4, backoff_factor=0.8, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
S.mount("https://", HTTPAdapter(max_retries=retry)); S.mount("http://", HTTPAdapter(max_retries=retry))

def save_html(name, text):
    path=os.path.join(HTML, name)
    with open(path,"w",encoding="utf-8") as f: f.write(text)
    return path

def fnum(x):
    if x is None: return None
    try: return float(x.replace(",","").strip())
    except: return None

def get(url, name):
    r=S.get(url, timeout=25); r.raise_for_status()
    save_html(name+".html", r.text)
    return r.text

# --------- KOSPI/KOSDAQ + 등락/보합 ----------
def parse_korea():
    out={}
    targets={
        "kospi":[
            ("https://stockpay.naver.com/domestic/index/KOSPI","np_kospi"),
            ("https://finance.naver.com/sise/sise_index.naver?code=KOSPI","nf_kospi")
        ],
        "kosdaq":[
            ("https://stockpay.naver.com/domestic/index/KOSDAQ","np_kosdaq"),
            ("https://finance.naver.com/sise/sise_index.naver?code=KOSDAQ","nf_kosdaq")
        ]
    }
    for key, cand in targets.items():
        html=None; src="NONE"
        for u,tag in cand:
            try:
                html=get(u, tag); src=u; break
            except Exception:
                continue
        idx=adv=dec=unch=None
        if html:
            if "stockpay.naver.com" in src:
                # 지수
                m=re.search(r"지수\s*</span>\s*<strong[^>]*>\s*([\d,]+\.\d+|\d{1,3}(?:,\d{3})*)", html, re.S)
                if m: idx=fnum(m.group(1))
                # 등락/보합(텍스트 블록 전역 탐색)
                m2=re.search(r"상승[^0-9]*([0-9,]+).*?하락[^0-9]*([0-9,]+).*?보합[^0-9]*([0-9,]+)", html, re.S)
                if m2:
                    adv=int(m2.group(1).replace(",",""))
                    dec=int(m2.group(2).replace(",",""))
                    unch=int(m2.group(3).replace(",",""))
            else:
                soup=BeautifulSoup(html,"lxml")
                v=soup.select_one("#now_value,.no_today .blind")
                if v: idx=fnum(v.get_text())
                blob=" ".join(t.get_text(" ") for t in soup.select("body"))
                m3=re.search(r"상승[^0-9]*([0-9,]+).*?하락[^0-9]*([0-9,]+).*?보합[^0-9]*([0-9,]+)", blob, re.S)
                if m3:
                    adv=int(m3.group(1).replace(",",""))
                    dec=int(m3.group(2).replace(",",""))
                    unch=int(m3.group(3).replace(",",""))
        out[key]={"index":idx,"adv":adv,"dec":dec,"unch":unch,"source":"NAVER_PAY" if "stockpay" in src else ("NAVER_FIN" if "finance.naver" in src else "NONE")}
    return out

# --------- USD/KRW + (WTI, 국제금) from “주요 시세표” + 폴백 ----------
def parse_fx_wti_gold():
    usd=gold=wti=None; src="NONE"; raw=None
    # 1) 네이버페이 환율 상세
    try:
        raw=get("https://stockpay.naver.com/market-index/USDKRW","np_usdkrw")
        src="NAVER_PAY"
        # 큰 숫자
        m=re.search(r"USDKRW.+?([\d,]+\.\d+|\d{1,3}(?:,\d{3})*)\s*원", raw, re.S)
        if m: usd=fnum(m.group(1))
        # 주요 시세표 블록에서 WTI, 국제 금
        # WTI
        m_w=re.search(r">WTI<[^0-9]+([\d,]+\.\d+|\d{1,3}(?:,\d{3})*)", raw, re.S)
        if m_w: wti=fnum(m_w.group(1))
        # 국제 금
        m_g=re.search(r"(국제\s*금|Gold)[^0-9]+([\d,]+\.\d+|\d{1,3}(?:,\d{3})*)", raw, re.S)
        if m_g: gold=fnum(m_g.group(2))
    except Exception:
        pass
    # 2) 폴백: 네이버파이낸스 환율 상세
    if usd is None:
        try:
            h=get("https://finance.naver.com/marketindex/exchangeDetail.naver?marketindexCd=FX_USDKRW","nf_usdkrw")
            src="NAVER_FIN"
            soup=BeautifulSoup(h,"lxml")
            v=soup.select_one(".no_today .blind")
            if v: usd=fnum(v.get_text())
        except Exception:
            pass
    # 3) 폴백: WTI/Gold 네이버파이낸스 상세
    if wti is None:
        try:
            h=get("https://finance.naver.com/marketindex/oilDetail.naver?marketindexCd=OIL_CL","nf_wti")
            soup=BeautifulSoup(h,"lxml")
            v=soup.select_one(".no_today .blind")
            if v: wti=fnum(v.get_text())
        except Exception:
            pass
    if gold is None:
        try:
            h=get("https://finance.naver.com/marketindex/goldDetail.naver?marketindexCd=CMDT_GC","nf_gold")
            soup=BeautifulSoup(h,"lxml")
            v=soup.select_one(".no_today .blind")
            if v: gold=fnum(v.get_text())
        except Exception:
            pass
    return usd, wti, gold, src

def write_csv(rows, name):
    p=os.path.join(OUT, name)
    with open(p,"w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); [w.writerow(r) for r in rows]
    return p

def main():
    t=now_kst()
    kr=parse_korea()
    usd, wti, gold, fxsrc = parse_fx_wti_gold()
    row={
        "time_kst":t,
        "kospi":kr["kospi"]["index"], "kosdaq":kr["kosdaq"]["index"],
        "adv":kr["kosdaq"]["adv"], "dec":kr["kosdaq"]["dec"], "unch":kr["kosdaq"]["unch"],
        "usdkrw":usd, "wti_usd":wti, "gold_usd":gold,
        "src_kospi":kr["kospi"]["source"], "src_kosdaq":kr["kosdaq"]["source"], "src_fx":fxsrc
    }
    write_csv([row], f"krx_{dt.datetime.utcnow().date().strftime('%Y%m%d')}.csv")
    write_csv([row], "latest.csv")
    print("OK", row)

if __name__=="__main__":
    main()
