# -*- coding: utf-8 -*-
import os, re, csv
import datetime as dt
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTDIR="out"; os.makedirs(OUTDIR, exist_ok=True)
HTMLDIR=os.path.join(OUTDIR,"html"); os.makedirs(HTMLDIR, exist_ok=True)

def now_kst(): return (dt.datetime.utcnow()+dt.timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")

# session with retry
S=requests.Session()
S.headers.update({"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36","Accept-Language":"ko-KR,ko;q=0.9"})
retry=Retry(total=3, backoff_factor=0.8, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
S.mount("https://", HTTPAdapter(max_retries=retry)); S.mount("http://", HTTPAdapter(max_retries=retry))

def save_html(name, text):
    p=os.path.join(HTMLDIR,name); open(p,"w",encoding="utf-8").write(text); return p

def to_f(x):
    if not x: return None
    try: return float(x.replace(",","").strip())
    except: return None

def get(url, name):
    r=S.get(url, timeout=20); r.raise_for_status(); save_html(name+".html", r.text); return r.text

# ---------- KOSPI / KOSDAQ + 등락/보합 ----------
def parse_korea_indices():
    out={}
    # 1차: stockpay
    urls = {
        "kospi":[("https://stockpay.naver.com/domestic/index/KOSPI","naverpay_kospi"),
                 ("https://finance.naver.com/sise/sise_index.naver?code=KOSPI","fin_kospi")],
        "kosdaq":[("https://stockpay.naver.com/domestic/index/KOSDAQ","naverpay_kosdaq"),
                  ("https://finance.naver.com/sise/sise_index.naver?code=KOSDAQ","fin_kosdaq")]
    }
    for key, candidates in urls.items():
        html=None; used=None
        for u,n in candidates:
            try: html=get(u,n); used=u; break
            except Exception: continue
        if html is None:
            out[key]={"index":None,"adv":None,"dec":None,"unch":None,"source":"NONE"}; continue

        if "stockpay.naver.com" in used:
            # 지수
            idx=re.search(r"지수\s*</span>\s*<strong[^>]*>\s*([\d,]+\.\d+)", html)
            # 등락/보합
            adv=re.search(r"상승\s*</em>\s*<strong[^>]*>\s*([\d,]+)", html)
            dec=re.search(r"하락\s*</em>\s*<strong[^>]*>\s*([\d,]+)", html)
            unch=re.search(r"보합\s*</em>\s*<strong[^>]*>\s*([\d,]+)", html)
            out[key]={"index":to_f(idx.group(1) if idx else None),
                      "adv":int(adv.group(1).replace(",","")) if adv else None,
                      "dec":int(dec.group(1).replace(",","")) if dec else None,
                      "unch":int(unch.group(1).replace(",","")) if unch else None,
                      "source":"NAVER_PAY"}
        else:
            # finance.naver.com
            soup=BeautifulSoup(html,"lxml")
            # 지수
            hd=soup.select_one("#now_value") or soup.select_one(".no_today .blind")
            idx=to_f(hd.get_text()) if hd else None
            # 등락/보합 표(우측 '상승/하락/보합' 박스)
            box_text=" ".join(t.get_text(" ") for t in soup.select(".lst_kos_info, .sise_report"))
            adv_m=re.search(r"상승[^0-9]*([0-9,]+)", box_text)
            dec_m=re.search(r"하락[^0-9]*([0-9,]+)", box_text)
            unch_m=re.search(r"보합[^0-9]*([0-9,]+)", box_text)
            out[key]={"index":idx,
                      "adv":int(adv_m.group(1).replace(",","")) if adv_m else None,
                      "dec":int(dec_m.group(1).replace(",","")) if dec_m else None,
                      "unch":int(unch_m.group(1).replace(",","")) if unch_m else None,
                      "source":"NAVER_FIN"}
    return out

# ---------- USD/KRW ----------
def parse_usdkrw():
    # 1차 stockpay, 2차 finance
    for u,n in [("https://stockpay.naver.com/market-index/USDKRW","naverpay_usdkrw"),
                ("https://finance.naver.com/marketindex/exchangeDetail.naver?marketindexCd=FX_USDKRW","fin_usdkrw")]:
        try:
            html=get(u,n)
            if "stockpay" in u:
                m=re.search(r"USDKRW[^<]{0,200}?([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*원", html)
                if m: return to_f(m.group(1)), ("NAVER_PAY", u)
            else:
                soup=BeautifulSoup(html,"lxml")
                v=soup.select_one(".no_today .blind")
                if not v: v=soup.select_one("#content .no_today .blind")
                if v: return to_f(v.get_text()), ("NAVER_FIN", u)
        except Exception:
            continue
    return None, ("NONE","")

# ---------- GOLD(불확실), OIL(불확실) : 없으면 None ----------
def parse_gold_dubai_optional():
    # 네이버 마크업 변동 가능 → 값 없으면 None 유지
    try:
        html=get("https://stockpay.naver.com/market-index/USDKRW","naverpay_usdkrw_misc")
        g=re.search(r"국제\s*금[^0-9]{0,30}([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*달러", html)
        d=re.search(r"두바이유[^0-9]{0,30}([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*달러", html)
        return to_f(g.group(1)) if g else None, to_f(d.group(1)) if d else None
    except Exception:
        return None, None

def write_csv(rows, fname):
    p=os.path.join(OUTDIR,fname)
    with open(p,"w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()))
        w.writeheader(); [w.writerow(r) for r in rows]
    return p

def main():
    t=now_kst()
    ki=parse_korea_indices()
    usdkrw,(src_fx,src_url)=parse_usdkrw()
    gold,dubai=parse_gold_dubai_optional()

    row={
        "time_kst":t,
        "kospi":ki["kospi"]["index"],
        "kosdaq":ki["kosdaq"]["index"],
        "adv":ki["kosdaq"]["adv"],
        "dec":ki["kosdaq"]["dec"],
        "unch":ki["kosdaq"]["unch"],
        "usdkrw":usdkrw,
        "gold_usd":gold,
        "dubai_usd":dubai,
        "src_kospi":ki["kospi"]["source"],
        "src_kosdaq":ki["kosdaq"]["source"],
        "src_fx":src_fx
    }
    write_csv([row], f"krx_{dt.datetime.utcnow().date().strftime('%Y%m%d')}.csv")
    write_csv([row], "latest.csv")
    print("OK", row)

if __name__=="__main__":
    main()
