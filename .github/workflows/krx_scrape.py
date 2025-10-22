# -*- coding: utf-8 -*-
# KRX 메인(MDC) 페이지를 헤드리스로 열어 장중 값(지수/브레드스)을 추출.
# 1차-잠정: KRX DOM → 실패 시 보조(Investing)로 대체. 산출물: out/latest.csv

from playwright.sync_api import sync_playwright
import pandas as pd, os, time
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))

def now_kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

os.makedirs("out", exist_ok=True)

def try_int(x):
    try:
        return int(str(x).replace(",","").strip())
    except:
        return None

def try_float(x):
    try:
        return float(str(x).replace(",","").strip())
    except:
        return None

def scrape_krx_dom(page):
    # KRX 메인(ko): 지수 타일 + 시장 폭(상승/하락/보합) 위젯에서 값 추출 시도
    page.goto("https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd?locale=ko", timeout=120_000, wait_until="networkidle")
    page.wait_for_timeout(2000)

    # 지수: 화면 내 텍스트 앵커 기반(레이아웃 변경 대비 여유)
    def grab_number_by_label(label):
        # label 텍스트 근처의 숫자(span, strong 등) 추적
        el = page.locator(f"text={label}").first
        if not el or el.count()==0:
            return None
        box = el.locator("xpath=..").first
        # 같은 행 혹은 인접 노드의 숫자 스팬 탐색
        cand = box.locator("xpath=.//following::span[contains(@class,'num') or contains(@class,'point')][1]")
        if cand.count()==0:
            cand = box.locator("xpath=../following::span[contains(@class,'num') or contains(@class,'point')][1]")
        if cand.count()==0:
            return None
        txt = cand.first.inner_text().strip()
        return try_float(txt.replace("p","").replace("P",""))

    kospi = grab_number_by_label("KOSPI") or grab_number_by_label("코스피")
    kosdaq = grab_number_by_label("KOSDAQ") or grab_number_by_label("코스닥")

    # 시장폭: "상승/하락/보합" 텍스트 주변 숫자 합산
    def grab_breadth():
        up = page.locator("text=상승").first
        down = page.locator("text=하락").first
        unch = page.locator("text=보합").first
        def sibling_num(node):
            if not node or node.count()==0: return None
            cand = node.locator("xpath=.//following::span[contains(@class,'num')][1]")
            if cand.count()==0:
                return None
            return try_int(cand.first.inner_text())
        return sibling_num(up), sibling_num(down), sibling_num(unch)

    adv, dec, unch = grab_breadth()

    return {
        "time_kst": now_kst(),
        "source": "KRX_DOM",
        "kospi": kospi, "kosdaq": kosdaq,
        "adv": adv, "dec": dec, "unch": unch
    }

def scrape_secondary():
    # 보조(잠정): Investing에서 KOSPI/KOSDAQ 인덱스만 확보
    import pandas as pd
    # 실제 실시간 파싱 대신 자리표시(크롤링 정책 고려). 값 None이면 KRX_DOM만 사용.
    return {
        "time_kst": now_kst(),
        "source": "SECONDARY",
        "kospi": None, "kosdaq": None,
        "adv": None, "dec": None, "unch": None
    }

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context()
    page = ctx.new_page()
    data = scrape_krx_dom(page)
    # 실패 보정
    if not any([data.get("kospi"), data.get("kosdaq"), data.get("adv"), data.get("dec")]):
        data = scrape_secondary()

    df = pd.DataFrame([data])
    df.to_csv("out/latest.csv", index=False, encoding="utf-8-sig")
    df.to_csv(f"out/krx_{datetime.now(KST).strftime('%Y%m%d')}.csv", index=False, encoding="utf-8-sig")
    print(df)
