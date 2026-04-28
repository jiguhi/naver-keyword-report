import streamlit as st
import pandas as pd
from datetime import date
import os
import requests
import hmac
import base64
import hashlib
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
from gspread_formatting import CellFormat, format_cell_range, NumberFormat
from urllib.parse import urlsplit, parse_qsl

from client_config import CLIENT_CONFIG
from naver_api import run_naver_report

from datetime import timedelta
import pandas as pd

def date_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)
st.set_page_config(page_title="광고 리포트 자동화", layout="wide")

# -----------------------------
# 샘플 데이터 생성 함수
# 나중에 여기에 네이버 API 코드 붙이면 됨
# -----------------------------
# API 설정

def fetch_report_data(client, channel, start_date, end_date):
    config = CLIENT_CONFIG[client]

    result_list = []

    for target_date in date_range(start_date, end_date):
        df_day = run_naver_report(
            report_date=target_date.strftime("%Y%m%d"),
            api_key=config["API_KEY"],
            secret_key=config["SECRET_KEY"],
            customer_id=config["CUSTOMER_ID"],
            campaign_ids=config["CAMPAIGN_IDS"]
        )

        result_list.append(df_day)

    if not result_list:
        return pd.DataFrame()

    df = pd.concat(result_list, ignore_index=True)
    return df

# -----------------------------
# 사이드바
# -----------------------------
st.sidebar.title("광고 리포트 툴")

client = st.sidebar.selectbox(
    "광고주 선택",
    list(CLIENT_CONFIG.keys())
)
channel = st.sidebar.selectbox(
    "매체 선택",
    ["네이버 SA", "네이버 GFA", "Meta", "Google Ads", "쿠팡", "11번가", "G마켓"]
)

report_type = st.sidebar.selectbox(
    "리포트 유형",
    ["일간", "주간", "월간", "기간별"]
)

# -----------------------------
# 메인 화면
# -----------------------------
st.title("광고 리포트 자동화")
st.caption(f"광고주: {client} | 매체: {channel} | 유형: {report_type}")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("시작일", value=date.today())
with col2:
    end_date = st.date_input("종료일", value=date.today())

run_button = st.button("리포트 실행", type="primary")

if run_button:
    try:
        with st.spinner("데이터 조회 중입니다..."):
            df = fetch_report_data(client, channel, start_date, end_date)

        st.success("리포트 실행 완료")

        # 요약 지표
        c1, c2, c3, c4 = st.columns(4)
        total_cost = int(df["Cost"].sum())
        total_sales = int(df["Sales by conversion"].sum())
        total_conv = int(df["Conversion count"].sum())

        c1.metric("총 광고비", f"{total_cost:,}원")
        c2.metric("총 매출", f"{total_sales:,}원")
        c3.metric("총 전환수", f"{total_conv:,}")
        st.subheader("리포트 결과")
        st.dataframe(df, width="stretch", hide_index=True)

        csv_data = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="CSV 다운로드",
            data=csv_data,
            file_name=f"{client}_{channel}_report.csv",
            mime="text/csv"
        )

    except Exception as e:
        st.error(f"오류 발생: {e}")





