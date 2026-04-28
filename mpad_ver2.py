import streamlit as st
import pandas as pd
from datetime import date, timedelta

from client_config import CLIENT_CONFIG
from naver_api_ver2 import run_naver_report, run_brand_naver_report


def date_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def fetch_report_data(client, channel, start_date, end_date, campaign_type, report_target):
    config = CLIENT_CONFIG[client]
    result_list = []

    for target_date in date_range(start_date, end_date):
        if report_target == "브랜드검색광고":
            df_day = run_brand_naver_report(
                report_date=target_date.strftime("%Y%m%d"),
                api_key=config["API_KEY"],
                secret_key=config["SECRET_KEY"],
                customer_id=config["CUSTOMER_ID"],
                brand_cost= config['BRAND_COST'],
            )
        else:
            df_day = run_naver_report(
                report_date=target_date.strftime("%Y%m%d"),
                api_key=config["API_KEY"],
                secret_key=config["SECRET_KEY"],
                customer_id=config["CUSTOMER_ID"],
                campaign_type=campaign_type
            )

        result_list.append(df_day)

    if not result_list:
        return pd.DataFrame()

    return pd.concat(result_list, ignore_index=True)

st.set_page_config(page_title="광고 리포트 자동화", layout="wide")

st.sidebar.title("광고 리포트 툴")

client = st.sidebar.selectbox(
    "광고주 선택",
    list(CLIENT_CONFIG.keys())
)

channel = st.sidebar.selectbox(
    "매체 선택",
    ["네이버 SA", "네이버 GFA", "Meta", "Google Ads", "쿠팡", "11번가", "G마켓"]
)

report_target = st.sidebar.selectbox(
    "조회 대상",
    ["일반 SA", "브랜드검색광고"]
)

report_type = st.sidebar.selectbox(
    "리포트 유형",
    ["일간", "주간", "월간", "기간별"]
)

campaign_type = st.sidebar.selectbox(
    "캠페인 유형",
    ["전체", "쇼핑검색", "파워링크", "브랜드검색", "플레이스", "쇼핑브랜드"]
)

st.title("광고 리포트 자동화")
st.caption(
    f"광고주: {client} | 매체: {channel} | 유형: {report_type} | 조회 대상: {report_target} | 캠페인: {campaign_type}"
)

col1, col2 = st.columns(2)

with col1:
    start_date = st.date_input("시작일", value=date.today() - timedelta(days=1))

with col2:
    end_date = st.date_input("종료일", value=date.today() - timedelta(days=1))

run_button = st.button("리포트 실행", type="primary")

if run_button:
    try:
        with st.spinner("데이터 조회 중입니다..."):
            df = fetch_report_data(
                client,
                channel,
                start_date,
                end_date,
                campaign_type,
                report_target
)

        if df.empty:
            st.warning("조회된 데이터가 없습니다.")
        else:
            st.success("리포트 실행 완료")

            c1, c2, c3, c4 = st.columns(4)

            total_cost = int(df["Cost"].sum())
            total_sales = int(df["Sales by conversion"].sum())
            total_conv = int(df["Conversion count"].sum())
            total_roas = round((total_sales / total_cost) * 100, 0) if total_cost > 0 else 0

            c1.metric("총 광고비", f"{total_cost:,}원")
            c2.metric("총 매출", f"{total_sales:,}원")
            c3.metric("총 전환수", f"{total_conv:,}")
            c4.metric("ROAS", f"{int(total_roas)}%")

            st.subheader("리포트 결과")
            st.dataframe(df, width="stretch", hide_index=True)

            csv_data = df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                label="CSV 다운로드",
                data=csv_data,
                file_name=f"{client}_{channel}_{campaign_type}_report.csv",
                mime="text/csv"
            )

    except Exception as e:
        st.error(f"오류 발생: {e}")