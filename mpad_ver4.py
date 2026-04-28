import streamlit as st
import pandas as pd
from datetime import date, timedelta

from client_config import CLIENT_CONFIG
from naver_api_ver2 import run_naver_report, run_brand_naver_report, get_shopping_keyword_report,get_powerlink_keyword_report
from io import BytesIO

def date_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

# 엑셀 다운 함수
def to_excel_bytes(dfs: dict):
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in dfs.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name[:31])

    output.seek(0)
    return output



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

# -----------------------------
# 세션 상태 초기화
# -----------------------------
if "df" not in st.session_state:
    st.session_state.df = None

if "df_keyword" not in st.session_state:
    st.session_state.df_keyword = None


run_button = st.button("리포트 실행", type="primary")

if run_button:
    try:
        with st.spinner("데이터 조회 중입니다..."):

            config = CLIENT_CONFIG[client]

            # -----------------------
            # 1. 일별 리포트
            # -----------------------
            df = fetch_report_data(
                client,
                channel,
                start_date,
                end_date,
                campaign_type,
                report_target
            )

            # -----------------------
            # 2. 키워드 리포트
            # -----------------------
            df_keyword = None

            if campaign_type in ["쇼핑검색", "파워링크"]:
                keyword_list = []

                for target_date in date_range(start_date, end_date):
                    if campaign_type == "쇼핑검색":
                        df_k = get_shopping_keyword_report(
                            report_date=target_date.strftime("%Y%m%d"),
                            api_key=config["API_KEY"],
                            secret_key=config["SECRET_KEY"],
                            customer_id=config["CUSTOMER_ID"]
                        )

                    elif campaign_type == "파워링크":
                        df_k = get_powerlink_keyword_report(
                            report_date=target_date.strftime("%Y%m%d"),
                            api_key=config["API_KEY"],
                            secret_key=config["SECRET_KEY"],
                            customer_id=config["CUSTOMER_ID"]
                        )

                    if df_k is not None and not df_k.empty:
                        keyword_list.append(df_k)

                if keyword_list:
                    # 🔥 raw 데이터 그대로 누적
                    df_keyword = pd.concat(keyword_list, ignore_index=True)
                else:
                    df_keyword = pd.DataFrame()

            # -----------------------
            # 3. 세션에 저장
            # -----------------------
            st.session_state.df = df
            st.session_state.df_keyword = df_keyword

            st.success("리포트 실행 완료")

    except Exception as e:
        st.error(f"오류 발생: {e}")


# -----------------------------
# 세션에서 데이터 불러오기
# -----------------------------
df = st.session_state.df
df_keyword = st.session_state.df_keyword


# -----------------------------
# 결과 출력
# -----------------------------
if df is not None:

    if df.empty:
        st.warning("일별 리포트 데이터가 없습니다.")

    else:
        # KPI
        c1, c2, c3, c4 = st.columns(4)

        total_cost = int(df["Cost"].sum())
        total_sales = int(df["Sales by conversion"].sum())
        total_conv = int(df["Conversion count"].sum())
        total_roas = round((total_sales / total_cost) * 100, 0) if total_cost > 0 else 0

        c1.metric("총 광고비", f"{total_cost:,}원")
        c2.metric("총 매출", f"{total_sales:,}원")
        c3.metric("총 전환수", f"{total_conv:,}")
        c4.metric("ROAS", f"{int(total_roas)}%")

        # 탭 UI
        if (
            campaign_type in ["쇼핑검색", "파워링크"]
            and df_keyword is not None
            and not df_keyword.empty
        ):
            tab1, tab2 = st.tabs(["📊 일별 리포트", "🔍 키워드 리포트"])

            with tab1:
                st.subheader("일별 리포트")
                st.dataframe(df, width="stretch", hide_index=True)

            with tab2:
                st.subheader("키워드 리포트")
                st.dataframe(df_keyword, width="stretch", hide_index=True)

        else:
            st.subheader("일별 리포트")
            st.dataframe(df, width="stretch", hide_index=True)


# -----------------------------
# 엑셀 다운로드
# -----------------------------
download_sheets = {}

if df is not None and not df.empty:
    download_sheets["일별"] = df

if df_keyword is not None and not df_keyword.empty:
    download_sheets["키워드"] = df_keyword

if download_sheets:
    excel_data = to_excel_bytes(download_sheets)

    st.download_button(
        label="엑셀 다운로드",
        data=excel_data,
        file_name=f"{client}_{campaign_type}_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        on_click="ignore"
    )
    