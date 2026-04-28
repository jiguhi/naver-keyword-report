import streamlit as st
import pandas as pd
from datetime import timedelta
from io import BytesIO

from naver_api_ver2 import get_shopping_keyword_report,get_powerlink_keyword_report





# -----------------------------
# 날짜 범위 생성
# -----------------------------
def make_date_list(start_date, end_date):
    dates = []
    cur = start_date
    while cur <= end_date:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return dates


# -----------------------------
# 엑셀 변환
# -----------------------------
def to_excel_bytes(sheets: dict):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


# -----------------------------
# UI
# -----------------------------
st.title("네이버 키워드 데이터 누적 다운로드")

st.subheader("조회 조건")

col1, col2 = st.columns(2)

with col1:
    start_date = st.date_input("시작일")

with col2:
    end_date = st.date_input("종료일")

report_types = st.multiselect(
    "리포트 선택",
    ["파워링크 키워드", "쇼핑검색 키워드"],
    default=["파워링크 키워드", "쇼핑검색 키워드"]
)

st.subheader("API 정보")

api_key = st.text_input("API KEY", type="password")
secret_key = st.text_input("SECRET KEY", type="password")
customer_id = st.text_input("CUSTOMER ID")

run_btn = st.button("데이터 조회")


# -----------------------------
# 실행
# -----------------------------
if run_btn:
    if start_date > end_date:
        st.error("시작일은 종료일보다 늦을 수 없습니다.")

    elif not api_key or not secret_key or not customer_id:
        st.error("API KEY, SECRET KEY, CUSTOMER ID를 모두 입력해주세요.")

    else:
        date_list = make_date_list(start_date, end_date)

        powerlink_list = []
        shopping_list = []

        progress = st.progress(0)
        status = st.empty()

        for idx, report_date in enumerate(date_list):
            status.write(f"수집 중: {report_date}")

            try:
                if "파워링크 키워드" in report_types:
                    df_powerlink = get_powerlink_keyword_report(
                        report_date=report_date,
                        api_key=api_key,
                        secret_key=secret_key,
                        customer_id=customer_id
                    )
                    powerlink_list.append(df_powerlink)

                if "쇼핑검색 키워드" in report_types:
                    df_shopping = get_shopping_keyword_report(
                        report_date=report_date,
                        api_key=api_key,
                        secret_key=secret_key,
                        customer_id=customer_id
                    )
                    shopping_list.append(df_shopping)

            except Exception as e:
                st.warning(f"{report_date} 수집 실패: {e}")

            progress.progress((idx + 1) / len(date_list))

        download_sheets = {}

        if powerlink_list:
            df_powerlink_all = pd.concat(powerlink_list, ignore_index=True)
            download_sheets["파워링크_키워드"] = df_powerlink_all

            st.subheader("파워링크 키워드 미리보기")
            st.dataframe(df_powerlink_all)

        if shopping_list:
            df_shopping_all = pd.concat(shopping_list, ignore_index=True)
            download_sheets["쇼핑검색_키워드"] = df_shopping_all

            st.subheader("쇼핑검색 키워드 미리보기")
            st.dataframe(df_shopping_all)

        if download_sheets:
            excel_data = to_excel_bytes(download_sheets)

            st.download_button(
                label="엑셀 다운로드",
                data=excel_data,
                file_name=f"naver_keyword_report_{start_date}_{end_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            st.success("데이터 수집 완료")
        else:
            st.info("수집된 데이터가 없습니다.")