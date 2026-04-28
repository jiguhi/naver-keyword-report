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
from datetime import datetime, timedelta
from io import StringIO
import gspread
from google.oauth2.service_account import Credentials
import numpy as np
from gspread_formatting import CellFormat, format_cell_range, NumberFormat
from urllib.parse import urlsplit, parse_qsl
from client_config import Media_code_map








''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

def summarize_daily(df):
    if df.empty:
        return df

    sum_cols = ["Cost", "Impression", "Click", "Conversion count", "Sales by conversion"]

    df_daily = df.groupby("Date", as_index=False)[sum_cols].sum()

    df_daily["CPC"] = np.where(
        df_daily["Click"] > 0,
        df_daily["Cost"] / df_daily["Click"],
        0
    )

    df_daily["CTR (%)"] = np.where(
        df_daily["Impression"] > 0,
        df_daily["Click"] / df_daily["Impression"] * 100,
        0
    )

    df_daily["CVR"] = np.where(
        df_daily["Click"] > 0,
        df_daily["Conversion count"] / df_daily["Click"] * 100,
        0
    )

    df_daily["ROAS (%)"] = np.where(
        df_daily["Cost"] > 0,
        df_daily["Sales by conversion"] / df_daily["Cost"] * 100,
        0
    )

    df_daily = df_daily.replace([np.inf, -np.inf], 0).fillna(0)

    df_daily = df_daily[[
        "Date",
        "Cost",
        "Impression",
        "Click",
        "CPC",
        "CTR (%)",
        "Conversion count",
        "CVR",
        "Sales by conversion",
        "ROAS (%)"
    ]]

    return df_daily.round({
        "Cost": 0,
        "Impression": 0,
        "Click": 0,
        "CPC": 0,
        "CTR (%)": 2,
        "Conversion count": 0,
        "CVR": 2,
        "Sales by conversion": 0,
        "ROAS (%)": 2,
    })


# 네이버 광고 데이터 (전체캠페인 합 - 일별데이터)

def run_naver_report(report_date, api_key, secret_key, customer_id, campaign_type="전체"):
    API_KEY = api_key
    SECRET_KEY = secret_key
    CUSTOMER_ID = customer_id

    BASE_URL = "https://api.searchad.naver.com"

    class Signature:
        @staticmethod
        def generate(timestamp, method, uri, secret_key):
            message = f"{timestamp}.{method}.{uri}"
            hash_value = hmac.new(
                bytes(secret_key, "utf-8"),
                bytes(message, "utf-8"),
                hashlib.sha256
            )
            return base64.b64encode(hash_value.digest()).decode()

    def get_header(method, uri, api_key, secret_key, customer_id):
        timestamp = str(round(time.time() * 1000))
        signature = Signature.generate(timestamp, method, uri, secret_key)

        return {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Timestamp": timestamp,
            "X-API-KEY": api_key,
            "X-Customer": str(customer_id),
            "X-Signature": signature,
        }

    # -----------------------------
    # 1. 캠페인 마스터 조회
    # -----------------------------
    uri = "/ncc/campaigns"
    method = "GET"

    response = requests.get(
        BASE_URL + uri,
        headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID)
    )

    if response.status_code != 200:
        raise Exception(f"캠페인 조회 실패: {response.status_code} / {response.text}")

    campaigns = response.json()

    if isinstance(campaigns, list):
        df_cam = pd.DataFrame(campaigns)
    elif isinstance(campaigns, dict) and "data" in campaigns:
        df_cam = pd.DataFrame(campaigns["data"])
    else:
        raise Exception(f"예상치 못한 캠페인 응답 구조: {campaigns}")

    required_cols = ["nccCampaignId", "name", "campaignTp"]
    missing_cols = [col for col in required_cols if col not in df_cam.columns]

    if missing_cols:
        raise Exception(f"캠페인 응답에 필요한 컬럼 없음: {missing_cols}")

    df_cam = df_cam[["nccCampaignId", "name", "campaignTp"]].rename(columns={
        "nccCampaignId": "Campaign ID",
        "name": "Campaign Name",
        "campaignTp": "Campaign Type Raw"
    })

    CAMPAIGN_TYPE_MAP = {
        "WEB_SITE": "파워링크",
        "SHOPPING": "쇼핑검색",
        "BRAND_SEARCH": "브랜드검색",
        "PLACE": "플레이스",
        "SHOPPING_BRAND": "쇼핑브랜드",
    }

    df_cam["Campaign Type"] = (
        df_cam["Campaign Type Raw"]
        .map(CAMPAIGN_TYPE_MAP)
        .fillna(df_cam["Campaign Type Raw"])
    )

    # 캠페인 유형 필터
    if campaign_type != "전체":
        df_cam = df_cam[df_cam["Campaign Type"] == campaign_type].copy()

    if df_cam.empty:
        return pd.DataFrame(columns=[
            "Date",
            "Campaign Type",
            "Campaign Name",
            "Campaign ID",
            "Cost",
            "Impression",
            "Click",
            "CPC",
            "CTR (%)",
            "Conversion count",
            "Sales by conversion",
            "ROAS (%)",
            "CVR",
        ])

    # -----------------------------
    # 2. /stats로 캠페인별 성과 조회
    # -----------------------------
    uri = "/stats"
    method = "GET"

    stat_rows = []

    for _, row in df_cam.iterrows():
        campaign_id = row["Campaign ID"]

        params = {
            "id": campaign_id,
            "fields": json.dumps([
                "impCnt",
                "clkCnt",
                "salesAmt",
                "ctr",
                "cpc",
                "purchaseCcnt",
                "purchaseConvAmt"
            ]),
            "timeRange": json.dumps({
                "since": report_date,
                "until": report_date
            })
        }

        r = requests.get(
            BASE_URL + uri,
            params=params,
            headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID)
        )

        if r.status_code != 200:
            print(f"❌ stats 요청 실패: {campaign_id} / {r.status_code} / {r.text}")
            continue

        result_json = r.json()

        if isinstance(result_json, dict) and "data" in result_json:
            stat_data = result_json["data"]
        else:
            stat_data = result_json

        if isinstance(stat_data, list) and len(stat_data) > 0:
            stat = stat_data[0]
        elif isinstance(stat_data, dict):
            stat = stat_data
        else:
            stat = {}

        stat_rows.append({
            "Date": report_date,
            "Campaign Type": row["Campaign Type"],
            "Campaign Name": row["Campaign Name"],
            "Campaign ID": campaign_id,
            "Impression": stat.get("impCnt", 0),
            "Click": stat.get("clkCnt", 0),
            "Cost": stat.get("salesAmt", 0),
            "Conversion count": stat.get("purchaseCcnt", 0),
            "Sales by conversion": stat.get("purchaseConvAmt", 0),
        })

        time.sleep(0.1)

    df_combined = pd.DataFrame(stat_rows)

    if df_combined.empty:
        return pd.DataFrame(columns=[
            "Date",
            "Campaign Type",
            "Campaign Name",
            "Campaign ID",
            "Cost",
            "Impression",
            "Click",
            "CPC",
            "CTR (%)",
            "Conversion count",
            "Sales by conversion",
            "ROAS (%)",
            "CVR",
        ])

    # -----------------------------
    # 3. 숫자형 변환
    # -----------------------------
    number_cols = [
        "Impression",
        "Click",
        "Cost",
        "Conversion count",
        "Sales by conversion"
    ]

    for col in number_cols:
        df_combined[col] = pd.to_numeric(df_combined[col], errors="coerce").fillna(0)

    # -----------------------------
    # 4. KPI 계산
    # -----------------------------
    df_combined["CTR (%)"] = np.where(
        df_combined["Impression"] > 0,
        df_combined["Click"] / df_combined["Impression"] * 100,
        0
    )

    df_combined["CPC"] = np.where(
        df_combined["Click"] > 0,
        df_combined["Cost"] / df_combined["Click"],
        0
    )

    df_combined["ROAS (%)"] = np.where(
        df_combined["Cost"] > 0,
        df_combined["Sales by conversion"] / df_combined["Cost"] * 100,
        0
    )

    df_combined["CVR"] = np.where(
        df_combined["Click"] > 0,
        df_combined["Conversion count"] / df_combined["Click"] * 100,
        0
    )

    df_combined = df_combined.replace([np.inf, -np.inf], 0).fillna(0)

    df_combined = df_combined.round({
        "Cost": 0,
        "Impression": 0,
        "Click": 0,
        "CPC": 0,
        "CTR (%)": 2,
        "Conversion count": 0,
        "Sales by conversion": 0,
        "ROAS (%)": 2,
        "CVR": 2,
    })

    # -----------------------------
    # 5. 컬럼 순서 정리
    # -----------------------------
    df_combined = df_combined[[
        "Date",
        "Campaign Type",
        "Campaign Name",
        "Campaign ID",
        "Cost",
        "Impression",
        "Click",
        "CPC",
        "CTR (%)",
        "Conversion count",
        "Sales by conversion",
        "ROAS (%)",
        "CVR",
    ]]
# 브랜드검색 제외
    # df_combined = df_combined[df_combined["Campaign Type"] != "브랜드검색"].copy()
    result = summarize_daily(df_combined)
    # df_combined.to_excel('C:/Users/User/Desktop/code/df_combined1.xlsx', index=False, sheet_name="Ad Group Report")
    # result.to_excel('C:/Users/User/Desktop/code/result1.xlsx', index=False, sheet_name="Ad Group Report")
    return result




def run_brand_naver_report(report_date, api_key, secret_key, customer_id, brand_cost, campaign_type="전체" ):
    
    API_KEY = api_key
    SECRET_KEY = secret_key
    CUSTOMER_ID = customer_id

    BASE_URL = "https://api.searchad.naver.com"

    class Signature:
        @staticmethod
        def generate(timestamp, method, uri, secret_key):
            message = f"{timestamp}.{method}.{uri}"
            hash_value = hmac.new(
                bytes(secret_key, "utf-8"),
                bytes(message, "utf-8"),
                hashlib.sha256
            )
            return base64.b64encode(hash_value.digest()).decode()

    def get_header(method, uri, api_key, secret_key, customer_id):
        timestamp = str(round(time.time() * 1000))
        signature = Signature.generate(timestamp, method, uri, secret_key)

        return {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Timestamp": timestamp,
            "X-API-KEY": api_key,
            "X-Customer": str(customer_id),
            "X-Signature": signature,
        }

    # -----------------------------
    # 1. 캠페인 마스터 조회
    # -----------------------------
    uri = "/ncc/campaigns"
    method = "GET"

    response = requests.get(
        BASE_URL + uri,
        headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID)
    )

    if response.status_code != 200:
        raise Exception(f"캠페인 조회 실패: {response.status_code} / {response.text}")

    campaigns = response.json()

    if isinstance(campaigns, list):
        df_cam = pd.DataFrame(campaigns)
    elif isinstance(campaigns, dict) and "data" in campaigns:
        df_cam = pd.DataFrame(campaigns["data"])
    else:
        raise Exception(f"예상치 못한 캠페인 응답 구조: {campaigns}")

    required_cols = ["nccCampaignId", "name", "campaignTp"]
    missing_cols = [col for col in required_cols if col not in df_cam.columns]

    if missing_cols:
        raise Exception(f"캠페인 응답에 필요한 컬럼 없음: {missing_cols}")

    df_cam = df_cam[["nccCampaignId", "name", "campaignTp"]].rename(columns={
        "nccCampaignId": "Campaign ID",
        "name": "Campaign Name",
        "campaignTp": "Campaign Type Raw"
    })

    CAMPAIGN_TYPE_MAP = {
        "WEB_SITE": "파워링크",
        "SHOPPING": "쇼핑검색",
        "BRAND_SEARCH": "브랜드검색",
        "PLACE": "플레이스",
        "SHOPPING_BRAND": "쇼핑브랜드",
    }

    df_cam["Campaign Type"] = (
        df_cam["Campaign Type Raw"]
        .map(CAMPAIGN_TYPE_MAP)
        .fillna(df_cam["Campaign Type Raw"])
    )

    # 캠페인 유형 필터
    if campaign_type != "전체":
        df_cam = df_cam[df_cam["Campaign Type"] == campaign_type].copy()

    if df_cam.empty:
        return pd.DataFrame(columns=[
            "Date",
            "Campaign Type",
            "Campaign Name",
            "Campaign ID",
            "Cost",
            "Impression",
            "Click",
            "CPC",
            "CTR (%)",
            "Conversion count",
            "Sales by conversion",
            "ROAS (%)",
            "CVR",
        ])

    # -----------------------------
    # 2. /stats로 캠페인별 성과 조회
    # -----------------------------
    uri = "/stats"
    method = "GET"

    stat_rows = []

    for _, row in df_cam.iterrows():
        campaign_id = row["Campaign ID"]

        params = {
            "id": campaign_id,
            "fields": json.dumps([
                "impCnt",
                "clkCnt",
                "salesAmt",
                "ctr",
                "cpc",
                "purchaseCcnt",
                "purchaseConvAmt"
            ]),
            "timeRange": json.dumps({
                "since": report_date,
                "until": report_date
            })
        }

        r = requests.get(
            BASE_URL + uri,
            params=params,
            headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID)
        )

        if r.status_code != 200:
            print(f"❌ stats 요청 실패: {campaign_id} / {r.status_code} / {r.text}")
            continue

        result_json = r.json()

        if isinstance(result_json, dict) and "data" in result_json:
            stat_data = result_json["data"]
        else:
            stat_data = result_json

        if isinstance(stat_data, list) and len(stat_data) > 0:
            stat = stat_data[0]
        elif isinstance(stat_data, dict):
            stat = stat_data
        else:
            stat = {}

        stat_rows.append({
            "Date": report_date,
            "Campaign Type": row["Campaign Type"],
            "Campaign Name": row["Campaign Name"],
            "Campaign ID": campaign_id,
            "Impression": stat.get("impCnt", 0),
            "Click": stat.get("clkCnt", 0),
            "Cost": stat.get("salesAmt", 0),
            "Conversion count": stat.get("purchaseCcnt", 0),
            "Sales by conversion": stat.get("purchaseConvAmt", 0),
        })

        time.sleep(0.1)

    df_combined = pd.DataFrame(stat_rows)

    if df_combined.empty:
        return pd.DataFrame(columns=[
            "Date",
            "Campaign Type",
            "Campaign Name",
            "Campaign ID",
            "Cost",
            "Impression",
            "Click",
            "CPC",
            "CTR (%)",
            "Conversion count",
            "Sales by conversion",
            "ROAS (%)",
            "CVR",
        ])

    # -----------------------------
    # 3. 숫자형 변환
    # -----------------------------
    number_cols = [
        "Impression",
        "Click",
        "Cost",
        "Conversion count",
        "Sales by conversion"
    ]

    for col in number_cols:
        df_combined[col] = pd.to_numeric(df_combined[col], errors="coerce").fillna(0)

    # -----------------------------
    # 4. KPI 계산
    # -----------------------------
    df_combined["Cost"] = brand_cost

    df_combined["CTR (%)"] = np.where(
        df_combined["Impression"] > 0,
        df_combined["Click"] / df_combined["Impression"] * 100,
        0
    )

    df_combined["CPC"] = np.where(
        df_combined["Click"] > 0,
        df_combined["Cost"] / df_combined["Click"],
        0
    )

    df_combined["ROAS (%)"] = np.where(
        df_combined["Cost"] > 0,
        df_combined["Sales by conversion"] / df_combined["Cost"] * 100,
        0
    )

    df_combined["CVR"] = np.where(
        df_combined["Click"] > 0,
        df_combined["Conversion count"] / df_combined["Click"] * 100,
        0
    )

    df_combined = df_combined.replace([np.inf, -np.inf], 0).fillna(0)

    df_combined = df_combined.round({
        "Cost": 0,
        "Impression": 0,
        "Click": 0,
        "CPC": 0,
        "CTR (%)": 2,
        "Conversion count": 0,
        "Sales by conversion": 0,
        "ROAS (%)": 2,
        "CVR": 2,
    })

    # -----------------------------
    # 5. 컬럼 순서 정리
    # -----------------------------
    df_combined = df_combined[[
        "Date",
        "Campaign Type",
        "Campaign Name",
        "Campaign ID",
        "Cost",
        "Impression",
        "Click",
        "CPC",
        "CTR (%)",
        "Conversion count",
        "Sales by conversion",
        "ROAS (%)",
        "CVR",
    ]]
    df_brand = df_combined[df_combined["Campaign Type"] == "브랜드검색"].copy()
    df_brand["Cost"] = brand_cost
    # df_brand.to_excel('C:/Users/User/Desktop/code/df_brand.xlsx', index=False, sheet_name="Ad Group Report")

    result = summarize_daily(df_brand)

    # df_combined.to_excel('C:/Users/User/Desktop/code/df_combined.xlsx', index=False, sheet_name="Ad Group Report")

    # result.to_excel('C:/Users/User/Desktop/code/result.xlsx', index=False, sheet_name="Ad Group Report")
    return result


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

# 쇼핑검색 검색어 함수 
import requests
import hmac
import base64
import hashlib
import time
import json
import pandas as pd
import numpy as np


# -----------------------------
# 공통 인증 헤더
# -----------------------------
class Signature:
    @staticmethod
    def generate(timestamp, method, uri, secret_key):
        message = f"{timestamp}.{method}.{uri}"
        hash_value = hmac.new(
            bytes(secret_key, "utf-8"),
            bytes(message, "utf-8"),
            hashlib.sha256
        )
        return base64.b64encode(hash_value.digest()).decode()


def get_header(method, uri, api_key, secret_key, customer_id):
    timestamp = str(round(time.time() * 1000))
    signature = Signature.generate(timestamp, method, uri, secret_key)

    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": api_key,
        "X-Customer": str(customer_id),
        "X-Signature": signature,
    }


BASE_URL = "https://api.searchad.naver.com"


# -----------------------------
# 쇼핑검색 캠페인 ID 자동 조회
# -----------------------------
def get_shopping_campaign_ids(api_key, secret_key, customer_id):
    uri = "/ncc/campaigns"
    method = "GET"

    r = requests.get(
        BASE_URL + uri,
        headers=get_header(method, uri, api_key, secret_key, customer_id)
    )

    if r.status_code != 200:
        raise Exception(f"캠페인 조회 실패: {r.status_code} / {r.text}")

    campaigns = r.json()

    if isinstance(campaigns, list):
        df_cam = pd.DataFrame(campaigns)
    elif isinstance(campaigns, dict) and "data" in campaigns:
        df_cam = pd.DataFrame(campaigns["data"])
    else:
        raise Exception(f"예상치 못한 캠페인 응답 구조: {campaigns}")

    df_cam = df_cam[["nccCampaignId", "name", "campaignTp"]].rename(columns={
        "nccCampaignId": "Campaign ID",
        "name": "Campaign Name",
        "campaignTp": "Campaign Type Raw"
    })

    # 쇼핑검색 캠페인만 추출
    df_shopping = df_cam[df_cam["Campaign Type Raw"] == "SHOPPING"].copy()

    return df_shopping[["Campaign ID", "Campaign Name"]]


# -----------------------------
# 쇼핑검색 검색어 조회
# -----------------------------
def download_stat_report(report_date, report_type, api_key, secret_key, customer_id):
    BASE_URL = "https://api.searchad.naver.com"

    uri = "/stat-reports"
    method = "POST"

    params = {
        "reportTp": report_type,
        "statDt": report_date
    }

    r = requests.post(
        BASE_URL + uri,
        json=params,
        headers=get_header(method, uri, api_key, secret_key, customer_id)
    )
    r.raise_for_status()

    time.sleep(10)

    uri = "/stat-reports"
    method = "GET"

    r = requests.get(
        BASE_URL + uri,
        headers=get_header(method, uri, api_key, secret_key, customer_id)
    )
    r.raise_for_status()

    report_list = pd.DataFrame(r.json())

    report_list = report_list[
        (report_list["reportTp"] == report_type) &
        (report_list["status"] == "BUILT")
    ].copy()

    report_list = report_list.sort_values("regTm", ascending=False).reset_index(drop=True)

    if report_list.empty:
        raise ValueError(f"{report_type} 리포트 다운로드 URL을 찾지 못했습니다.")

    url = report_list["downloadUrl"].iloc[0]

    qs = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
    token = qs.get("authtoken")
    file_version = qs.get("fileVersion") or qs.get("fileversion") or "v2"

    uri = "/report-download"
    method = "GET"

    r = requests.get(
        BASE_URL + uri,
        params={
            "authtoken": token,
            "fileVersion": file_version
        },
        headers=get_header(method, uri, api_key, secret_key, customer_id)
    )
    r.raise_for_status()

    data = StringIO(r.text)

    df_raw = pd.read_csv(data, sep="\t", header=None)

    print(f"[{report_type}] 컬럼 개수:", len(df_raw.columns))
    print(df_raw.head())
    print("첫 행:", df_raw.iloc[0].to_list())

    df_raw.columns = [f"col_{i}" for i in range(len(df_raw.columns))]

    return df_raw

def get_shopping_keyword_report(report_date, api_key, secret_key, customer_id):
    # 1. 쇼핑검색 검색어 성과 리포트
    df_perf = download_stat_report(
        report_date=report_date,
        report_type="SHOPPINGKEYWORD_DETAIL",
        api_key=api_key,
        secret_key=secret_key,
        customer_id=customer_id
    )

    df_perf.columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID",
        "Search keyword", "AD ID", "Business Channel ID",
        "Hours", "Region code", "Media code", "PC Mobile Type",
        "Impression", "Click", "Cost", "Sum of AD rank", "View count"
    ]

    # 2. 쇼핑검색 검색어 전환 리포트
    df_conv = download_stat_report(
        report_date=report_date,
        report_type="SHOPPINGKEYWORD_CONVERSION_DETAIL",
        api_key=api_key,
        secret_key=secret_key,
        customer_id=customer_id
    )

    df_conv.columns = [
        "Date", "CUSTOMER ID", "Campaign ID", "AD Group ID",
        "Search keyword", "AD ID", "Business Channel ID",
        "Hours", "Region code", "Media code", "PC Mobile Type",
        "Conversion Method", "Conversion Type",
        "Conversion count", "Sales by conversion"
    ]

    # 3. 캠페인명 가져오기
    BASE_URL = "https://api.searchad.naver.com"

    uri = "/ncc/campaigns"
    r = requests.get(
        BASE_URL + uri,
        headers=get_header("GET", uri, api_key, secret_key, customer_id)
    )
    r.raise_for_status()

    df_campaign = pd.DataFrame(r.json())[["nccCampaignId", "name"]].rename(columns={
        "nccCampaignId": "Campaign ID",
        "name": "Campaign name"
    })

    # 4. 광고그룹명 가져오기
    uri = "/ncc/adgroups"
    r = requests.get(
        BASE_URL + uri,
        headers=get_header("GET", uri, api_key, secret_key, customer_id)
    )
    r.raise_for_status()

    df_adgroup = pd.DataFrame(r.json())[["nccAdgroupId", "name"]].rename(columns={
        "nccAdgroupId": "AD Group ID",
        "name": "Ad group name"
    })

    # 5. 숫자형 변환
    for col in ["Impression", "Click", "Cost", "Sum of AD rank"]:
        df_perf[col] = pd.to_numeric(df_perf[col], errors="coerce").fillna(0)

    for col in ["Conversion count", "Sales by conversion"]:
        df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").fillna(0)

    # 6. 평균 노출 순위 계산
    df_perf["평균 노출 순위"] = np.where(
        df_perf["Impression"] > 0,
        df_perf["Sum of AD rank"] / df_perf["Impression"],
        0
    )

    # 7. 성과 데이터 집계
    perf_keys = [
        "Date", "Campaign ID", "AD Group ID",
        "Search keyword", "AD ID", "Business Channel ID",
        "PC Mobile Type", "Media code"
    ]

    df_perf_g = (
        df_perf.groupby(perf_keys, as_index=False)
        .agg({
            "Impression": "sum",
            "Click": "sum",
            "Cost": "sum",
            "Sum of AD rank": "sum"
        })
    )

    df_perf_g["평균 노출 순위"] = np.where(
        df_perf_g["Impression"] > 0,
        df_perf_g["Sum of AD rank"] / df_perf_g["Impression"],
        0
    )

    # 8. 전환 데이터 집계
    conv_keys = perf_keys + ["Conversion Type"]

    df_conv = df_conv[df_conv["Conversion Type"] == "purchase"]

    df_conv_g = (
        df_conv.groupby(conv_keys, as_index=False)
        .agg({
            "Conversion count": "sum",
            "Sales by conversion": "sum"
        })
    )
    # 9. 성과 + 전환 병합
    df_final = pd.merge(
        df_perf_g,
        df_conv_g,
        on=perf_keys,
        how="left"
    )

    df_final["Conversion Type"] = df_final["Conversion Type"].fillna("전환없음")
    df_final["Conversion count"] = df_final["Conversion count"].fillna(0)
    df_final["Sales by conversion"] = df_final["Sales by conversion"].fillna(0)

    # 10. 캠페인명 / 광고그룹명 병합
    df_final = df_final.merge(df_campaign, on="Campaign ID", how="left")
    df_final = df_final.merge(df_adgroup, on="AD Group ID", how="left")
    df_final["Media code"] = pd.to_numeric(df_final["Media code"], errors="coerce")
    df_final["매체"] = df_final["Media code"].map(Media_code_map)

    # 11. 최종 컬럼 정리
    df_final = df_final[[
        "Date",
        "매체",
        "Campaign name",
        "Ad group name",
        "AD ID",
        "Impression",
        "Click",
        "Cost",
        "평균 노출 순위",
        "Search keyword",
        # "Conversion Type",
        "Conversion count",
        "Sales by conversion"
    ]]

    df_final = df_final.round({
        "Cost": 0,
        "평균 노출 순위": 1,
        "Conversion count": 0,
        "Sales by conversion": 0
    })
    # df_final.to_excel('C:/Users/User/Desktop/code/df_final.xlsx', index=False, sheet_name="Ad Group Report")

    return df_final

def get_powerlink_keyword_report(report_date, api_key, secret_key, customer_id):

    BASE_URL = "https://api.searchad.naver.com"

    # 1. 파워링크 검색어 리포트
    df_exp = download_stat_report(
        report_date=report_date,
        report_type="EXPKEYWORD",
        api_key=api_key,
        secret_key=secret_key,
        customer_id=customer_id
    )

    df_exp.columns = [
        "Date",
        "CUSTOMER ID",
        "Campaign ID",
        "AD Group ID",
        "Search keyword",
        "Media code",
        "PC Mobile Type",
        "Search Keyword Type",
        "Impression",
        "Click",
        "Cost",
        "View count"
    ]

    for col in ["Impression", "Click", "Cost"]:
        df_exp[col] = pd.to_numeric(df_exp[col], errors="coerce").fillna(0)

    # 2. 캠페인명
    uri = "/ncc/campaigns"
    r = requests.get(
        BASE_URL + uri,
        headers=get_header("GET", uri, api_key, secret_key, customer_id)
    )
    r.raise_for_status()

    df_campaign = pd.DataFrame(r.json())[["nccCampaignId", "name"]].rename(columns={
        "nccCampaignId": "Campaign ID",
        "name": "Campaign name"
    })

    # 3. 광고그룹명
    uri = "/ncc/adgroups"
    r = requests.get(
        BASE_URL + uri,
        headers=get_header("GET", uri, api_key, secret_key, customer_id)
    )
    r.raise_for_status()

    df_adgroup = pd.DataFrame(r.json())[["nccAdgroupId", "name"]].rename(columns={
        "nccAdgroupId": "AD Group ID",
        "name": "Ad group name"
    })

    # 4. 등록 키워드 목록 가져오기
    uri = "/ncc/keywords"
    keyword_rows = []

    for adgroup_id in df_adgroup["AD Group ID"].dropna().unique():
        r = requests.get(
            BASE_URL + uri,
            params={"nccAdgroupId": adgroup_id},
            headers=get_header("GET", uri, api_key, secret_key, customer_id)
        )

        if r.status_code == 200:
            keyword_rows.extend(r.json())

    if keyword_rows:
        df_keyword = pd.DataFrame(keyword_rows)[[
            "nccKeywordId",
            "keyword",
            "nccAdgroupId"
        ]].rename(columns={
            "nccKeywordId": "AD keyword ID",
            "keyword": "Registered keyword",
            "nccAdgroupId": "AD Group ID"
        })
    else:
        df_keyword = pd.DataFrame(columns=[
            "AD keyword ID",
            "Registered keyword",
            "AD Group ID"
        ])

    # 5. 캠페인/광고그룹명 merge
    df_exp = df_exp.merge(df_campaign, on="Campaign ID", how="left")
    df_exp = df_exp.merge(df_adgroup, on="AD Group ID", how="left")

    # 6. 검색어가 등록 키워드인지 매칭
    df_exp["Search keyword clean"] = df_exp["Search keyword"].astype(str).str.strip()
    df_keyword["Registered keyword clean"] = df_keyword["Registered keyword"].astype(str).str.strip()

    df_exp = df_exp.merge(
        df_keyword[[
            "AD Group ID",
            "AD keyword ID",
            "Registered keyword clean"
        ]],
        left_on=["AD Group ID", "Search keyword clean"],
        right_on=["AD Group ID", "Registered keyword clean"],
        how="left"
    )

    df_exp["AD keyword ID"] = df_exp["AD keyword ID"].fillna("")

    df_exp["Keyword Type"] = np.where(
        df_exp["AD keyword ID"].astype(str).str.strip() != "",
        "등록키워드",
        "확장키워드"
    )

    # 7. 매체명
    df_exp["Media code"] = pd.to_numeric(df_exp["Media code"], errors="coerce")
    df_exp["매체"] = df_exp["Media code"].map(Media_code_map).fillna("기타")

    # 8. 검색어 성과 먼저 집계
    perf_keys = [
        "Date",
        "매체",
        "Campaign ID",
        "Campaign name",
        "AD Group ID",
        "Ad group name",
        "AD keyword ID",
        "Keyword Type",
        "Search keyword",
        "Media code",
        "PC Mobile Type"
    ]

    df_perf_g = (
        df_exp.groupby(perf_keys, as_index=False)
        .agg({
            "Impression": "sum",
            "Click": "sum",
            "Cost": "sum"
        })
    )

    # 9. 전환 리포트 가져오기
    df_conv = download_stat_report(
        report_date=report_date,
        report_type="AD_CONVERSION_DETAIL",
        api_key=api_key,
        secret_key=secret_key,
        customer_id=customer_id
    )

    df_conv.columns = [
        "Date",
        "CUSTOMER ID",
        "Campaign ID",
        "AD Group ID",
        "AD keyword ID",
        "AD ID",
        "Business Channel ID",
        "Hours",
        "Region code",
        "Media code",
        "PC Mobile Type",
        "Conversion Method",
        "Conversion Type",
        "Conversion count",
        "Sales by conversion"
    ]

    # 구매완료 전환만 사용
    df_conv = df_conv[df_conv["Conversion Type"] == "purchase"].copy()

    for col in ["Conversion count", "Sales by conversion"]:
        df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").fillna(0)

    df_conv["Media code"] = pd.to_numeric(df_conv["Media code"], errors="coerce")
    df_conv["AD keyword ID"] = df_conv["AD keyword ID"].astype(str).str.strip()

    conv_keys = [
        "Date",
        "Campaign ID",
        "AD Group ID",
        "AD keyword ID",
        "Media code",
        "PC Mobile Type",
        "Conversion Type"
    ]

    df_conv_g = (
        df_conv.groupby(conv_keys, as_index=False)
        .agg({
            "Conversion count": "sum",
            "Sales by conversion": "sum"
        })
    )

    # 10. 등록키워드만 전환 merge 가능
    merge_keys = [
        "Date",
        "Campaign ID",
        "AD Group ID",
        "AD keyword ID",
        "Media code",
        "PC Mobile Type"
    ]

    df_final = pd.merge(
        df_perf_g,
        df_conv_g,
        on=merge_keys,
        how="left"
    )

    # 확장키워드는 AD keyword ID가 없으므로 전환 0
    df_final["Conversion Type"] = df_final["Conversion Type"].fillna("")
    df_final["Conversion count"] = df_final["Conversion count"].fillna(0)
    df_final["Sales by conversion"] = df_final["Sales by conversion"].fillna(0)

    # 11. 최종 컬럼
    df_final = df_final[[
        "Date",
        "매체",
        "Campaign name",
        "Ad group name",
        "AD keyword ID",
        "Keyword Type",
        "Impression",
        "Click",
        "Cost",
        "Search keyword",
        "Conversion Type",
        "Conversion count",
        "Sales by conversion"
    ]]

    df_final = df_final.round({
        "Cost": 0,
        "Conversion count": 0,
        "Sales by conversion": 0
    })

   
    return df_final
    

# get_shopping_keyword_report(report_date = "20260427", api_key ='01000000000d4de559bd8515cd989bd4ae76535359afb03c701256acf023d9ae1619091fc9',
# secret_key='AQAAAAANTeVZvYUVzZib1K52U1NZmqVA5PBGsAxIMJMEwM3yMQ==', customer_id=	'1394697')

