import base64
import hashlib
import hmac
import json
import time
from datetime import date, datetime
from io import StringIO
from urllib.parse import parse_qsl, urlsplit

import numpy as np
import pandas as pd
import requests

from client_config import Media_code_map


BASE_URL = "https://api.searchad.naver.com"
CAMPAIGN_TYPE_LABELS = {
    "SHOPPING": "쇼핑검색",
    "WEB_SITE": "파워링크",
    "PLACE": "플레이스",
    "POWER_CONTENTS": "파워컨텐츠",
    "SHOPPING_BRAND": "쇼핑브랜드",
    "BRAND_SEARCH": "브랜드검색",
}
SUMMARY_COLUMNS = [
    "Date",
    "Cost",
    "Impression",
    "Click",
    "CPC",
    "CTR (%)",
    "Conversion count",
    "CVR",
    "Sales by conversion",
    "ROAS (%)",
]
DETAIL_COLUMNS = [
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
]
CAMPAIGN_REPORT_COLUMNS = [
    "Date",
    "Campaign Type",
    "Campaign name",
    "Campaign ID",
    "Impression",
    "Click",
    "Cost",
    "CPC",
    "CTR (%)",
    "Conversion count",
    "CVR",
    "Sales by conversion",
    "ROAS (%)",
]


class Signature:
    @staticmethod
    def generate(timestamp, method, uri, secret_key):
        message = f"{timestamp}.{method}.{uri}"
        digest = hmac.new(
            secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode()


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


def _format_naver_date(value):
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y%m%d")

    value = str(value).strip()
    if not value:
        return None
    if len(value) == 8 and value.isdigit():
        return value

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y%m%d")
        except ValueError:
            pass

    return value.replace("-", "").replace("/", "").replace(".", "")


def _resolve_report_range(report_date=None, start_date=None, end_date=None):
    if report_date is not None:
        since = until = _format_naver_date(report_date)
    else:
        since = _format_naver_date(start_date)
        until = _format_naver_date(end_date)

    if not since or not until:
        raise ValueError("report_date 또는 start_date/end_date를 입력해야 합니다.")
    if since > until:
        raise ValueError("start_date는 end_date보다 늦을 수 없습니다.")

    return since, until


def _empty_dataframe(columns):
    return pd.DataFrame(columns=columns)


def _ensure_columns(df, columns, fill_value=0):
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            df[col] = fill_value
    return df


def _safe_json(response):
    try:
        return response.json()
    except Exception as exc:
        raise ValueError(f"JSON 응답 파싱 실패: {response.text}") from exc


def _request(method, uri, api_key, secret_key, customer_id, params=None, json_body=None):
    response = requests.request(
        method=method,
        url=f"{BASE_URL}{uri}",
        headers=get_header(method, uri, api_key, secret_key, customer_id),
        params=params,
        json=json_body,
        timeout=60,
    )
    response.raise_for_status()
    return response


def _request_json(method, uri, api_key, secret_key, customer_id, params=None, json_body=None):
    response = _request(
        method=method,
        uri=uri,
        api_key=api_key,
        secret_key=secret_key,
        customer_id=customer_id,
        params=params,
        json_body=json_body,
    )
    return _safe_json(response)


def _to_dataframe(payload):
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    if isinstance(payload, dict) and "data" in payload:
        return pd.DataFrame(payload["data"])
    if isinstance(payload, dict):
        return pd.DataFrame([payload])
    return pd.DataFrame()


def _extract_stat_entries(stat_data, fallback_date):
    if isinstance(stat_data, dict):
        stat_data = [stat_data]
    if not isinstance(stat_data, list):
        stat_data = [{}]

    entries = []
    for item in stat_data:
        if not isinstance(item, dict):
            item = {}
        stat_date = item.get("period") or item.get("statDt") or item.get("date") or fallback_date
        entries.append((stat_date, item))
    return entries


def _compute_kpis(df):
    if df.empty:
        return df

    df = df.copy()
    numeric_cols = ["Impression", "Click", "Cost", "Conversion count", "Sales by conversion"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["CPC"] = np.where(df["Click"] > 0, df["Cost"] / df["Click"], 0)
    df["CTR (%)"] = np.where(df["Impression"] > 0, df["Click"] / df["Impression"] * 100, 0)
    df["CVR"] = np.where(df["Click"] > 0, df["Conversion count"] / df["Click"] * 100, 0)
    df["ROAS (%)"] = np.where(df["Cost"] > 0, df["Sales by conversion"] / df["Cost"] * 100, 0)
    df = df.replace([np.inf, -np.inf], 0).fillna(0)
    return df


def _round_performance(df):
    if df.empty:
        return df
    return df.round(
        {
            "Cost": 0,
            "Impression": 0,
            "Click": 0,
            "CPC": 0,
            "CTR (%)": 2,
            "Conversion count": 0,
            "CVR": 2,
            "Sales by conversion": 0,
            "ROAS (%)": 2,
        }
    )


def summarize_daily(df):
    if df.empty:
        return _empty_dataframe(SUMMARY_COLUMNS)

    summary = (
        df.groupby("Date", as_index=False)[
            ["Cost", "Impression", "Click", "Conversion count", "Sales by conversion"]
        ]
        .sum()
    )
    summary = _compute_kpis(summary)
    summary = _ensure_columns(summary, SUMMARY_COLUMNS)
    summary = summary[SUMMARY_COLUMNS]
    return _round_performance(summary)


def _get_campaign_master(api_key, secret_key, customer_id):
    payload = _request_json("GET", "/ncc/campaigns", api_key, secret_key, customer_id)
    df = _to_dataframe(payload)

    if df.empty:
        return _empty_dataframe(["Campaign ID", "Campaign Name", "Campaign Type Raw", "Campaign Type"])

    required_cols = ["nccCampaignId", "name", "campaignTp"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"캠페인 응답에 필요한 컬럼이 없습니다: {missing}")

    df = df[required_cols].rename(
        columns={
            "nccCampaignId": "Campaign ID",
            "name": "Campaign Name",
            "campaignTp": "Campaign Type Raw",
        }
    )
    df["Campaign Type"] = df["Campaign Type Raw"].map(CAMPAIGN_TYPE_LABELS).fillna(df["Campaign Type Raw"])
    return df


def _get_adgroup_master(api_key, secret_key, customer_id):
    payload = _request_json("GET", "/ncc/adgroups", api_key, secret_key, customer_id)
    df = _to_dataframe(payload)
    if df.empty:
        return _empty_dataframe(["AD Group ID", "Ad group name"])

    return df[["nccAdgroupId", "name"]].rename(
        columns={"nccAdgroupId": "AD Group ID", "name": "Ad group name"}
    )


def _get_all_keywords(api_key, secret_key, customer_id, adgroup_ids):
    keyword_rows = []
    for adgroup_id in adgroup_ids:
        payload = _request_json(
            "GET",
            "/ncc/keywords",
            api_key,
            secret_key,
            customer_id,
            params={"nccAdgroupId": adgroup_id},
        )
        if isinstance(payload, list):
            keyword_rows.extend(payload)

    if not keyword_rows:
        return _empty_dataframe(["AD keyword ID", "Registered keyword", "AD Group ID"])

    return pd.DataFrame(keyword_rows)[["nccKeywordId", "keyword", "nccAdgroupId"]].rename(
        columns={
            "nccKeywordId": "AD keyword ID",
            "keyword": "Registered keyword",
            "nccAdgroupId": "AD Group ID",
        }
    )


def _fetch_campaign_stats(df_campaigns, since, until, api_key, secret_key, customer_id):
    if df_campaigns.empty:
        return []

    stat_rows = []
    fallback_date = since if since == until else None
    fields = json.dumps(
        ["impCnt", "clkCnt", "salesAmt", "ctr", "cpc", "purchaseCcnt", "purchaseConvAmt"]
    )

    for _, row in df_campaigns.iterrows():
        payload = _request_json(
            "GET",
            "/stats",
            api_key,
            secret_key,
            customer_id,
            params={
                "id": row["Campaign ID"],
                "fields": fields,
                "timeRange": json.dumps({"since": since, "until": until}),
                "timeIncrement": "1",
            },
        )

        stat_data = payload["data"] if isinstance(payload, dict) and "data" in payload else payload
        for stat_date, stat in _extract_stat_entries(stat_data, fallback_date):
            stat_rows.append(
                {
                    "Date": stat_date,
                    "Campaign Type": row["Campaign Type"],
                    "Campaign Name": row["Campaign Name"],
                    "Campaign ID": row["Campaign ID"],
                    "Impression": stat.get("impCnt", 0),
                    "Click": stat.get("clkCnt", 0),
                    "Cost": stat.get("salesAmt", 0),
                    "Conversion count": stat.get("purchaseCcnt", 0),
                    "Sales by conversion": stat.get("purchaseConvAmt", 0),
                }
            )
        time.sleep(0.1)

    return stat_rows


def run_naver_report(
    report_date=None,
    api_key=None,
    secret_key=None,
    customer_id=None,
    campaign_type="전체",
    start_date=None,
    end_date=None,
):
    since, until = _resolve_report_range(report_date, start_date, end_date)
    df_campaigns = _get_campaign_master(api_key, secret_key, customer_id)

    if campaign_type != "전체":
        df_campaigns = df_campaigns[df_campaigns["Campaign Type"] == campaign_type].copy()

    stat_rows = _fetch_campaign_stats(df_campaigns, since, until, api_key, secret_key, customer_id)
    if not stat_rows:
        return _empty_dataframe(SUMMARY_COLUMNS)

    df = pd.DataFrame(stat_rows)
    df = _compute_kpis(df)
    df = _ensure_columns(df, DETAIL_COLUMNS)
    df = _round_performance(df[DETAIL_COLUMNS])
    return summarize_daily(df)


def run_brand_naver_report(
    report_date=None,
    api_key=None,
    secret_key=None,
    customer_id=None,
    brand_cost=0,
    campaign_type="전체",
    start_date=None,
    end_date=None,
):
    since, until = _resolve_report_range(report_date, start_date, end_date)
    df_campaigns = _get_campaign_master(api_key, secret_key, customer_id)
    df_campaigns = df_campaigns[df_campaigns["Campaign Type"] == "브랜드검색"].copy()

    if campaign_type != "전체":
        df_campaigns = df_campaigns[df_campaigns["Campaign Type"] == campaign_type].copy()

    stat_rows = _fetch_campaign_stats(df_campaigns, since, until, api_key, secret_key, customer_id)
    if not stat_rows:
        return _empty_dataframe(SUMMARY_COLUMNS)

    df = pd.DataFrame(stat_rows)
    summary = summarize_daily(df)
    if summary.empty:
        return summary

    summary["Cost"] = float(brand_cost)
    summary = _compute_kpis(summary)
    summary = _ensure_columns(summary, SUMMARY_COLUMNS)
    summary = summary[SUMMARY_COLUMNS]
    return _round_performance(summary)


def get_shopping_campaign_ids(api_key, secret_key, customer_id):
    df_campaigns = _get_campaign_master(api_key, secret_key, customer_id)
    df_campaigns = df_campaigns[df_campaigns["Campaign Type Raw"] == "SHOPPING"].copy()
    return df_campaigns[["Campaign ID", "Campaign Name"]]


def download_stat_report(report_date, report_type, api_key, secret_key, customer_id, wait_seconds=2, max_attempts=10):
    report_date = _format_naver_date(report_date)

    _request(
        "POST",
        "/stat-reports",
        api_key,
        secret_key,
        customer_id,
        json_body={"reportTp": report_type, "statDt": report_date},
    )

    report_list = pd.DataFrame()
    for _ in range(max_attempts):
        payload = _request_json("GET", "/stat-reports", api_key, secret_key, customer_id)
        report_list = pd.DataFrame(payload)
        if not report_list.empty:
            filtered = report_list[
                (report_list["reportTp"] == report_type) & (report_list["status"] == "BUILT")
            ].copy()
            if not filtered.empty:
                report_list = filtered.sort_values("regTm", ascending=False).reset_index(drop=True)
                break
        time.sleep(wait_seconds)
    else:
        raise ValueError(f"{report_type} 리포트 생성이 완료되지 않았습니다.")

    download_url = report_list["downloadUrl"].iloc[0]
    query = dict(parse_qsl(urlsplit(download_url).query, keep_blank_values=True))
    token = query.get("authtoken")
    file_version = query.get("fileVersion") or query.get("fileversion") or "v2"

    response = _request(
        "GET",
        "/report-download",
        api_key,
        secret_key,
        customer_id,
        params={"authtoken": token, "fileVersion": file_version},
    )
    df_raw = pd.read_csv(StringIO(response.text), sep="\t", header=None)
    df_raw.columns = [f"col_{idx}" for idx in range(len(df_raw.columns))]
    return df_raw


def _safe_download_stat_report(report_date, report_type, columns, api_key, secret_key, customer_id):
    try:
        df = download_stat_report(
            report_date=report_date,
            report_type=report_type,
            api_key=api_key,
            secret_key=secret_key,
            customer_id=customer_id,
        )
        if df.empty:
            return _empty_dataframe(columns)
        df.columns = columns
        return df
    except Exception as exc:
        print(f"[{report_type}] {report_date} report empty/failed: {exc}")
        return _empty_dataframe(columns)


def _get_keyword_adgroup_master(api_key, secret_key, customer_id):
    columns = ["AD Group ID", "Ad group name", "Campaign ID", "Adgroup Type"]
    df_campaigns = _get_campaign_master(api_key, secret_key, customer_id)

    adgroup_rows = []
    for campaign_id in df_campaigns["Campaign ID"].dropna().astype(str).unique():
        try:
            payload = _request_json(
                "GET",
                "/ncc/adgroups",
                api_key,
                secret_key,
                customer_id,
                params={"nccCampaignId": campaign_id},
            )
            if isinstance(payload, list):
                adgroup_rows.extend(payload)
            elif isinstance(payload, dict) and "data" in payload:
                adgroup_rows.extend(payload["data"])
        except Exception as exc:
            print(f"[adgroups] campaign={campaign_id} failed: {exc}")
        time.sleep(0.1)

    df = pd.DataFrame(adgroup_rows)
    if df.empty:
        return _empty_dataframe(columns)

    needed = ["nccAdgroupId", "name", "nccCampaignId", "adgroupType"]
    for col in needed:
        if col not in df.columns:
            df[col] = ""

    return df[needed].rename(
        columns={
            "nccAdgroupId": "AD Group ID",
            "name": "Ad group name",
            "nccCampaignId": "Campaign ID",
            "adgroupType": "Adgroup Type",
        }
    )


def _normalize_id_columns(df, columns):
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"nan": "", "None": ""})
    return df


def _group_conversion_breakdown(df, group_keys):
    result_cols = group_keys + [
        "Total conversion count",
        "Total sales by conversion",
        "Purchase conversion count",
        "Purchase sales by conversion",
        "Cart conversion count",
        "Cart sales by conversion",
    ]
    if df.empty:
        return _empty_dataframe(result_cols)

    df = df.copy()
    df["Conversion Type"] = df["Conversion Type"].fillna(0).astype(str).str.strip().str.lower()
    df["Conversion count"] = pd.to_numeric(df["Conversion count"], errors="coerce").fillna(0)
    df["Sales by conversion"] = pd.to_numeric(df["Sales by conversion"], errors="coerce").fillna(0)

    purchase_values = {"1", "1.0", "purchase", "purchasing", "purchased", "구매", "구매완료"}
    cart_values = {"2", "2.0", "add_to_cart", "cart", "장바구니", "장바구니담기"}

    total = (
        df.groupby(group_keys, as_index=False)[["Conversion count", "Sales by conversion"]]
        .sum()
        .rename(
            columns={
                "Conversion count": "Total conversion count",
                "Sales by conversion": "Total sales by conversion",
            }
        )
    )
    purchase = (
        df[df["Conversion Type"].isin(purchase_values)]
        .groupby(group_keys, as_index=False)[["Conversion count", "Sales by conversion"]]
        .sum()
        .rename(
            columns={
                "Conversion count": "Purchase conversion count",
                "Sales by conversion": "Purchase sales by conversion",
            }
        )
    )
    cart = (
        df[df["Conversion Type"].isin(cart_values)]
        .groupby(group_keys, as_index=False)[["Conversion count", "Sales by conversion"]]
        .sum()
        .rename(
            columns={
                "Conversion count": "Cart conversion count",
                "Sales by conversion": "Cart sales by conversion",
            }
        )
    )

    result = total.merge(purchase, on=group_keys, how="outer").merge(cart, on=group_keys, how="outer")
    for col in result_cols:
        if col not in result.columns:
            result[col] = 0
    for col in result_cols[len(group_keys):]:
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)
    return result[result_cols]


def _keyword_output_columns():
    return [
        "Date",
        "Campaign Type",
        "Campaign name",
        "Ad group name",
        "Search keyword",
        "Impression",
        "Click",
        "Cost",
        "Total conversion count",
        "Total sales by conversion",
        "Purchase conversion count",
        "Purchase sales by conversion",
        "Average rank",
        "Cart conversion count",
        "Cart sales by conversion",
        "Search Type",
    ]


def _normalize_keyword_output(df):
    columns = _keyword_output_columns()
    numeric_cols = [
        "Impression",
        "Click",
        "Cost",
        "Total conversion count",
        "Total sales by conversion",
        "Purchase conversion count",
        "Purchase sales by conversion",
        "Average rank",
        "Cart conversion count",
        "Cart sales by conversion",
    ]
    if df is None or df.empty:
        return _empty_dataframe(columns)

    df = df.copy()
    for col in columns:
        if col not in df.columns:
            df[col] = 0 if col in numeric_cols else ""
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in [col for col in columns if col not in numeric_cols]:
        df[col] = df[col].astype(str).replace("nan", "").fillna("")
    return df[columns].round(
        {
            "Cost": 0,
            "Total conversion count": 0,
            "Total sales by conversion": 0,
            "Purchase conversion count": 0,
            "Purchase sales by conversion": 0,
            "Average rank": 1,
            "Cart conversion count": 0,
            "Cart sales by conversion": 0,
        }
    )


def _drop_expanded_when_exact_exists(df):
    if df.empty or "Search Type" not in df.columns:
        return df

    df = df.copy()
    dedupe_keys = ["Date", "Campaign ID", "AD Group ID", "Search keyword"]
    for col in dedupe_keys + ["Search Type"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    exact_keys = set(
        map(
            tuple,
            df[df["Search Type"] == "일치"][dedupe_keys].drop_duplicates().to_numpy(),
        )
    )
    if not exact_keys:
        return df

    is_expanded_duplicate = df.apply(
        lambda row: row["Search Type"] == "확장"
        and tuple(row[col] for col in dedupe_keys) in exact_keys,
        axis=1,
    )
    return df[~is_expanded_duplicate].copy()


def get_shopping_keyword_report(report_date, api_key, secret_key, customer_id):
    perf_cols = [
        "Date",
        "CUSTOMER ID",
        "Campaign ID",
        "AD Group ID",
        "Search keyword",
        "AD ID",
        "Business Channel ID",
        "Hours",
        "Region code",
        "Media code",
        "PC Mobile Type",
        "Impression",
        "Click",
        "Cost",
        "Sum of AD rank",
        "View count",
    ]
    conv_cols = [
        "Date",
        "CUSTOMER ID",
        "Campaign ID",
        "AD Group ID",
        "Search keyword",
        "AD ID",
        "Business Channel ID",
        "Hours",
        "Region code",
        "Media code",
        "PC Mobile Type",
        "Conversion Method",
        "Conversion Type",
        "Conversion count",
        "Sales by conversion",
    ]

    df_perf = _safe_download_stat_report(
        report_date, "SHOPPINGKEYWORD_DETAIL", perf_cols, api_key, secret_key, customer_id
    )
    df_conv = _safe_download_stat_report(
        report_date, "SHOPPINGKEYWORD_CONVERSION_DETAIL", conv_cols, api_key, secret_key, customer_id
    )

    df_campaign = _get_campaign_master(api_key, secret_key, customer_id)[
        ["Campaign ID", "Campaign Name", "Campaign Type"]
    ].rename(columns={"Campaign Name": "Campaign name"})
    df_adgroup = _get_keyword_adgroup_master(api_key, secret_key, customer_id)[
        ["AD Group ID", "Ad group name", "Campaign ID", "Adgroup Type"]
    ]
    df_master = df_adgroup.merge(df_campaign, on="Campaign ID", how="left")
    df_perf = _normalize_id_columns(df_perf, ["Campaign ID", "AD Group ID"])
    df_conv = _normalize_id_columns(df_conv, ["Campaign ID", "AD Group ID"])
    df_master = _normalize_id_columns(df_master, ["Campaign ID", "AD Group ID"])

    for col in ["Impression", "Click", "Cost", "Sum of AD rank"]:
        if col in df_perf.columns:
            df_perf[col] = pd.to_numeric(df_perf[col], errors="coerce").fillna(0)
    for col in ["Conversion count", "Sales by conversion"]:
        if col in df_conv.columns:
            df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").fillna(0)

    perf_keys = ["Date", "Campaign ID", "AD Group ID", "Search keyword"]
    df_perf_grouped = (
        df_perf.groupby(perf_keys, as_index=False)
        .agg(
            {
                "Impression": "sum",
                "Click": "sum",
                "Cost": "sum",
                "Sum of AD rank": "sum",
            }
        )
    )
    df_perf_grouped["Average rank"] = np.where(
        df_perf_grouped["Impression"] > 0,
        df_perf_grouped["Sum of AD rank"] / df_perf_grouped["Impression"],
        0,
    )

    df_conv_grouped = _group_conversion_breakdown(df_conv, perf_keys)

    df_final = df_perf_grouped.merge(df_conv_grouped, on=perf_keys, how="left")
    df_final = df_final.merge(
        df_master.drop_duplicates(subset=["Campaign ID", "AD Group ID"]),
        on=["Campaign ID", "AD Group ID"],
        how="left",
    )
    df_final["Search Type"] = ""
    return _normalize_keyword_output(df_final)


def get_powerlink_keyword_report(report_date, api_key, secret_key, customer_id):
    ad_cols = [
        "Date",
        "CUSTOMER ID",
        "Campaign ID",
        "AD Group ID",
        "AD keyword ID",
        "AD ID",
        "Business Channel ID",
        "Media code",
        "PC Mobile Type",
        "Impression",
        "Click",
        "Cost",
        "Sum of AD rank",
        "View count",
    ]
    conv_cols = [
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
        "Sales by conversion",
    ]
    exp_cols = [
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
        "View count",
    ]

    df_ad = _safe_download_stat_report(report_date, "AD", ad_cols, api_key, secret_key, customer_id)
    df_exp = _safe_download_stat_report(report_date, "EXPKEYWORD", exp_cols, api_key, secret_key, customer_id)
    df_conv = _safe_download_stat_report(
        report_date, "AD_CONVERSION_DETAIL", conv_cols, api_key, secret_key, customer_id
    )

    df_campaign = _get_campaign_master(api_key, secret_key, customer_id)[
        ["Campaign ID", "Campaign Name", "Campaign Type"]
    ].rename(columns={"Campaign Name": "Campaign name"})
    df_adgroup = _get_keyword_adgroup_master(api_key, secret_key, customer_id)[
        ["AD Group ID", "Ad group name", "Campaign ID", "Adgroup Type"]
    ]
    df_keyword = _get_all_keywords(
        api_key,
        secret_key,
        customer_id,
        df_adgroup["AD Group ID"].dropna().unique(),
    )
    df_kw_map = (
        df_keyword.rename(columns={"Registered keyword": "Search keyword"})
        if not df_keyword.empty
        else _empty_dataframe(["AD keyword ID", "Search keyword", "AD Group ID"])
    )
    df_master = df_adgroup.merge(df_campaign, on="Campaign ID", how="left")
    df_ad = _normalize_id_columns(df_ad, ["Campaign ID", "AD Group ID", "AD keyword ID"])
    df_exp = _normalize_id_columns(df_exp, ["Campaign ID", "AD Group ID"])
    df_conv = _normalize_id_columns(df_conv, ["Campaign ID", "AD Group ID", "AD keyword ID"])
    df_kw_map = _normalize_id_columns(df_kw_map, ["AD Group ID", "AD keyword ID"])
    df_master = _normalize_id_columns(df_master, ["Campaign ID", "AD Group ID"])

    for col in ["Impression", "Click", "Cost", "Sum of AD rank"]:
        if col in df_ad.columns:
            df_ad[col] = pd.to_numeric(df_ad[col], errors="coerce").fillna(0)
    for col in ["Impression", "Click", "Cost"]:
        if col in df_exp.columns:
            df_exp[col] = pd.to_numeric(df_exp[col], errors="coerce").fillna(0)
    exact_keys = ["Date", "Campaign ID", "AD Group ID", "AD keyword ID"]
    df_ad = df_ad.merge(
        df_kw_map.drop_duplicates(subset=["AD Group ID", "AD keyword ID"]),
        on=["AD Group ID", "AD keyword ID"],
        how="left",
    )
    df_ad_grouped = (
        df_ad.groupby(exact_keys + ["Search keyword"], as_index=False)
        .agg({"Impression": "sum", "Click": "sum", "Cost": "sum", "Sum of AD rank": "sum"})
    )
    df_ad_grouped["Average rank"] = np.where(
        df_ad_grouped["Impression"] > 0,
        df_ad_grouped["Sum of AD rank"] / df_ad_grouped["Impression"],
        0,
    )

    df_conv["AD keyword ID"] = df_conv["AD keyword ID"].astype(str).str.strip()
    df_ad_grouped["AD keyword ID"] = df_ad_grouped["AD keyword ID"].astype(str).str.strip()
    df_conv_grouped = _group_conversion_breakdown(df_conv, exact_keys)

    df_exact = df_ad_grouped.merge(df_conv_grouped, on=exact_keys, how="left")
    df_exact = df_exact.merge(
        df_master.drop_duplicates(subset=["Campaign ID", "AD Group ID"]),
        on=["Campaign ID", "AD Group ID"],
        how="left",
    )
    df_exact["Search Type"] = "일치"

    exp_keys = ["Date", "Campaign ID", "AD Group ID", "Search keyword"]
    df_exp_grouped = (
        df_exp.groupby(exp_keys, as_index=False)
        .agg({"Impression": "sum", "Click": "sum", "Cost": "sum"})
    )
    df_exp_grouped["Average rank"] = 0
    for col in [
        "Total conversion count",
        "Total sales by conversion",
        "Purchase conversion count",
        "Purchase sales by conversion",
        "Cart conversion count",
        "Cart sales by conversion",
    ]:
        df_exp_grouped[col] = 0
    df_exp_grouped = df_exp_grouped.merge(
        df_master.drop_duplicates(subset=["Campaign ID", "AD Group ID"]),
        on=["Campaign ID", "AD Group ID"],
        how="left",
    )
    df_exp_grouped["Search Type"] = "확장"

    df_all = pd.concat([df_exact, df_exp_grouped], ignore_index=True)
    df_all = _drop_expanded_when_exact_exists(df_all)
    return _normalize_keyword_output(df_all)


def get_campaign_report(
    report_date=None,
    api_key=None,
    secret_key=None,
    customer_id=None,
    campaign_type="ALL",
    start_date=None,
    end_date=None,
):
    since, until = _resolve_report_range(report_date, start_date, end_date)
    df_campaigns = _get_campaign_master(api_key, secret_key, customer_id)
    period_label = since if since == until else f"{since}~{until}"

    target_types = ["SHOPPING", "WEB_SITE", "PLACE", "POWER_CONTENTS", "SHOPPING_BRAND", "BRAND_SEARCH"]
    if campaign_type == "ALL":
        df_campaigns = df_campaigns[df_campaigns["Campaign Type Raw"].isin(target_types)].copy()
    else:
        df_campaigns = df_campaigns[df_campaigns["Campaign Type Raw"] == campaign_type].copy()

    stat_rows = _fetch_campaign_stats(df_campaigns, since, until, api_key, secret_key, customer_id)
    if not stat_rows:
        return _empty_dataframe(CAMPAIGN_REPORT_COLUMNS)

    df = pd.DataFrame(stat_rows).rename(columns={"Campaign Name": "Campaign name"})
    df = (
        df.groupby(["Campaign Type", "Campaign name", "Campaign ID"], as_index=False)[
            ["Impression", "Click", "Cost", "Conversion count", "Sales by conversion"]
        ]
        .sum()
    )
    df.insert(0, "Date", period_label)
    df = _compute_kpis(df)
    df = _ensure_columns(df, CAMPAIGN_REPORT_COLUMNS)
    df = df[CAMPAIGN_REPORT_COLUMNS]
    return _round_performance(df)
