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







''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


# 네이버 광고 데이터 (전체캠페인 합 - 일별데이터)

def run_naver_report(report_date, api_key, secret_key, customer_id, campaign_ids):
    
    
    API_KEY = api_key
    SECRET_KEY = secret_key
    CUSTOMER_ID = customer_id
        
        
    class Signature:
        @staticmethod
        def generate(timestamp, method, uri, secret_key):
            message = f"{timestamp}.{method}.{uri}"
            hash = hmac.new(bytes(secret_key, "utf-8"), bytes(message, "utf-8"), hashlib.sha256)
            return base64.b64encode(hash.digest()).decode()

    def get_header(method, uri, api_key, secret_key, customer_id):
        timestamp = str(round(time.time() * 1000))
        signature = Signature.generate(timestamp, method, uri, secret_key)
        return {
            'Content-Type': 'application/json; charset=UTF-8',
            'X-Timestamp': timestamp,
            'X-API-KEY': api_key,
            'X-Customer': str(customer_id),
            'X-Signature': signature
        }


    BASE_URL = 'https://api.searchad.naver.com'

    uri = '/ncc/campaigns'
    method = 'GET'
    response = requests.get(BASE_URL + uri, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))

    # 응답 확인
    if response.status_code == 200:
        campaigns = response.json()

        # json 구조가 list 형태일 때 DataFrame 변환
        if isinstance(campaigns, list):
            df_cam = pd.DataFrame(campaigns)[['nccCampaignId', 'name']]
            df_cam = df_cam.rename(columns={'nccCampaignId': 'Campaign ID', 'name': 'Campaign Name'})
            print(df_cam.head())

        # json이 dict 형태일 때 (예: {'data': [...]} 형태)
        elif isinstance(campaigns, dict):
            if 'data' in campaigns:
                df_cam = pd.DataFrame(campaigns['data'])[['nccCampaignId', 'name']]
                df_cam = df_cam.rename(columns={'nccCampaignId': 'Campaign ID', 'name': 'Campaign Name'})
                print(df_cam.head())
            else:
                print("⚠️ 예상치 못한 JSON 구조:", campaigns)

    else:
        print(f"❌ 요청 실패: {response.status_code}")
        print(response.text)



    # 전환수 포함 리포트를 요청하는 코드 
    # 전환수 구하기
    uri = '/stat-reports'
    method = 'POST'

    params = {
        'reportTp': 'AD_CONVERSION_DETAIL',   # 전환수 리포트
        'statDt': report_date       # 보고서 날짜
    }


    # 리포트 생성 요청
    r = requests.post(BASE_URL + uri, json=params, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))

    # 리포트 생성이 완료될 때까지 대기
    time.sleep(10)

    # 리포트 다운로드 URL 조회
    uri = '/stat-reports'
    method = 'GET'

    r = requests.get(BASE_URL + uri, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
    data = r.json()
    data_1 = pd.DataFrame(data)
    data_1 = data_1[
        (data_1["reportTp"] == "AD_CONVERSION_DETAIL") &
        (data_1["status"] == "BUILT")
    ].copy()

    data_1 = data_1.sort_values(by="regTm", ascending=False).reset_index(drop=True)

    url = data_1["downloadUrl"].iloc[0]

    # 쿼리 파라미터 파싱
    qs = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
    token = qs.get('authtoken')
    file_version =  qs.get('fileVersion') or qs.get('fileversion') or 'v2'


    # # 리포트 다운로드
    uri = '/report-download'
    method = 'GET'

    r = requests.get(BASE_URL + uri,   params={'authtoken': token, 'fileVersion': file_version}, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
    data = StringIO(r.text)

    # 전환수 구하기
    column_names = ["Date","CUSTOMER ID","Campaign ID", "AD Group ID", "AD keyword ID", "AD ID", "Business Channel ID", "Hours","Region code", "Media code", "PC Mobile Type",
                    "Conversion Method", "Conversion Type","Conversion count","Sales by conversion"]

    df_report = pd.read_csv(data, sep='\t', header=None, names=column_names)
    # print(df_report)

    df_report = df_report.merge(df_cam, on='Campaign ID', how='left')
    df_report = df_report.rename(columns={'name': 'Campaign Name'})
    df_report = df_report.fillna(0).replace([float('inf'), float('-inf')], 0)

    priority_map = {
        'purchase': 2,
        'add_to_cart': 1,
        0: 0,
        '0': 0
    }

    df_report_group = df_report.groupby(
        ['Campaign Name', 'Campaign ID', 'Conversion Type'],
        as_index=False
    )[['Conversion count', 'Sales by conversion']].sum()
    print(df_report_group)
    df_report_group['priority'] = (
        df_report_group['Conversion Type']
        .map(priority_map)
        .fillna(0)
    )

    df_report_group = df_report_group.loc[
        df_report_group.groupby('Campaign ID')['priority'].idxmax()
    ].reset_index(drop=True)

    df_report_group = df_report_group.drop(columns=['priority'])

    print(df_report_group)





    # 총비용,  구하기
    uri2 = '/stat-reports'
    method2 = 'POST'

    # reportTp를 'CONVERSION'으로 설정하여 전환 데이터가 포함된 리포트 요청
    params2 = {
        'reportTp': 'AD',   # 전환수 리포트
        'statDt': report_date     # 보고서 날짜
    }

    # 리포트 생성 요청
    r2 = requests.post(BASE_URL + uri2, json=params2, headers=get_header(method2, uri2, API_KEY, SECRET_KEY, CUSTOMER_ID))

    # 리포트 생성이 완료될 때까지 대기
    time.sleep(10)

    # 리포트 다운로드 URL 조회
    uri2 = '/stat-reports'
    method2 = 'GET'

    r2 = requests.get(BASE_URL + uri2, headers=get_header(method2, uri2, API_KEY, SECRET_KEY, CUSTOMER_ID))
    data2 = r2.json()
    data_2 = pd.DataFrame(data2)
    data_2 = data_2[
        (data_2["reportTp"] == "AD") &
        (data_2["status"] == "BUILT")
    ].copy()

    data_2 = data_2.sort_values(by="regTm", ascending=False).reset_index(drop=True)

    url2 = data_2["downloadUrl"].iloc[0]
    # 쿼리 파라미터 파싱
    parsed = dict(parse_qsl(urlsplit(url2).query, keep_blank_values=True))
    token2 = parsed.get('authtoken')
    file_version =  parsed.get('fileVersion') or qs.get('fileversion') or 'v2'

    # token2 = data_2['downloadUrl'][0].split('=')[1]

    # 리포트 다운로드
    uri2 = '/report-download'
    method2 = 'GET'

    r2 = requests.get(BASE_URL + uri2, params={'authtoken': token2, 'fileVersion': file_version}, headers=get_header(method2, uri2, API_KEY, SECRET_KEY, CUSTOMER_ID))
    data2 = StringIO(r2.text)

    column_names2 = ["Date", "Customer_ID", "Campaign ID","AD Group ID","AD keyword ID","AD ID","Business Channel ID",
                    "Media Code","PC Mobile Type","Impression","Click","Cost","Sum of Ad Rank","View Count"

    ]

    df_report2 = pd.read_csv(data2, sep='\t', header=None, names=column_names2)
    df_report2 = df_report2.merge(df_cam, on='Campaign ID', how='left')
    df_report2 = df_report2.rename(columns={'name': 'Campaign Name'})
    df_report2 = df_report2.fillna(0).replace([float('inf'), float('-inf')], 0)
    df_report2['Cost'] = df_report2['Cost'] 
    df_report_group2 = df_report2.groupby(['Campaign Name','Campaign ID'])[['Impression','Click','Cost']].sum().reset_index()
    # 'Campaign Name' 열을 문자열 형식으로 변환
    df_report_group['Campaign Name'] = df_report_group['Campaign Name'].astype(str)
    df_report_group2['Campaign Name'] = df_report_group2['Campaign Name'].astype(str)

    # 두 데이터프레임 병합
    df_combined = pd.merge(df_report_group, df_report_group2, on=['Campaign Name', 'Campaign ID'], how='outer')
    # CTR 및 CPC 계산
    df_combined['CTR (%)'] = (df_combined['Click'] / df_combined['Impression']) * 100
    df_combined['CPC'] = df_combined['Cost'] / df_combined['Click']
    df_combined['ROAS (%)'] = (df_combined['Sales by conversion'] / df_combined['Cost']) * 100
    # df_combined['CPCV'] = (df_combined['Cost'] / df_combined['Conversion count']) 
    df_combined['CVR'] = (df_combined['Conversion count']/ df_combined['Click']) * 100

    # 결측값을 0으로 채워줌
    df_combined = df_combined.fillna(0)


    # 데이터 형식 지정 및 포맷 적용
    df_combined['CTR (%)'] = df_combined['CTR (%)'].apply(lambda x: f"{round(x, 2):,.2f}".rstrip('0').rstrip('.') + "%")
    df_combined['ROAS (%)'] = df_combined['ROAS (%)'].apply(lambda x: f"{round(x, 2):,.2f}".rstrip('0').rstrip('.') + "%")
    df_combined['CPC'] = df_combined['CPC'].apply(lambda x: f"{round(x, 2):,.2f}".rstrip('0').rstrip('.'))
    df_combined['Cost'] = df_combined['Cost'].apply(lambda x: f"{x:,.2f}".rstrip('0').rstrip('.'))
    df_combined['Impression'] = df_combined['Impression'].apply(lambda x: f"{x:,}")
    df_combined['Click'] = df_combined['Click'].apply(lambda x: f"{x:,}")
    df_combined['Sales by conversion'] = df_combined['Sales by conversion'].apply(lambda x: f"{x:,.2f}".rstrip('0').rstrip('.'))
    df_combined['Conversion count'] = df_combined['Conversion count'].apply(lambda x: f"{x:,}")
    # df_combined['CPCV'] = df_combined['CPCV'].apply(lambda x: f"{round(x, 2):,.2f}".rstrip('0').rstrip('.'))
    df_combined['CVR'] = df_combined['CVR'].apply(lambda x: f"{round(x, 2):,.2f}".rstrip('0').rstrip('.') + "%")




    # Date 열을 추가하고 지정된 날짜 값을 채워넣기
    df_combined['Date'] = report_date

    # 열 순서를 조정하여 Date 열이 첫 번째 열로 오게 함
    df_combined = df_combined[['Date', 'Campaign Name','Campaign ID', 'Cost', 'Impression', 'Click','CPC','CTR (%)','Conversion Type', 'Conversion count', 
                            'Sales by conversion','ROAS (%)','CVR']]
    # df_combined = df_combined[df_combined['Conversion Type'].isin(['purchase', 0])]

    df_combined.loc[
        df_combined['Conversion Type'] == 'add_to_cart',
        ['Conversion count', 'Sales by conversion', 'ROAS (%)', 'CVR']
    ] = 0

    ''''''''''''
    
    for col in ["CTR (%)", "ROAS (%)",'CVR', "CPC", "Cost", "Impression", "Click", "Conversion count", "Sales by conversion"]:
        df_combined[col] = df_combined[col].astype(str).str.replace(",", "").str.replace("%", "").replace("inf", float("inf")).astype(float)
    
    filtered_data = df_combined[df_combined["Campaign ID"].isin(campaign_ids)]
    
        # 순서정렬 
    filtered_data = filtered_data[['Date','Campaign ID', 'Campaign Name', 'Impression', 'Click','CTR (%)','CPC', 'Cost', 'Conversion count', 
                            'Sales by conversion', 'CVR', 'ROAS (%)']]


    summable_columns = ["Cost", "Impression", "Click", "Conversion count", "Sales by conversion"]
    summed_values = filtered_data[summable_columns].sum()


    result = df_combined[['Date','Campaign ID', 'Campaign Name', 'Cost', 'Impression', 'Click','CPC','CTR (%)', 'Conversion count', 
                           'Sales by conversion','ROAS (%)']]
    result = pd.DataFrame(summed_values).T
    result['Cost'] = result['Cost'].replace(0, np.nan)
    result['Click'] = result['Click'].replace(0, np.nan)
    result['Impression'] = result['Impression'].replace(0, np.nan)
    result['Conversion count'] = result['Conversion count'].replace(0, np.nan)

    result['Date'] = report_date
    result['ROAS (%)'] = (result['Sales by conversion'] / result['Cost']) *100
    result['CPC'] = (result['Cost'] / result['Click'] ) 
    result['CTR (%)'] = (result['Click'] / result['Impression']) *100
    # result['CPCV'] = (result['Cost'] / result['Conversion count']) 
    result['CVR'] = (result['Conversion count']/ result['Click']) *100

    # 필요한 컬럼들을 float로 변환 (문자열이 포함되어 있을 경우 대비)
    result['CVR'] = pd.to_numeric(result['CVR'], errors='coerce')
    result['CTR (%)'] = pd.to_numeric(result['CTR (%)'], errors='coerce')
    result['ROAS (%)'] = pd.to_numeric(result['ROAS (%)'], errors='coerce')
    result['CPC'] = pd.to_numeric(result['CPC'], errors='coerce')
    result['Cost'] = pd.to_numeric(result['Cost'], errors='coerce')
    result['Sales by conversion'] = pd.to_numeric(result['Sales by conversion'], errors='coerce')
    # result['CPCV'] = pd.to_numeric(result['CPCV'], errors='coerce')
    result['CTR (%)'] = (result['Click'] / result['Impression']) 

    # NaN 값이 있으면 0으로 변환
    result = result.fillna(0)

    result = result.round({
    # 'CTR (%)': 2,
    # 'ROAS (%)': 2,
        'CPC': 0,
        'Cost': 2,
        'Sales by conversion': 2
    })
   

    # 열 순서 재정렬
    result = result[['Date', 'Cost','Impression', 'Click',   'CPC', 'CTR (%)', 'Conversion count' ,  'CVR', 'Sales by conversion', 'ROAS (%)']]
    

    return result
    





def run_brand_naver_report(report_date, api_key, secret_key, customer_id, campaign_ids, brand_cost):
    
    
    API_KEY = api_key
    SECRET_KEY = secret_key
    CUSTOMER_ID = customer_id
        
        
    class Signature:
        @staticmethod
        def generate(timestamp, method, uri, secret_key):
            message = f"{timestamp}.{method}.{uri}"
            hash = hmac.new(bytes(secret_key, "utf-8"), bytes(message, "utf-8"), hashlib.sha256)
            return base64.b64encode(hash.digest()).decode()

    def get_header(method, uri, api_key, secret_key, customer_id):
        timestamp = str(round(time.time() * 1000))
        signature = Signature.generate(timestamp, method, uri, secret_key)
        return {
            'Content-Type': 'application/json; charset=UTF-8',
            'X-Timestamp': timestamp,
            'X-API-KEY': api_key,
            'X-Customer': str(customer_id),
            'X-Signature': signature
        }


    BASE_URL = 'https://api.searchad.naver.com'

    uri = '/ncc/campaigns'
    method = 'GET'
    response = requests.get(BASE_URL + uri, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))

    # 응답 확인
    if response.status_code == 200:
        campaigns = response.json()

        # json 구조가 list 형태일 때 DataFrame 변환
        if isinstance(campaigns, list):
            df_cam = pd.DataFrame(campaigns)[['nccCampaignId', 'name']]
            df_cam = df_cam.rename(columns={'nccCampaignId': 'Campaign ID', 'name': 'Campaign Name'})
            print(df_cam.head())

        # json이 dict 형태일 때 (예: {'data': [...]} 형태)
        elif isinstance(campaigns, dict):
            if 'data' in campaigns:
                df_cam = pd.DataFrame(campaigns['data'])[['nccCampaignId', 'name']]
                df_cam = df_cam.rename(columns={'nccCampaignId': 'Campaign ID', 'name': 'Campaign Name'})
                print(df_cam.head())
            else:
                print("⚠️ 예상치 못한 JSON 구조:", campaigns)

    else:
        print(f"❌ 요청 실패: {response.status_code}")
        print(response.text)



    # 전환수 포함 리포트를 요청하는 코드 
    # 전환수 구하기
    uri = '/stat-reports'
    method = 'POST'

    params = {
        'reportTp': 'AD_CONVERSION_DETAIL',   # 전환수 리포트
        'statDt': report_date       # 보고서 날짜
    }


    # 리포트 생성 요청
    r = requests.post(BASE_URL + uri, json=params, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))

    # 리포트 생성이 완료될 때까지 대기
    time.sleep(10)

    # 리포트 다운로드 URL 조회
    uri = '/stat-reports'
    method = 'GET'

    r = requests.get(BASE_URL + uri, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
    data = r.json()
    data_1 = pd.DataFrame(data)
    data_1 = data_1[
        (data_1["reportTp"] == "AD_CONVERSION_DETAIL") &
        (data_1["status"] == "BUILT")
    ].copy()

    data_1 = data_1.sort_values(by="regTm", ascending=False).reset_index(drop=True)

    url = data_1["downloadUrl"].iloc[0]

    # 쿼리 파라미터 파싱
    qs = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
    token = qs.get('authtoken')
    file_version =  qs.get('fileVersion') or qs.get('fileversion') or 'v2'


    # # 리포트 다운로드
    uri = '/report-download'
    method = 'GET'

    r = requests.get(BASE_URL + uri,   params={'authtoken': token, 'fileVersion': file_version}, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
    data = StringIO(r.text)

    # 전환수 구하기
    column_names = ["Date","CUSTOMER ID","Campaign ID", "AD Group ID", "AD keyword ID", "AD ID", "Business Channel ID", "Hours","Region code", "Media code", "PC Mobile Type",
                    "Conversion Method", "Conversion Type","Conversion count","Sales by conversion"]

    df_report = pd.read_csv(data, sep='\t', header=None, names=column_names)
    # print(df_report)

    df_report = df_report.merge(df_cam, on='Campaign ID', how='left')
    df_report = df_report.rename(columns={'name': 'Campaign Name'})
    df_report = df_report.fillna(0).replace([float('inf'), float('-inf')], 0)

    priority_map = {
        'purchase': 2,
        'add_to_cart': 1,
        0: 0,
        '0': 0
    }

    df_report_group = df_report.groupby(
        ['Campaign Name', 'Campaign ID', 'Conversion Type'],
        as_index=False
    )[['Conversion count', 'Sales by conversion']].sum()
    print(df_report_group)
    df_report_group['priority'] = (
        df_report_group['Conversion Type']
        .map(priority_map)
        .fillna(0)
    )

    df_report_group = df_report_group.loc[
        df_report_group.groupby('Campaign ID')['priority'].idxmax()
    ].reset_index(drop=True)

    df_report_group = df_report_group.drop(columns=['priority'])

    print(df_report_group)





    # 총비용,  구하기
    uri2 = '/stat-reports'
    method2 = 'POST'

    # reportTp를 'CONVERSION'으로 설정하여 전환 데이터가 포함된 리포트 요청
    params2 = {
        'reportTp': 'AD',   # 전환수 리포트
        'statDt': report_date     # 보고서 날짜
    }

    # 리포트 생성 요청
    r2 = requests.post(BASE_URL + uri2, json=params2, headers=get_header(method2, uri2, API_KEY, SECRET_KEY, CUSTOMER_ID))

    # 리포트 생성이 완료될 때까지 대기
    time.sleep(10)

    # 리포트 다운로드 URL 조회
    uri2 = '/stat-reports'
    method2 = 'GET'

    r2 = requests.get(BASE_URL + uri2, headers=get_header(method2, uri2, API_KEY, SECRET_KEY, CUSTOMER_ID))
    data2 = r2.json()
    data_2 = pd.DataFrame(data2)
    data_2 = data_2[
        (data_2["reportTp"] == "AD") &
        (data_2["status"] == "BUILT")
    ].copy()

    data_2 = data_2.sort_values(by="regTm", ascending=False).reset_index(drop=True)

    url2 = data_2["downloadUrl"].iloc[0]
    # 쿼리 파라미터 파싱
    parsed = dict(parse_qsl(urlsplit(url2).query, keep_blank_values=True))
    token2 = parsed.get('authtoken')
    file_version =  parsed.get('fileVersion') or qs.get('fileversion') or 'v2'

    # token2 = data_2['downloadUrl'][0].split('=')[1]

    # 리포트 다운로드
    uri2 = '/report-download'
    method2 = 'GET'

    r2 = requests.get(BASE_URL + uri2, params={'authtoken': token2, 'fileVersion': file_version}, headers=get_header(method2, uri2, API_KEY, SECRET_KEY, CUSTOMER_ID))
    data2 = StringIO(r2.text)

    column_names2 = ["Date", "Customer_ID", "Campaign ID","AD Group ID","AD keyword ID","AD ID","Business Channel ID",
                    "Media Code","PC Mobile Type","Impression","Click","Cost","Sum of Ad Rank","View Count"

    ]

    df_report2 = pd.read_csv(data2, sep='\t', header=None, names=column_names2)
    df_report2 = df_report2.merge(df_cam, on='Campaign ID', how='left')
    df_report2 = df_report2.rename(columns={'name': 'Campaign Name'})
    df_report2 = df_report2.fillna(0).replace([float('inf'), float('-inf')], 0)
    df_report2['Cost'] = df_report2['Cost'] 
    df_report_group2 = df_report2.groupby(['Campaign Name','Campaign ID'])[['Impression','Click','Cost']].sum().reset_index()
    # 'Campaign Name' 열을 문자열 형식으로 변환
    df_report_group['Campaign Name'] = df_report_group['Campaign Name'].astype(str)
    df_report_group2['Campaign Name'] = df_report_group2['Campaign Name'].astype(str)

    # 두 데이터프레임 병합
    df_combined = pd.merge(df_report_group, df_report_group2, on=['Campaign Name', 'Campaign ID'], how='outer')
    # CTR 및 CPC 계산
    df_combined['CTR (%)'] = (df_combined['Click'] / df_combined['Impression']) * 100
    df_combined['CPC'] = df_combined['Cost'] / df_combined['Click']
    df_combined['ROAS (%)'] = (df_combined['Sales by conversion'] / df_combined['Cost']) * 100
    # df_combined['CPCV'] = (df_combined['Cost'] / df_combined['Conversion count']) 
    df_combined['CVR'] = (df_combined['Conversion count']/ df_combined['Click']) * 100

    # 결측값을 0으로 채워줌
    df_combined = df_combined.fillna(0)


    # 데이터 형식 지정 및 포맷 적용
    df_combined['CTR (%)'] = df_combined['CTR (%)'].apply(lambda x: f"{round(x, 2):,.2f}".rstrip('0').rstrip('.') + "%")
    df_combined['ROAS (%)'] = df_combined['ROAS (%)'].apply(lambda x: f"{round(x, 2):,.2f}".rstrip('0').rstrip('.') + "%")
    df_combined['CPC'] = df_combined['CPC'].apply(lambda x: f"{round(x, 2):,.2f}".rstrip('0').rstrip('.'))
    df_combined['Cost'] = df_combined['Cost'].apply(lambda x: f"{x:,.2f}".rstrip('0').rstrip('.'))
    df_combined['Impression'] = df_combined['Impression'].apply(lambda x: f"{x:,}")
    df_combined['Click'] = df_combined['Click'].apply(lambda x: f"{x:,}")
    df_combined['Sales by conversion'] = df_combined['Sales by conversion'].apply(lambda x: f"{x:,.2f}".rstrip('0').rstrip('.'))
    df_combined['Conversion count'] = df_combined['Conversion count'].apply(lambda x: f"{x:,}")
    # df_combined['CPCV'] = df_combined['CPCV'].apply(lambda x: f"{round(x, 2):,.2f}".rstrip('0').rstrip('.'))
    df_combined['CVR'] = df_combined['CVR'].apply(lambda x: f"{round(x, 2):,.2f}".rstrip('0').rstrip('.') + "%")




    # Date 열을 추가하고 지정된 날짜 값을 채워넣기
    df_combined['Date'] = report_date
    df_combined.to_excel('C:/Users/User/Desktop/code/df_combined.xlsx', index=False, sheet_name="Ad Group Report")
    # 열 순서를 조정하여 Date 열이 첫 번째 열로 오게 함
    for col in ["CTR (%)", "ROAS (%)",'CVR', "CPC", "Cost", "Impression", "Click", "Conversion count", "Sales by conversion"]:
        df_combined[col] = df_combined[col].astype(str).str.replace(",", "").str.replace("%", "").replace("inf", float("inf")).astype(float)
    
    filtered_data = df_combined[df_combined["Campaign ID"].isin(campaign_ids)]
        # 순서정렬 
    filtered_data = filtered_data[['Date','Campaign ID', 'Campaign Name', 'Impression', 'Click','CTR (%)','CPC', 'Cost', 'Conversion count', 
                            'Sales by conversion', 'CVR', 'ROAS (%)']]
    filtered_data['Cost'] = brand_cost # 브랜드 금액 

    summable_columns = ["Cost", "Impression", "Click", "Conversion count", "Sales by conversion"]
    summed_values = filtered_data[summable_columns].sum()


    result = df_combined[['Date','Campaign ID', 'Campaign Name', 'Cost', 'Impression', 'Click','CPC','CTR (%)', 'Conversion count', 
                           'Sales by conversion','ROAS (%)']]
    result = pd.DataFrame(summed_values).T
    result['Cost'] = result['Cost'].replace(0, np.nan)
    result['Click'] = result['Click'].replace(0, np.nan)
    result['Impression'] = result['Impression'].replace(0, np.nan)
    result['Conversion count'] = result['Conversion count'].replace(0, np.nan)

    result['Date'] = report_date
    result['ROAS (%)'] = (result['Sales by conversion'] / result['Cost']) 
    result['CPC'] = (result['Cost'] / result['Click'] ) 
    result['CTR (%)'] = (result['Click'] / result['Impression']) 
    # result['CPCV'] = (result['Cost'] / result['Conversion count']) 
    result['CVR'] = (result['Conversion count']/ result['Click']) 

    # 필요한 컬럼들을 float로 변환 (문자열이 포함되어 있을 경우 대비)
    result['CVR'] = pd.to_numeric(result['CVR'], errors='coerce')
    result['CTR (%)'] = pd.to_numeric(result['CTR (%)'], errors='coerce')
    result['ROAS (%)'] = pd.to_numeric(result['ROAS (%)'], errors='coerce')
    result['CPC'] = pd.to_numeric(result['CPC'], errors='coerce')
    result['Cost'] = pd.to_numeric(result['Cost'], errors='coerce')
    result['Sales by conversion'] = pd.to_numeric(result['Sales by conversion'], errors='coerce')
    # result['CPCV'] = pd.to_numeric(result['CPCV'], errors='coerce')
    result['CTR (%)'] = (result['Click'] / result['Impression']) 

    # NaN 값이 있으면 0으로 변환
    result = result.fillna(0)
    result = result.round({
    # 'CTR (%)': 2,
    # 'ROAS (%)': 2,
        'CPC': 0,
        'Cost': 2,
        'Sales by conversion': 2
    })
   

    # 열 순서 재정렬
    result = result[['Date', 'Cost','Impression', 'Click',   'CPC', 'CTR (%)', 'Conversion count' ,  'CVR', 'Sales by conversion', 'ROAS (%)']]
    
    return result
    









# campaign_ids= [
#     "cmp-a001-02-000000006258220",
#     "cmp-a001-02-000000006258641",
#     "cmp-a001-02-000000006259085",
#     "cmp-a001-02-000000006259509",
#     "cmp-a001-02-000000006259577",
#     "cmp-a001-02-000000006259680",
#     "cmp-a001-02-000000006262004",
#     "cmp-a001-02-000000006262005",
#     "cmp-a001-02-000000006262006",
#     "cmp-a001-02-000000006262007",
#     "cmp-a001-02-000000006262008",
#     "cmp-a001-02-000000006262009",
#     "cmp-a001-02-000000006262059",
#     "cmp-a001-02-000000006262063",
#     "cmp-a001-02-000000006262065",
#     "cmp-a001-02-000000006262068",
#     "cmp-a001-02-000000006262074",
#     "cmp-a001-02-000000006262077",
#     "cmp-a001-02-000000006262081",
#     "cmp-a001-02-000000006262082",
#     "cmp-a001-02-000000006262084",
#     "cmp-a001-02-000000006262087",
#     "cmp-a001-02-000000006262091",
#     "cmp-a001-02-000000006262095",
#     "cmp-a001-02-000000006262097",
#     "cmp-a001-02-000000006262098",
#     "cmp-a001-02-000000006262100",
#     "cmp-a001-02-000000006262118",
#     "cmp-a001-02-000000006262121",
#     "cmp-a001-02-000000006262124",
#     "cmp-a001-02-000000006262127",
#     "cmp-a001-02-000000006276329",
#     "cmp-a001-02-000000006276334",
#     "cmp-a001-02-000000006276495",
#     "cmp-a001-02-000000006280273",
#     "cmp-a001-02-000000006519246",
#     "cmp-a001-02-000000006562698",
#     "cmp-a001-02-000000006577793",
#     "cmp-a001-02-000000006618236",
#     "cmp-a001-02-000000006654836",
#     "cmp-a001-02-000000006723213",
#     "cmp-a001-02-000000006814453",
#     "cmp-a001-02-000000007865985",
#     "cmp-a001-02-000000007876242",
#     "cmp-a001-02-000000008235932",
#     "cmp-a001-02-000000008334292",
#     "cmp-a001-02-000000008417712",
#     "cmp-a001-02-000000008417718",
#     "cmp-a001-02-000000008456093",
#     "cmp-a001-02-000000008556328",
#     "cmp-a001-02-000000008556954",
#     "cmp-a001-02-000000009019602",
#     "cmp-a001-02-000000009254038",
#     "cmp-a001-02-000000009968461",
#     "cmp-a001-01-000000002806600",
#     "cmp-a001-01-000000006258542",
#     "cmp-a001-01-000000006258545"
# ]


# run_naver_report(report_date = "20260424", api_key ='01000000000d4de559bd8515cd989bd4ae76535359afb03c701256acf023d9ae1619091fc9',
# secret_key='AQAAAAANTeVZvYUVzZib1K52U1NZmqVA5PBGsAxIMJMEwM3yMQ==', customer_id=	'1394697', campaign_ids=campaign_ids)
