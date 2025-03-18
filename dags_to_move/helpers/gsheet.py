# -*- coding: utf-8 -*-
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from oauth2client.service_account import ServiceAccountCredentials

import base64
import gspread
import json
import logging
import os
import pandas as pd
import pytz


def write_variable_to_local_file(variable_name, local_file_path):
    content = Variable.get(variable_name)
    f = open(local_file_path, "w")
    f.write(content)
    f.close()


def get_gsheet_client(sheet_api_credential_key="google_sheet_access_token"):
    data_dir = Variable.get("data_dir")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    gs_json_file_path = data_dir + 'google-sheet.json'

    write_variable_to_local_file(sheet_api_credential_key, gs_json_file_path)
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gs_json_file_path, scope)
    gc = gspread.authorize(credentials)

    return gc


def p2f(x):
    return float(x.strip('%'))/100


def get_google_sheet_to_csv(
    sheet_uri,
    tab,
    filename,
    sheet_api_credential_key,
    header_line=1,
    remove_dollar_comma=0,
    rate_to_float=0
):
    """
    스프레드시트(sheet_uri)의 특정 탭(“tab”)에 있는 데이터를 CSV 파일(filename)로 다운로드.
    - tab이 None이면, 시트의 첫 번째 탭에 있는 데이터를 다운로드
    - tab의 헤더 행이 한 줄뿐이라면 기본값인 1을 사용
    - remove_dollar_comma 값을 1로 설정하면 CSV 파일의 값에서 달러 기호($)나 쉼표(,)를 제거
    - 여기서 달러 기호는 원화(₩) 기호로 변경가능
    - rate_to_float 값을 1로 설정하면 퍼센트(%) 형태의 숫자 값을 소수로 변환 (예: 50% → 0.5)
    """

    data, header = get_google_sheet_to_lists(
        sheet_api_credential_key,
        sheet_uri,
        tab,
        header_line,
        remove_dollar_comma=remove_dollar_comma)

    if rate_to_float:
        for row in data:
            for i in range(len(row)):
                if str(row[i]).endswith("%"):
                    row[i] = p2f(row[i])

    data = pd.DataFrame(data, columns=header).to_csv(
        filename,
        index=False,
        header=True,
        encoding='utf-8'
    )


def get_google_sheet_to_lists(
    sheet_api_credential_key,
    sheet_uri,
    tab_name=None,
    header_line=1,
    remove_dollar_comma=0
):
    """
    sheet_uri와tab_name으로 지정된 시트의 내용을 list로 리턴해줌
    - sheet_api_credential_key: Sheet API 사용을 허용한 JSON credential이 들어있는 Airflow Variable 이름
    - sheet_uri: 구글시트의 URL
    - tab_name: 읽기 대상 sheet의 이름. 지정되지 않으면 첫번째 sheet를 대상으로함
    """
    gc = get_gsheet_client(sheet_api_credential_key)

    # tab이 주어져있지 않다면 첫번째 시트를 사용
    if tab_name is None:
        wks = gc.open_by_url(sheet_uri).sheet1
    else:
        wks = gc.open_by_url(sheet_uri).worksheet(tab_name)

    # list of lists, first value of each list is column header
    data = wks.get_all_values()[header_line-1:]

    header = data[0]
    if remove_dollar_comma:
        data = [replace_dollar_comma(l) for l in data if l != header]
    else:
        data = [l for l in data if l != header]

    return data, header


def add_df_to_sheet_in_bulk(sh, sheet, df, header=None, clear=False):
    records = []
    headers = list(df.columns)
    records.append(headers)

    for _, row in df.iterrows():
        record = []
        for column in headers:
            if str(df.dtypes[column]) in ('object', 'datetime64[ns]', 'int64'):
                record.append(str(row[column]))
            else:
                record.append(row[column])
        records.append(record)

    if clear:
        sh.worksheet(sheet).clear()

    sh.values_update(
        '{sheet}!A1'.format(sheet=sheet),
        params={'valueInputOption': 'RAW'},
        body={'values': records}
    )


def update_sheet(sheet_api_credential_key, spreadsheet_name, tab_name, sql, conn_id):
    """
    conn_id가 Snowflake DB에 sql을 실행한 후 그 결과를
    spreadsheet_name이 가르키는 구글시트에서 sheetname이라는 시트로 덤프
    - sheet_api_credential_key: Sheet API 사용을 허용한 JSON credential이 들어있는Airflow Variable 이름
    - spreadsheet_name: 대상 구글시트 파일 이름
    - tab_name: 대상 시트(tab)의 이름
    """
    client = get_gsheet_client(sheet_api_credential_key)
    sh = client.open(spreadsheet_name)

    # conn_id가 가리키는 Snowflake에 sql을 실행해서 그 결과를 df라는 데이터프레임으로 저장
    hook = SnowflakeHook(conn_id)
    df = hook.get_pandas_df(sql)

    # df의 내용을 해당 구글시트로 복사하기 전에 내용을 먼저 클리어
    sh.worksheet(tab_name).clear()
    # 이제 df의 내용을 복사하는 내용이 없는 셀은 ''으로 바꾸어서 복사
    add_df_to_sheet_in_bulk(sh, tab_name, df.fillna(''))


def replace_dollar_comma(lll):
    return [ ll.replace(',', '').replace('$', '') for ll in lll ]
