# streamlit_app.py (Version 2: 금현물 그래프 추가)

import streamlit as st
import pandas as pd
import numpy as np
import json
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import traceback
import yfinance as yf
from collections.abc import Mapping # Secrets 타입 체크 위해 추가
import re # 숫자 처리 위해 추가

# --- 기본 설정 ---
PAGE_TITLE = "포트폴리오 대시보드"
PAGE_ICON = "📊"
st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout="wide")

# --- 경로 설정 ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
TWR_CSV_PATH = os.path.join(CURRENT_DIR, 'twr_results.csv')
GAIN_LOSS_JSON_PATH = os.path.join(CURRENT_DIR, 'gain_loss.json')
GOOGLE_SHEET_NAME = 'KYI_자산배분'
BALANCE_RAW_SHEET = '일별잔고_Raw'
WEIGHTS_RAW_SHEET = '일별비중_Raw'
SETTINGS_SHEET = '⚙️설정'
TRADES_SHEET = '🗓️매매일지'
GOLD_RATE_SHEET = '📈금현물 수익률' # 금현물 시트 이름 정의

# --- 지수 티커 설정 ---
KOSPI_TICKER = "^KS200"
SP500_TICKER = "^GSPC"
# --- ---

# --- 유틸리티 함수 ---
def clean_numeric_value(value, type_func=int):
    """단일 값을 숫자로 변환 (쉼표 및 타입 처리 개선)"""
    if isinstance(value, (int, float)):
        # 이미 숫자 타입이면 원하는 타입으로 변환 시도
        try: return type_func(value)
        except (ValueError, TypeError): return type_func(0) # 변환 실패 시 0 반환
    if not value: return type_func(0)
    try:
        # 숫자 및 소수점, 마이너스 부호 관련 문자 외 제거 (정규식 사용)
        # (주의: 과학적 표기법 'e' 등은 처리 못함)
        cleaned_str = re.sub(r'[^\d.-]+', '', str(value))
        if not cleaned_str or cleaned_str in ['-', '.']: return type_func(0)
        # float으로 먼저 변환 후 최종 타입으로 변환
        num_val = float(cleaned_str)
        return type_func(num_val)
    except (ValueError, TypeError):
        return type_func(0)
# --- ---

# --- 데이터 로딩 함수들 --- (기존 함수들은 동일)
@st.cache_data(ttl=600)
def load_twr_data():
    """TWR 결과 CSV 파일을 로드합니다."""
    try:
        df = pd.read_csv(TWR_CSV_PATH, parse_dates=['Date'])
        print(f"Log: TWR 데이터 로드 완료 ({TWR_CSV_PATH})")
        return df
    except FileNotFoundError: st.warning(f"TWR 결과 파일({TWR_CSV_PATH})을 찾을 수 없습니다. `portfolio_performance.py`를 먼저 실행하세요."); return pd.DataFrame()
    except Exception as e: st.error(f"TWR 데이터 로딩 중 오류 발생: {e}"); return pd.DataFrame()

@st.cache_data(ttl=600)
def load_gain_loss_data():
    """단순 손익 결과 JSON 파일을 로드합니다."""
    try:
        with open(GAIN_LOSS_JSON_PATH, 'r', encoding='utf-8') as f: data = json.load(f)
        print(f"Log: 단순 손익 데이터 로드 완료 ({GAIN_LOSS_JSON_PATH})")
        cleaned_data = {};
        for k, v in data.items(): cleaned_data[k] = None if isinstance(v, (int, float)) and (np.isnan(v) or np.isinf(v)) else v
        return cleaned_data
    except FileNotFoundError: st.warning(f"단순 손익 결과 파일({GAIN_LOSS_JSON_PATH})을 찾을 수 없습니다. `portfolio_performance.py`를 먼저 실행하세요."); return {}
    except Exception as e: st.error(f"단순 손익 데이터 로딩 중 오류 발생: {e}"); return {}

@st.cache_resource(ttl=600)
def connect_google_sheets():
    """구글 시트 API에 연결하고 클라이언트 객체를 반환합니다."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        if "gcs_credentials" not in st.secrets: st.error("Streamlit Secrets에 'gcs_credentials'가 설정되지 않았습니다..."); return None
        creds_value = st.secrets["gcs_credentials"]; creds_dict = None
        if isinstance(creds_value, Mapping): print("Log: Reading secrets as dictionary-like object."); creds_dict = dict(creds_value)
        elif isinstance(creds_value, str):
            print("Log: Reading secrets as string, attempting JSON parse.")
            try: creds_dict = json.loads(creds_value)
            except json.JSONDecodeError:
                try: escaped_string = creds_value.replace("\n", "\\n"); creds_dict = json.loads(escaped_string); print("Log: JSON parsing successful after escaping newlines.")
                except json.JSONDecodeError as e_escaped: st.error(f"Secrets의 'gcs_credentials' 값 JSON 파싱 오류: {e_escaped}..."); return None
        else: st.error(f"Secrets의 'gcs_credentials' 값 타입 오류..."); return None
        if creds_dict:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope); gc = gspread.authorize(creds)
            gc.list_spreadsheet_files(); print("Log: Google Sheets 연결 성공"); return gc
        else: st.error("인증 정보(creds_dict)를 준비하지 못했습니다."); return None
    except KeyError as e: st.error(f"Streamlit Secrets 접근 오류: 키 '{e}' 없음..."); return None
    except Exception as e: st.error(f"구글 시트 연결 실패 (Secrets 사용 중): {e}"); traceback.print_exc(); return None

@st.cache_data(ttl=600)
def load_latest_balances(_gc):
    """'일별잔고_Raw' 시트에서 가장 최근 날짜의 계좌별 총자산을 로드합니다."""
    if not isinstance(_gc, gspread.Client): st.error("load_latest_balances: 유효한 Google Sheets 클라이언트 객체(gc)가 아닙니다."); return {}, None
    try:
        spreadsheet = _gc.open(GOOGLE_SHEET_NAME); worksheet = spreadsheet.worksheet(BALANCE_RAW_SHEET)
        data = worksheet.get_all_records(); latest_date = None # 초기화
        if not data: st.warning(f"'{BALANCE_RAW_SHEET}' 시트 데이터 없음."); return {}, None
        df = pd.DataFrame(data); df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
        valid_dates = df.dropna(subset=['날짜'])
        if valid_dates.empty: st.warning(f"'{BALANCE_RAW_SHEET}' 유효 날짜 데이터 없음."); return {}, None
        latest_date = valid_dates['날짜'].max() # 날짜 계산 후 할당
        latest_df = df[df['날짜'] == latest_date].copy()
        if '총자산' not in latest_df.columns: st.error(f"'{BALANCE_RAW_SHEET}' 시트에 '총자산' 컬럼 없음."); return {}, latest_date
        latest_df['총자산_num'] = pd.to_numeric(latest_df['총자산'].astype(str).str.replace(',','', regex=False), errors='coerce')
        balances = latest_df.dropna(subset=['총자산_num']).set_index('계좌명')['총자산_num'].to_dict()
        print(f"Log: 최신 잔고 데이터 로드 완료 (날짜: {latest_date.strftime('%Y-%m-%d')})")
        return balances, latest_date
    except gspread.exceptions.WorksheetNotFound: st.error(f"워크시트 '{BALANCE_RAW_SHEET}'를 찾을 수 없음."); return {}, None
    except Exception as e: st.error(f"'일별잔고_Raw' 로딩 중 오류: {e}"); traceback.print_exc(); return {}, None

@st.cache_data(ttl=600)
def load_allocation_data(_gc, latest_data_date):
    """자산 배분 데이터('⚙️설정', '일별비중_Raw')를 로드하고 비교 테이블 생성"""
    # (이전 버전과 동일)
    if not isinstance(_gc, gspread.Client) or not isinstance(latest_data_date, pd.Timestamp): st.error("load_allocation_data: 유효한 gc 또는 latest_data_date 아님."); return pd.DataFrame(), pd.DataFrame()
    settings_df = pd.DataFrame(); target_allocation_map = {}; comparison_df_final = pd.DataFrame(); current_weights_df = pd.DataFrame()
    BASE_TOTAL_ASSET = 80000000
    try:
        spreadsheet = _gc.open(GOOGLE_SHEET_NAME); settings_ws = spreadsheet.worksheet(SETTINGS_SHEET); settings_values = settings_ws.get_all_values()
        if len(settings_values) > 1:
            header = settings_values[0]
            try:
                required_cols = ['목표구분', '목표국적', '목표비중']; col_indices = {}; missing_cols = []
                for col in required_cols:
                    try: col_indices[col] = header.index(col)
                    except ValueError: missing_cols.append(col)
                if missing_cols: raise ValueError(f"설정 시트 헤더 오류: {missing_cols} 누락")
                target_class_col, target_nation_col, target_perc_col = col_indices['목표구분'], col_indices['목표국적'], col_indices['목표비중']
                processed_targets_combined = {}; unique_target_keys = set()
                for i, row in enumerate(settings_values[1:]):
                    if len(row) > max(target_class_col, target_nation_col, target_perc_col):
                        try:
                            asset_class = str(row[target_class_col]).strip(); nationality = str(row[target_nation_col]).strip(); target_perc_str = str(row[target_perc_col]).strip().replace('%','')
                            combined_key = (asset_class, nationality)
                            if asset_class and nationality and target_perc_str:
                                if combined_key not in unique_target_keys:
                                     try:
                                         target_perc = float(target_perc_str)
                                         if target_perc > 0:
                                             combined_name = f"{nationality} {asset_class}" if asset_class != '대체투자' else "금"
                                             processed_targets_combined[combined_name] = target_perc; unique_target_keys.add(combined_key)
                                     except ValueError: pass
                        except Exception as e_row: print(f"Log: 설정 {i+2}행 목표 처리 오류: {e_row}")
                target_allocation_map = processed_targets_combined
                if target_allocation_map: target_df = pd.DataFrame(list(target_allocation_map.items()), columns=['종합 분류', '목표 비중(%)']); settings_df = target_df[target_df['목표 비중(%)'] > 0].sort_values(by='목표 비중(%)', ascending=False); print(f"Log: 목표 비중 로드 완료: {len(settings_df)}개 항목")
                else: print("Log: 목표 비중 정보 없음.")
            except ValueError as e_col: st.error(f"설정 시트 처리 중 값 오류: {e_col}"); traceback.print_exc()
            except Exception as e_set: st.error(f"설정 시트 처리 중 예상치 못한 오류: {e_set}"); traceback.print_exc()
        else: print("Log: 설정 시트 데이터 없음.")

        weights_ws = spreadsheet.worksheet(WEIGHTS_RAW_SHEET); weights_data = weights_ws.get_all_records()
        if not weights_data:
            st.warning("'일별비중_Raw' 시트 데이터 없음."); comparison_df_final = pd.DataFrame(columns=['종합 분류', '현재 비중(%)', '현재 평가액', '목표 비중(%)', '목표 금액', '차이(%)', '현금차이'])
            if not settings_df.empty: comparison_df_final = settings_df.rename(columns={'목표 비중(%)':'목표 비중(%)'}); comparison_df_final['현재 비중(%)'] = 0.0; comparison_df_final['현재 평가액'] = 0; comparison_df_final['목표 금액'] = (BASE_TOTAL_ASSET * (comparison_df_final['목표 비중(%)'] / 100)).round(0).astype(int); comparison_df_final['차이(%)'] = -comparison_df_final['목표 비중(%)']; comparison_df_final['현금차이'] = -comparison_df_final['목표 금액']
            return comparison_df_final.round({'차이(%)': 2}), settings_df

        weights_df = pd.DataFrame(weights_data); weights_df['날짜'] = pd.to_datetime(weights_df['날짜'], errors='coerce')
        latest_weights_df = weights_df[weights_df['날짜'] == latest_data_date].copy()
        if latest_weights_df.empty:
             st.warning(f"{latest_data_date.strftime('%Y-%m-%d')} 날짜의 비중 데이터 없음."); comparison_df_final = pd.DataFrame(columns=['종합 분류', '현재 비중(%)', '현재 평가액', '목표 비중(%)', '목표 금액', '차이(%)', '현금차이'])
             if not settings_df.empty: comparison_df_final = settings_df.rename(columns={'목표 비중(%)':'목표 비중(%)'}); comparison_df_final['현재 비중(%)'] = 0.0; comparison_df_final['현재 평가액'] = 0; comparison_df_final['목표 금액'] = (BASE_TOTAL_ASSET * (comparison_df_final['목표 비중(%)'] / 100)).round(0).astype(int); comparison_df_final['차이(%)'] = -comparison_df_final['목표 비중(%)']; comparison_df_final['현금차이'] = -comparison_df_final['목표 금액']
             return comparison_df_final.round({'차이(%)': 2}), settings_df

        required_weight_cols = ['자산구분', '포트폴리오내비중(%)', '평가금액']; missing_weight_cols = [col for col in required_weight_cols if col not in latest_weights_df.columns]; has_nationality_col = '국적' in latest_weights_df.columns
        if not has_nationality_col: st.warning("'일별비중_Raw' 시트에 '국적' 컬럼 없음.")
        if missing_weight_cols: st.error(f"'{WEIGHTS_RAW_SHEET}' 시트에 필수 컬럼 누락: {missing_weight_cols}"); return pd.DataFrame(), settings_df

        def get_combined_name(row):
            asset_class = str(row.get('자산구분', '')).strip(); nationality = str(row.get('국적', '')).strip() if has_nationality_col else ""
            if not asset_class: return '미분류'
            if asset_class == '대체투자': return "금"
            elif not nationality: return asset_class
            else: return f"{nationality} {asset_class}"

        latest_weights_df['종합 분류'] = latest_weights_df.apply(get_combined_name, axis=1)
        latest_weights_df['현재 비중(%)'] = pd.to_numeric(latest_weights_df['포트폴리오내비중(%)'], errors='coerce').fillna(0.0)
        latest_weights_df['현재 평가액'] = pd.to_numeric(latest_weights_df['평가금액'].astype(str).str.replace(',','', regex=False), errors='coerce').fillna(0).astype(int)
        current_weights_grouped = latest_weights_df.groupby('종합 분류').agg({'현재 비중(%)': 'sum', '현재 평가액': 'sum'}).reset_index()
        current_weights_df = current_weights_grouped[current_weights_grouped['현재 비중(%)'] > 0].sort_values(by='현재 비중(%)', ascending=False)
        print(f"Log: 현재 비중 및 평가액 계산 완료: {len(current_weights_df)}개 항목")

        if not current_weights_df.empty:
            if not settings_df.empty: comparison_df = current_weights_df.merge(settings_df.set_index('종합 분류'), on='종합 분류', how='outer').fillna(0)
            else: comparison_df = current_weights_df.copy(); comparison_df['목표 비중(%)'] = 0.0
            for col in ['현재 비중(%)', '현재 평가액', '목표 비중(%)']:
                if col not in comparison_df.columns: comparison_df[col] = 0.0 if '%' in col else 0
            comparison_df['차이(%)'] = comparison_df['현재 비중(%)'] - comparison_df['목표 비중(%)']
            comparison_df['목표 금액'] = BASE_TOTAL_ASSET * (comparison_df['목표 비중(%)'] / 100)
            comparison_df['현금차이'] = comparison_df['현재 평가액'] - comparison_df['목표 금액']
            for col in ['현재 평가액', '목표 금액', '현금차이']:
                 if col in comparison_df.columns: comparison_df[col] = comparison_df[col].round(0).astype(int)
            comparison_df_final = comparison_df
            print("Log: 현재/목표 비중 및 금액 비교 테이블 생성 완료")
        else:
             print("Log: 현재 비중 정보 없음.")
             if not settings_df.empty: comparison_df_final = settings_df.rename(columns={'목표 비중(%)':'목표 비중(%)'}); comparison_df_final['현재 비중(%)'] = 0.0; comparison_df_final['현재 평가액'] = 0; comparison_df_final['목표 금액'] = (BASE_TOTAL_ASSET * (comparison_df_final['목표 비중(%)'] / 100)).round(0).astype(int); comparison_df_final['차이(%)'] = -comparison_df_final['목표 비중(%)']; comparison_df_final['현금차이'] = -comparison_df_final['목표 금액']
             else: comparison_df_final = pd.DataFrame(columns=['종합 분류', '현재 비중(%)', '현재 평가액', '목표 비중(%)', '목표 금액', '차이(%)', '현금차이'])

    except gspread.exceptions.WorksheetNotFound as e_ws: st.error(f"워크시트 '{e_ws.args[0] if e_ws.args else ''}'를 찾을 수 없음.")
    except Exception as e: st.error(f"자산 배분 데이터 로딩/처리 중 오류: {e}"); traceback.print_exc()

    final_cols_order = ['종합 분류', '현재 비중(%)', '현재 평가액', '목표 비중(%)', '목표 금액', '차이(%)', '현금차이']
    available_final_cols = [col for col in final_cols_order if col in comparison_df_final.columns]
    return comparison_df_final[available_final_cols], settings_df

@st.cache_data(ttl=3600)
def download_yf_data(ticker, start_date, end_date):
    """Yahoo Finance 데이터 다운로드 (기존과 동일)"""
    try:
        end_date_adj = pd.to_datetime(end_date) + timedelta(days=1)
        data = yf.download(ticker, start=start_date, end=end_date_adj, progress=False, auto_adjust=True) # auto_adjust=True 추가
        if data.empty: st.warning(f"⚠️ {ticker} 데이터 다운로드 실패."); return pd.DataFrame()
        if isinstance(data.index, pd.DatetimeIndex): data.index = data.index.tz_localize(None) # 시간대 정보 제거
        return data
    except Exception as e: st.error(f"{ticker} 데이터 다운로드 중 오류: {e}"); return pd.DataFrame()

@st.cache_data
def calculate_index_twr(index_df, ticker):
    """주가 데이터프레임으로 TWR(%) 계산 (기존과 동일, 입력 DataFrame 처리 강화)"""
    if index_df is None or index_df.empty or len(index_df) < 2: return pd.DataFrame()
    # 'Close' 컬럼 찾기 (MultiIndex 또는 일반 Index 고려)
    close_col_name = None
    if isinstance(index_df.columns, pd.MultiIndex):
        # yfinance가 가끔 ('Adj Close', '') 같은 식으로 반환할 때 대비
        potential_cols = [('Close', ticker), ('Adj Close', ticker), ('Close', ''), ('Adj Close', '')]
        for col in potential_cols:
            if col in index_df.columns: close_col_name = col; break
        if not close_col_name: # 그래도 못 찾으면 'Close'나 'Adj Close' 레벨에서 찾기
             level_zero = index_df.columns.get_level_values(0)
             if 'Close' in level_zero: close_col_name = [c for c in index_df.columns if c[0] == 'Close'][0]
             elif 'Adj Close' in level_zero: close_col_name = [c for c in index_df.columns if c[0] == 'Adj Close'][0]
    elif 'Close' in index_df.columns: close_col_name = 'Close'
    elif 'Adj Close' in index_df.columns: close_col_name = 'Adj Close'

    if close_col_name is None: st.warning(f"{ticker} TWR 계산 불가: 종가('Close') 컬럼 못 찾음."); return pd.DataFrame()

    df = index_df[[close_col_name]].copy(); df.columns = ['Close'] # 컬럼 이름 통일
    df = df.dropna().astype('float64')
    if not pd.api.types.is_float_dtype(df['Close']) or df.empty: return pd.DataFrame()

    df = df.sort_index(); df['StartValue'] = df['Close'].shift(1); df = df.iloc[1:].copy()
    if df.empty: return pd.DataFrame()
    denominator = df['StartValue']
    df['DailyFactor'] = 1.0
    valid_calc_mask = (denominator.abs() > 1e-9) # 0으로 나누는 것 방지
    df.loc[valid_calc_mask, 'DailyFactor'] = (df.loc[valid_calc_mask, 'Close'] / denominator.loc[valid_calc_mask])
    df['DailyFactor'] = df['DailyFactor'].replace([np.inf, -np.inf], np.nan).fillna(1.0)
    df['DailyFactor'] = df['DailyFactor'].clip(lower=0.1, upper=10.0) # 이상치 제한
    df['CumulativeFactor'] = df['DailyFactor'].cumprod()
    df['TWR'] = (df['CumulativeFactor'] - 1.0) * 100.0
    return df[['TWR']].reset_index()

@st.cache_data(ttl=600)
def load_current_holdings(_gc, latest_data_date):
    """'일별비중_Raw' 시트에서 현재 보유 종목 목록 로드 (기존과 동일)"""
    if not isinstance(_gc, gspread.Client) or not isinstance(latest_data_date, pd.Timestamp): st.error("load_current_holdings: 유효한 gc 또는 latest_data_date 아님."); return pd.DataFrame(columns=['종목코드', '종목명'])
    try:
        spreadsheet = _gc.open(GOOGLE_SHEET_NAME); weights_ws = spreadsheet.worksheet(WEIGHTS_RAW_SHEET)
        weights_data = weights_ws.get_all_records(); holdings_df = pd.DataFrame(columns=['종목코드', '종목명']) # 기본값
        if not weights_data: st.warning(f"'{WEIGHTS_RAW_SHEET}' 시트 데이터 없음."); return holdings_df
        weights_df = pd.DataFrame(weights_data); weights_df['날짜'] = pd.to_datetime(weights_df['날짜'], errors='coerce')
        latest_weights_df = weights_df[weights_df['날짜'] == latest_data_date].copy()
        if latest_weights_df.empty: st.warning(f"{latest_data_date.strftime('%Y-%m-%d')} 날짜의 비중 데이터 없음."); return holdings_df
        required_cols = ['종목코드', '종목명', '평가금액']; missing_cols = [col for col in required_cols if col not in latest_weights_df.columns]
        if missing_cols: st.error(f"'{WEIGHTS_RAW_SHEET}' 필수 컬럼 누락: {missing_cols}."); return holdings_df
        latest_weights_df['평가금액_num'] = pd.to_numeric(latest_weights_df['평가금액'].astype(str).str.replace(',','', regex=False), errors='coerce').fillna(0).astype(int)
        latest_weights_df['종목명_정리'] = latest_weights_df['종목명'].astype(str).str.replace(' ', '')
        holdings_df = latest_weights_df[latest_weights_df['평가금액_num'] > 0][['종목코드', '종목명_정리']].rename(columns={'종목명_정리':'종목명'}).drop_duplicates().sort_values(by='종목명').reset_index(drop=True)
        gold_mask = (holdings_df['종목명'] == '금현물') | (holdings_df['종목명'] == '금'); code_missing_mask = holdings_df['종목코드'].isnull() | (holdings_df['종목코드'].astype(str).str.strip() == ''); rows_to_update = gold_mask & code_missing_mask
        if rows_to_update.any(): holdings_df.loc[rows_to_update, '종목코드'] = 'GOLD'; print(f"Log: '{holdings_df.loc[rows_to_update, '종목명'].iloc[0]}' 항목에 'GOLD' 코드 할당됨.")
        print(f"Log: 현재 보유 종목 목록 로드 완료 ({len(holdings_df)} 종목)")
        return holdings_df
    except gspread.exceptions.WorksheetNotFound: st.error(f"워크시트 '{WEIGHTS_RAW_SHEET}'를 찾을 수 없음."); return pd.DataFrame(columns=['종목코드', '종목명'])
    except Exception as e: st.error(f"보유 종목 목록 로딩 중 오류: {e}"); traceback.print_exc(); return pd.DataFrame(columns=['종목코드', '종목명'])

@st.cache_data(ttl=300)
def calculate_moving_avg_cost(_gc, stock_code):
    """'🗓️매매일지' 시트에서 이동평균법으로 평단가 계산 (기존과 동일, 숫자 변환 함수 사용)"""
    if not isinstance(_gc, gspread.Client): st.error("calculate_moving_avg_cost: 유효한 Google Sheets 클라이언트 객체(gc)가 아닙니다."); return 0.0
    if not stock_code: return 0.0
    final_avg_cost = 0.0
    TRADE_DATE_HEADER = '날짜'; TRADE_TYPE_HEADER = '매매구분'; TRADE_PRICE_HEADER = '단가'; TRADE_QTY_HEADER = '수량'; TRADE_CODE_HEADER = '종목코드'
    try:
        spreadsheet = _gc.open(GOOGLE_SHEET_NAME); trades_ws = spreadsheet.worksheet(TRADES_SHEET)
        all_trades_records = trades_ws.get_all_records()
        if not all_trades_records: return final_avg_cost
        trades_df = pd.DataFrame(all_trades_records)
        required_trade_headers = [TRADE_DATE_HEADER, TRADE_TYPE_HEADER, TRADE_PRICE_HEADER, TRADE_QTY_HEADER, TRADE_CODE_HEADER]
        missing_trade_headers = [h for h in required_trade_headers if h not in trades_df.columns]
        if missing_trade_headers: st.error(f"'{TRADES_SHEET}' 필수 헤더 누락: {missing_trade_headers}"); return final_avg_cost
        trades_df['Date'] = pd.to_datetime(trades_df[TRADE_DATE_HEADER], errors='coerce')
        trades_df = trades_df.dropna(subset=['Date']).sort_values(by='Date')
        stock_code_str = str(stock_code).strip().upper().replace('KRX:', '').replace('A','')
        is_gold = (stock_code_str == 'GOLD')
        def code_match(row_code):
            row_code_str = str(row_code).strip().upper().replace('KRX:', '').replace('A','')
            if is_gold: return row_code_str == 'GOLD'
            else: return row_code_str == stock_code_str
        filtered_trades_df = trades_df[trades_df[TRADE_CODE_HEADER].apply(code_match)]
        if filtered_trades_df.empty: return final_avg_cost
        current_qty = 0.0; total_cost = 0.0 # 수량, 비용 float으로 처리
        for index, row in filtered_trades_df.iterrows():
            row_type = str(row[TRADE_TYPE_HEADER]).strip()
            try:
                # clean_numeric_value 사용하여 숫자 변환
                qty = clean_numeric_value(row[TRADE_QTY_HEADER], float) # float으로 수량 처리
                price = clean_numeric_value(row[TRADE_PRICE_HEADER], float)
                if row_type == '매수':
                    if qty > 0 and price >= 0: cost_of_buy = qty * price; total_cost += cost_of_buy; current_qty += qty
                elif row_type == '매도':
                    if qty > 0 and current_qty > 1e-9: # 0에 가까운지 비교
                        sell_qty = min(qty, current_qty); avg_cost_before_sell = total_cost / current_qty
                        cost_of_sold = sell_qty * avg_cost_before_sell; total_cost -= cost_of_sold; current_qty -= sell_qty
                        if abs(current_qty) < 1e-9: total_cost = 0.0 # 0에 가까우면 비용 초기화
            except Exception as e_row: print(f"Log: Row {index} 처리 중 오류 (이동평균): {e_row}"); continue
        if current_qty > 1e-9: final_avg_cost = total_cost / current_qty
        else: final_avg_cost = 0.0
        print(f"Log: {stock_code} 최종 평단가(이동평균): {final_avg_cost:.2f}")
    except gspread.exceptions.WorksheetNotFound: st.error(f"워크시트 '{TRADES_SHEET}'를 찾을 수 없음.")
    except KeyError as e: st.error(f"'{TRADES_SHEET}' 시트 처리 오류: 컬럼 '{e}' 확인 필요.")
    except Exception as e: st.error(f"평단가(이동평균) 계산 중 오류: {e}"); traceback.print_exc()
    # 금 가격은 소수점 필요할 수 있으므로 float 반환
    return float(final_avg_cost)

@st.cache_data(ttl=3600)
def get_first_purchase_date(_gc, stock_code):
    """'🗓️매매일지' 시트에서 최초 매수일 찾기 (기존과 동일)"""
    if not isinstance(_gc, gspread.Client): st.error("get_first_purchase_date: 유효한 Google Sheets 클라이언트 객체 아님."); return None
    if not stock_code: return None
    first_date = None
    TRADE_DATE_HEADER = '날짜'; TRADE_TYPE_HEADER = '매매구분'; TRADE_CODE_HEADER = '종목코드'
    try:
        spreadsheet = _gc.open(GOOGLE_SHEET_NAME); trades_ws = spreadsheet.worksheet(TRADES_SHEET)
        all_trades_records = trades_ws.get_all_records()
        if not all_trades_records: return None
        trades_df = pd.DataFrame(all_trades_records)
        required_trade_headers = [TRADE_DATE_HEADER, TRADE_TYPE_HEADER, TRADE_CODE_HEADER]
        missing_trade_headers = [h for h in required_trade_headers if h not in trades_df.columns]
        if missing_trade_headers: st.error(f"'{TRADES_SHEET}' 필수 헤더 누락: {missing_trade_headers}"); return None
        trades_df['Date'] = pd.to_datetime(trades_df[TRADE_DATE_HEADER], errors='coerce')
        trades_df = trades_df.dropna(subset=['Date'])
        stock_code_str = str(stock_code).strip().upper().replace('KRX:', '').replace('A','')
        is_gold = (stock_code_str == 'GOLD')
        def code_match(row_code):
            row_code_str = str(row_code).strip().upper().replace('KRX:', '').replace('A','')
            if is_gold: return row_code_str == 'GOLD'
            else: return row_code_str == stock_code_str
        purchase_trades_df = trades_df[(trades_df[TRADE_CODE_HEADER].apply(code_match)) & (trades_df[TRADE_TYPE_HEADER] == '매수')]
        if not purchase_trades_df.empty: first_date = purchase_trades_df['Date'].min(); print(f"Log: Success! First purchase date for '{stock_code}': {first_date.strftime('%Y-%m-%d')}")
        else: print(f"Log: Failed to find valid purchase date for '{stock_code}'.")
    except gspread.exceptions.WorksheetNotFound: st.error(f"워크시트 '{TRADES_SHEET}'를 찾을 수 없음.")
    except Exception as e: st.error(f"최초 매수일 조회 중 오류: {e}"); traceback.print_exc()
    return first_date

def get_yf_ticker(stock_code):
    """종목코드를 Yahoo Finance 티커 형식으로 변환 (기존과 동일)"""
    code = str(stock_code).strip()
    if code == 'GOLD': return None
    if code.startswith('KRX:'): code_only = code.split(':')[-1]
    elif code.startswith('A') and code[1:].isdigit(): code_only = code[1:]
    else: code_only = code
    if code_only.isdigit() and len(code_only) == 6: return f"{code_only}.KS"
    elif code_only.isalnum() or '.' in code_only: return code_only.upper()
    else: return code_only

# **** 신규 함수: 금 가격 데이터 로드 ****
@st.cache_data(ttl=600)
def load_gold_price_data(_gc):
    """📈금현물 수익률 시트에서 날짜(A열)와 금가격(J열)을 로드합니다."""
    if not isinstance(_gc, gspread.Client):
        st.error("load_gold_price_data: 유효한 Google Sheets 클라이언트 객체(gc)가 아닙니다.")
        return pd.DataFrame()

    DATE_COL = 1  # A열
    PRICE_COL = 10 # J열

    try:
        print(f"Log: Loading gold price data from '{GOLD_RATE_SHEET}'...")
        spreadsheet = _gc.open(GOOGLE_SHEET_NAME)
        worksheet = spreadsheet.worksheet(GOLD_RATE_SHEET)

        # A열과 J열 데이터 가져오기
        data = worksheet.get_all_values() # 전체 데이터를 가져오는 것이 더 효율적일 수 있음
        if len(data) < 2: # 헤더만 있거나 비어있는 경우
            st.warning(f"'{GOLD_RATE_SHEET}' 시트에 데이터가 부족합니다 (헤더 제외).")
            return pd.DataFrame()

        # 헤더와 데이터 분리
        header = data[0]
        records = data[1:]

        # 데이터프레임 생성 준비
        dates = []
        prices = []
        expected_date_header = '날짜' # 실제 시트의 A1 셀 값
        expected_price_header = header[PRICE_COL-1] if len(header) >= PRICE_COL else f'Column_{PRICE_COL}' # J1 셀 값 또는 기본값

        for i, row in enumerate(records):
            if len(row) >= PRICE_COL: # 행 길이가 충분한지 확인
                date_str = row[DATE_COL-1]
                price_str = row[PRICE_COL-1]

                # 날짜 파싱
                dt_obj = None
                try:
                    dt_obj = pd.to_datetime(date_str, errors='coerce')
                except ValueError: # 다양한 형식 시도 (필요 시)
                    pass

                if pd.notna(dt_obj):
                    dates.append(dt_obj)
                    # 가격 숫자 변환 (소수점 유지 위해 float)
                    prices.append(clean_numeric_value(price_str, float))
                #else: # 날짜 파싱 실패 시 경고 (너무 많을 수 있으므로 주석 처리)
                    #print(f"Log: Skipping row {i+2} in '{GOLD_RATE_SHEET}' due to date parsing error: '{date_str}'")

        if not dates:
            st.warning(f"'{GOLD_RATE_SHEET}' 시트에서 유효한 날짜 데이터를 찾지 못했습니다.")
            return pd.DataFrame()

        # 데이터프레임 생성
        df = pd.DataFrame({'Date': dates, 'Close': prices})
        df = df.set_index('Date')
        df = df.sort_index() # 날짜 순으로 정렬
        print(f"Log: Gold price data loaded successfully ({len(df)} rows).")
        return df

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"워크시트 '{GOLD_RATE_SHEET}'를 찾을 수 없습니다.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"금 가격 데이터 로딩 중 오류 발생: {e}")
        traceback.print_exc()
        return pd.DataFrame()
# ***************************************

# --- 데이터 로드 실행 및 대시보드 구성 ---
gc = connect_google_sheets()

if gc:
    twr_data_df = load_twr_data()
    gain_loss_data = load_gain_loss_data()
    latest_balances, latest_data_date = load_latest_balances(gc)
    if latest_data_date:
        allocation_comparison_df, target_allocation_df = load_allocation_data(gc, latest_data_date)
    else:
        st.warning("최신 데이터 기준일을 가져올 수 없어 자산 배분 정보를 로드할 수 없습니다.")
        allocation_comparison_df, target_allocation_df = pd.DataFrame(), pd.DataFrame()
else:
    st.error("Google Sheets에 연결할 수 없어 데이터를 로드할 수 없습니다.")
    twr_data_df = pd.DataFrame(); gain_loss_data = {}
    latest_balances, latest_data_date = {}, None
    allocation_comparison_df, target_allocation_df = pd.DataFrame(), pd.DataFrame()

# --- 대시보드 제목 및 데이터 기준일 ---
st.title(PAGE_TITLE)
if latest_data_date: st.caption(f"데이터 기준일: {latest_data_date.strftime('%Y년 %m월 %d일')}")
else: st.caption("데이터 기준일을 불러올 수 없습니다.")

# --- 개요 (Overview) 섹션 ---
st.header("📊 개요")
col1, col2, col3 = st.columns(3)
total_asset = 0
if latest_balances: total_asset = np.nansum(pd.to_numeric(list(latest_balances.values()), errors='coerce'))
col1.metric("💰 총 평가액", f"{total_asset:,.0f} 원")
latest_twr = "N/A"
if not twr_data_df.empty and 'Total' in twr_data_df['Account'].unique():
    total_twr_series = twr_data_df[twr_data_df['Account'] == 'Total'].sort_values(by='Date', ascending=False)
    if not total_twr_series.empty: latest_twr_value = total_twr_series['TWR'].iloc[0]; latest_twr = f"{latest_twr_value:.2f}%" if pd.notna(latest_twr_value) else "N/A"
col2.metric("📈 전체 TWR (기간)", latest_twr)
total_gain_loss = "N/A"
if gain_loss_data and 'Total' in gain_loss_data and gain_loss_data['Total'] is not None: total_gain_loss = f"{gain_loss_data['Total']:,.0f} 원"
col3.metric("💸 전체 단순 손익 (기간)", total_gain_loss)
st.markdown("---")

# --- 자산 배분 섹션 ---
st.header("⚖️ 자산 배분")
if allocation_comparison_df is not None and not allocation_comparison_df.empty:
    st.subheader("현재 vs 목표 비중 비교")
    df_to_display = allocation_comparison_df.copy()
    total_target_amount = 0; target_amount_col_original = '목표 금액'; new_target_amount_header = target_amount_col_original
    if target_amount_col_original in df_to_display.columns:
        try: numeric_target_amounts = pd.to_numeric(df_to_display[target_amount_col_original], errors='coerce').fillna(0); total_target_amount = int(numeric_target_amounts.sum()); new_target_amount_header = f"{target_amount_col_original} (총 {total_target_amount:,.0f} 원)"
        except Exception as e_sum: print(f"Warning: 목표 금액 총합 계산 중 오류: {e_sum}")
    formats = {'현재 비중(%)': '{:.2f}%', '목표 비중(%)': '{:.2f}%', '차이(%)': '{:+.2f}%', '현재 평가액': '{:,.0f} 원', target_amount_col_original: '{:,.0f} 원', '현금차이': '{:+,d} 원'}
    if new_target_amount_header != target_amount_col_original and target_amount_col_original in df_to_display.columns: df_to_display.rename(columns={target_amount_col_original: new_target_amount_header}, inplace=True); formats[new_target_amount_header] = formats.pop(target_amount_col_original)
    original_display_cols = ['종합 분류', '목표 비중(%)', '현재 비중(%)', '차이(%)', '목표 금액', '현재 평가액', '현금차이']
    display_cols_final = [];
    for col in original_display_cols:
        if col == target_amount_col_original and new_target_amount_header != target_amount_col_original:
             if new_target_amount_header in df_to_display.columns: display_cols_final.append(new_target_amount_header)
        elif col in df_to_display.columns: display_cols_final.append(col)
    available_formats = {k: v for k, v in formats.items() if k in display_cols_final}
    if display_cols_final:
        sort_col = '목표 비중(%)' if '목표 비중(%)' in display_cols_final else ('종합 분류' if '종합 분류' in display_cols_final else None)
        if sort_col and sort_col == '목표 비중(%)' and not pd.api.types.is_numeric_dtype(df_to_display[sort_col]): sort_col = '종합 분류'
        if sort_col: df_display_final = df_to_display[display_cols_final].sort_values(by=sort_col, ascending=False).reset_index(drop=True)
        else: df_display_final = df_to_display[display_cols_final] # 정렬 컬럼 없으면 그냥 표시
        st.dataframe(df_display_final.style.format(available_formats).set_properties(**{'text-align': 'center'}))
    else: st.warning("비교 테이블 표시할 컬럼 부족.")

    st.subheader("자산 배분 시각화 (종합 분류 기준)")
    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        if '현재 비중(%)' in allocation_comparison_df.columns:
             current_display_df = allocation_comparison_df[allocation_comparison_df['현재 비중(%)'] > 0].copy()
             if not current_display_df.empty:
                 current_display_df.sort_values(by='현재 비중(%)', inplace=True); fig_current = px.pie(current_display_df, values='현재 비중(%)', names='종합 분류', title='현재 자산 배분', hole=.4, color_discrete_sequence=px.colors.sequential.RdBu); fig_current.update_traces(textposition='outside', textinfo='percent+label', insidetextorientation='radial', sort=False); fig_current.update_layout(showlegend=False, margin=dict(l=40, r=40, t=50, b=40)); st.plotly_chart(fig_current, use_container_width=True)
             else: st.info("현재 보유 자산 비중 정보 없음 (0% 초과).")
        else: st.info("현재 보유 자산 비중 정보 없음.")
    with col_chart2:
        if target_allocation_df is not None and not target_allocation_df.empty and '목표 비중(%)' in target_allocation_df.columns:
             target_display_df = target_allocation_df[target_allocation_df['목표 비중(%)'] > 0].copy()
             if not target_display_df.empty:
                 target_display_df.sort_values(by='목표 비중(%)', inplace=True); fig_target = px.pie(target_display_df, values='목표 비중(%)', names='종합 분류', title='목표 자산 배분', hole=.4, color_discrete_sequence=px.colors.sequential.RdBu); fig_target.update_traces(textposition='outside', textinfo='percent+label', insidetextorientation='radial', sort=False); fig_target.update_layout(showlegend=False, margin=dict(l=40, r=40, t=50, b=40)); st.plotly_chart(fig_target, use_container_width=True)
             else: st.info("목표 자산 배분 정보 없음 (0% 초과).")
        else: st.info("목표 자산 배분 정보 없음.")
elif allocation_comparison_df is None: st.error("자산 배분 데이터 로딩 실패 또는 기준 날짜 데이터 없음.")
else: st.info("표시할 자산 배분 정보 없음.")
st.markdown("---")

# --- 성과 분석 섹션 ---
st.header("📈 성과 분석")
if twr_data_df is not None and not twr_data_df.empty:
    st.subheader("📊 시간가중수익률(TWR) 추이")
    st.markdown("#### 전체 포트폴리오 TWR 및 시장 지수 비교")
    total_twr_df = twr_data_df[twr_data_df['Account'] == 'Total'].sort_values(by='Date')
    if not total_twr_df.empty:
        start_date = total_twr_df['Date'].min(); end_date = total_twr_df['Date'].max()
        kospi_raw_data = download_yf_data(KOSPI_TICKER, start_date, end_date)
        sp500_raw_data = download_yf_data(SP500_TICKER, start_date, end_date)
        kospi_twr_df = calculate_index_twr(kospi_raw_data, KOSPI_TICKER)
        sp500_twr_df = calculate_index_twr(sp500_raw_data, SP500_TICKER)
        fig_total_compare = go.Figure()
        fig_total_compare.add_trace(go.Scatter(x=total_twr_df['Date'], y=total_twr_df['TWR'], mode='lines', name='전체 포트폴리오', line=dict(color='royalblue', width=2.5)))
        if kospi_twr_df is not None and not kospi_twr_df.empty: fig_total_compare.add_trace(go.Scatter(x=kospi_twr_df['Date'], y=kospi_twr_df['TWR'], mode='lines', name='KOSPI 200', line=dict(color='tomato', width=1.5, dash='dash')))
        if sp500_twr_df is not None and not sp500_twr_df.empty: fig_total_compare.add_trace(go.Scatter(x=sp500_twr_df['Date'], y=sp500_twr_df['TWR'], mode='lines', name='S&P 500', line=dict(color='mediumseagreen', width=1.5, dash='dot')))
        fig_total_compare.update_layout(title='전체 포트폴리오 TWR 및 시장 지수 비교', xaxis_title='날짜', yaxis_title='TWR (%)', legend_title='구분', hovermode="x unified")
        st.plotly_chart(fig_total_compare, use_container_width=True)
    else: st.info("전체 TWR 데이터 없음.")

    st.markdown("#### 계좌별 TWR")
    account_twr_df = twr_data_df[twr_data_df['Account'] != 'Total'].sort_values(by=['Account', 'Date'])
    if not account_twr_df.empty:
        fig_accounts_twr = px.line(account_twr_df, x='Date', y='TWR', color='Account', title='계좌별 TWR 추이', labels={'TWR': 'TWR (%)'})
        fig_accounts_twr.update_layout(xaxis_title='날짜', yaxis_title='수익률 (%)')
        st.plotly_chart(fig_accounts_twr, use_container_width=True)
    else: st.info("개별 계좌 TWR 데이터 없음.")

    # **** 종목별 그래프 로직 수정 ****
    st.markdown("---"); st.subheader("📈 종목별 가격/주가 및 평단가 (이동평균법)")
    if gc and latest_data_date:
        holdings_list_df = load_current_holdings(gc, latest_data_date)
        if holdings_list_df is not None and not holdings_list_df.empty:
            stock_options = holdings_list_df.set_index('종목명')['종목코드'].to_dict()
            stock_names = ["종목을 선택하세요..."] + list(holdings_list_df['종목명'])
            default_index = 0; default_stock_name = "TIGER미국S&P500" # 기본 선택 종목
            try:
                 if default_stock_name in stock_names: default_index = stock_names.index(default_stock_name)
            except ValueError: pass
            selected_stock_name = st.selectbox("종목 선택:", stock_names, index=default_index)

            if selected_stock_name != "종목을 선택하세요...":
                stock_code = stock_options.get(selected_stock_name)
                if stock_code:
                    avg_cost = calculate_moving_avg_cost(gc, stock_code) if gc else 0.0
                    first_purchase_dt = get_first_purchase_date(gc, stock_code) if gc else None

                    if first_purchase_dt:
                        chart_start_date = first_purchase_dt.date() # 날짜만 사용
                        current_date = datetime.now().date()
                        close_price_df = pd.DataFrame() # 초기화
                        plot_title = f"{selected_stock_name}" # 기본 제목

                        # --- 금현물과 다른 종목 로직 분기 ---
                        if stock_code == 'GOLD':
                            st.info(f"'{selected_stock_name}' 가격 데이터를 구글 시트 '{GOLD_RATE_SHEET}'에서 로드합니다.")
                            gold_price_history = load_gold_price_data(gc) # 신규 함수 호출
                            if gold_price_history is not None and not gold_price_history.empty:
                                # 구매 시작일 이후 데이터 필터링
                                gold_data_filtered = gold_price_history[gold_price_history.index.date >= chart_start_date]
                                if not gold_data_filtered.empty:
                                    close_price_df = gold_data_filtered # 'Close' 컬럼 사용
                                    plot_title = f"{selected_stock_name} 가격 추이 및 평단가 (KRW/g)"
                                else:
                                    st.warning(f"'{selected_stock_name}'의 매수 시작일({chart_start_date}) 이후 가격 데이터가 '{GOLD_RATE_SHEET}' 시트에 없습니다.")
                            else:
                                st.warning(f"'{selected_stock_name}' 가격 데이터를 '{GOLD_RATE_SHEET}' 시트에서 로드하지 못했습니다.")

                        else: # 금현물 외 다른 종목 (Yahoo Finance 사용)
                            yf_ticker = get_yf_ticker(stock_code)
                            if not yf_ticker:
                                st.info(f"{selected_stock_name} ({stock_code})의 외부 주가 정보를 조회할 수 없습니다.")
                                # 평단가만 표시 (그래프 없음)
                                if avg_cost > 0: st.metric(label=f"{selected_stock_name} 평단가 (이동평균)", value=f"{avg_cost:,.0f} 원")
                                else: st.metric(label=f"{selected_stock_name} 평단가 (이동평균)", value="계산 불가")
                            else:
                                st.info(f"{selected_stock_name}({yf_ticker}) 주가 데이터를 Yahoo Finance에서 로드합니다.")
                                stock_price_data = download_yf_data(yf_ticker, chart_start_date, current_date)
                                if stock_price_data is not None and not stock_price_data.empty:
                                    # 종가 컬럼 찾기 (이전 로직 개선)
                                    close_col_found = None
                                    if isinstance(stock_price_data.columns, pd.MultiIndex):
                                        potential_cols = [('Close', yf_ticker), ('Adj Close', yf_ticker), ('Close', ''), ('Adj Close', '')]
                                        for col in potential_cols:
                                            if col in stock_price_data.columns: close_col_found = col; break
                                        if not close_col_found:
                                             level_zero = stock_price_data.columns.get_level_values(0)
                                             if 'Close' in level_zero: close_col_found = [c for c in stock_price_data.columns if c[0] == 'Close'][0]
                                             elif 'Adj Close' in level_zero: close_col_found = [c for c in stock_price_data.columns if c[0] == 'Adj Close'][0]
                                    elif 'Close' in stock_price_data.columns: close_col_found = 'Close'
                                    elif 'Adj Close' in stock_price_data.columns: close_col_found = 'Adj Close'

                                    if close_col_found:
                                        temp_df = stock_price_data[[close_col_found]].copy()
                                        temp_df.columns = ['Close'] # 컬럼명 통일
                                        close_price_df = temp_df.dropna(subset=['Close'])
                                        plot_title = f"{selected_stock_name} ({yf_ticker}) 주가 추이 및 평단가"
                                    else:
                                        st.warning(f"{selected_stock_name}({yf_ticker}) 데이터에서 종가 컬럼을 찾을 수 없습니다.")
                                else:
                                    st.warning(f"{selected_stock_name}({yf_ticker}) 주가 데이터를 다운로드하지 못했습니다.")
                        # --- ---

                        # --- 그래프 출력 (데이터가 있을 경우 공통) ---
                        if not close_price_df.empty:
                            fig_stock = go.Figure()
                            fig_stock.add_trace(go.Scatter(x=close_price_df.index, y=close_price_df['Close'], mode='lines', name='종가/가격', line=dict(color='skyblue', width=2)))

                            # 평단가 표시 (avg_cost가 0보다 클 때)
                            if avg_cost > 0:
                                # 금현물은 소수점 표시, 주식은 정수 표시 (선택적)
                                avg_cost_format = "{:,.2f}" if stock_code == 'GOLD' else "{:,.0f}"
                                fig_stock.add_hline(y=avg_cost, line_dash="dot", line_color="tomato",
                                                    annotation_text=f"평단가: {avg_cost_format.format(avg_cost)}",
                                                    annotation_position="bottom right")

                            fig_stock.update_layout(
                                title=plot_title,
                                xaxis_title='날짜',
                                yaxis_title='가격 (KRW)', # 단위 통일 (필요시 수정)
                                yaxis=dict(showticklabels=True, autorange=True), # Y축 범위 자동 조절
                                hovermode="x unified"
                            )
                            st.plotly_chart(fig_stock, use_container_width=True)
                        elif yf_ticker: # Yahoo Finance 사용 종목인데 데이터 로드 실패/종가 없음
                             pass # 위에서 이미 경고/정보 메시지 표시됨
                        elif stock_code == 'GOLD' and not gold_price_history.empty: # 금 데이터는 있는데 시작일 이후 데이터 없는 경우
                             pass # 위에서 이미 경고 메시지 표시됨
                        elif stock_code != 'GOLD' and not yf_ticker: # 조회 불가 티커
                             pass # 위에서 이미 정보 메시지 표시됨
                        else: # 그 외 (최초 매수일 없음 등)
                             st.info("그래프를 표시할 가격 데이터가 없습니다.")
                    # --- ---

                    elif gc: # first_purchase_dt가 없는 경우
                         st.warning(f"{selected_stock_name}의 매수 기록을 '{TRADES_SHEET}' 시트에서 찾을 수 없어 그래프 시작일을 지정할 수 없습니다.")
                    # else: # gc 연결 실패는 이미 처리됨
                    #    pass
                else:
                    st.error("선택된 종목의 코드를 찾을 수 없습니다.")
        elif holdings_list_df is None:
             st.error("현재 보유 중인 종목 정보를 불러오는 데 실패했습니다.")
        else:
            st.info("현재 보유 중인 종목 정보가 없습니다. ('일별비중_Raw' 시트 확인 필요)")
    else:
        st.info("구글 시트 연결 실패 또는 최신 데이터 날짜가 없어 보유 종목을 조회할 수 없습니다.")
    # ***************************************

elif twr_data_df is None: st.error("성과 분석 데이터 로딩 실패.")
else: st.warning("성과 분석 위한 TWR 데이터 없음. `portfolio_performance.py` 실행 필요.")

st.markdown("---")

# --- 데이터 조회 섹션 (구현 예정) ---
st.header("📝 데이터 조회 (구현 예정)")
with st.expander("데이터 조회 섹션 구현 아이디어 보기"):
    st.markdown("""
    '데이터 조회' 섹션은 대시보드의 다양한 계산과 시각화에 사용된 **원본 데이터를 사용자가 직접 확인하고 탐색**할 수 있도록 만드는 공간입니다.
    구현 아이디어:
    * **테이블 표시:** 사용자가 특정 시트(예: `일별잔고_Raw`, `일별비중_Raw`, `🗓️매매일지`, `twr_results.csv` 등)를 선택하면 해당 데이터를 테이블 형태로 보여줍니다.
    * **필터링/정렬:** 날짜, 계좌명, 종목명 등으로 데이터를 필터링하거나 특정 컬럼 기준으로 정렬하는 기능을 제공합니다.
    * **데이터 다운로드:** 사용자가 필터링/선택한 데이터를 CSV 파일 등으로 다운로드 받을 수 있게 합니다.
    * **간단한 시각화:** 선택된 데이터에 대해 간단한 라인 차트나 막대 차트를 즉석에서 그려볼 수 있는 옵션을 제공할 수 있습니다. (예: 특정 종목의 일별 평가액 추이)
    """)
# --- ---