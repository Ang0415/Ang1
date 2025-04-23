# -*- coding: utf-8 -*-
# Workspace_kiwoom_trades.py: 키움증권 매매 내역(최근 7일)을 조회하여 구글 시트 '매매일지_Raw'에 기록 (공휴일 제외)
# (텔레그램 알림 수정: 설정 파일 로드 방식)

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, date
import time
import traceback
import os
import sys
# import requests # telegram_utils 사용하므로 직접 임포트 불필요

# 공휴일 처리를 위한 라이브러리 임포트
try:
    import holidays
except ImportError:
    print("⚠️ 'holidays' 라이브러리가 설치되지 않았습니다. 공휴일 제외 없이 진행됩니다.")
    print("   (설치 방법: pip install holidays)")
    holidays = None # 라이브러리 없으면 None으로 설정

# 키움 API 모듈 임포트 (인증 모듈에서 경로 처리 완료됨 가정)
import kiwoom_auth_isa as auth
import kiwoom_domstk_isa as kiwoom_api # 사용자 파일명 사용

# --- 텔레그램 유틸리티 임포트 ---
import telegram_utils # 또는 from telegram_utils import send_telegram_message
# --- ---

# --- 설정 ---
GOOGLE_SHEET_NAME = 'KYI_자산배분'
TRADES_WORKSHEET_NAME = '매매일지_Raw' # 키움 거래 기록용 시트 이름
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_KEYFILE_PATH = os.path.join(CURRENT_DIR, 'stock-auto-writer-44eaa06c140c.json')
DEFAULT_FETCH_DAYS = 7
TRADE_LOG_COLUMNS = [
    '날짜', '시간', '증권사', '계좌구분', '종목코드', '종목명',
    '매매구분', '수량', '단가', '금액', '수수료', '세금', '메모'
]
SCRIPT_NAME = os.path.basename(__file__) # 스크립트 파일명 가져오기
# --- ---

# --- 구글 시트 설정 함수 ---
def setup_google_sheet():
    """구글 시트에 연결하고 '매매일지_Raw' 워크시트 객체를 반환합니다."""
    worksheet = None
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE_PATH, scope)
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        try:
            worksheet = spreadsheet.worksheet(TRADES_WORKSHEET_NAME)
            print(f"✅ Google Sheet '{GOOGLE_SHEET_NAME}/{TRADES_WORKSHEET_NAME}' 워크시트 열기 성공.")
            header = worksheet.row_values(1)
            if not header or header != TRADE_LOG_COLUMNS:
                 print(f"⚠️ 워크시트 헤더가 비어있거나 예상과 다릅니다. 헤더를 업데이트합니다.")
                 all_values = worksheet.get_all_values() # 헤더 업데이트 전 전체 값 확인
                 if not all_values:
                     worksheet.append_row(TRADE_LOG_COLUMNS, value_input_option='USER_ENTERED')
                     print("✅ 비어있는 시트에 헤더 행 추가 완료.")
                 else:
                     try:
                         worksheet.update('A1', [TRADE_LOG_COLUMNS], value_input_option='USER_ENTERED')
                         print("✅ 헤더 행 업데이트 완료.")
                     except Exception as e_header:
                         print(f"❗️ 헤더 자동 업데이트 실패. 수동 확인 필요: {e_header}")
        except gspread.exceptions.WorksheetNotFound:
            print(f"⚠️ 워크시트 '{TRADES_WORKSHEET_NAME}'을(를) 찾을 수 없어 새로 생성합니다.")
            worksheet = spreadsheet.add_worksheet(title=TRADES_WORKSHEET_NAME, rows="1000", cols=len(TRADE_LOG_COLUMNS))
            worksheet.append_row(TRADE_LOG_COLUMNS, value_input_option='USER_ENTERED')
            print(f"✅ 워크시트 '{TRADES_WORKSHEET_NAME}' 생성 및 헤더 추가 완료.")
        return worksheet
    except FileNotFoundError:
         print(f"❌ 오류: 서비스 계정 키 파일({JSON_KEYFILE_PATH})을 찾을 수 없습니다.")
         return None
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"❌ 오류: 스프레드시트 '{GOOGLE_SHEET_NAME}'을 찾을 수 없습니다.")
        return None
    except Exception as e:
        print(f"❌ 구글 시트 연결 중 오류 발생: {e}")
        traceback.print_exc()
        return None

# --- 데이터 처리 함수 ---
def clean_num_str(num_str, type_func=int):
    """문자열 형태의 숫자를 실제 숫자 타입으로 변환"""
    if not num_str: return type_func(0)
    try:
        cleaned_str = str(num_str).replace(',', '')
        is_negative = cleaned_str.startswith('-')
        cleaned = cleaned_str.lstrip('-').lstrip('0')
        if not cleaned: return type_func(0)
        value = type_func(cleaned)
        return -value if is_negative else value
    except (ValueError, TypeError):
        return type_func(0)

def format_trade_data(api_response_data, base_date):
    """ka10170 응답 데이터를 매매일지 형식에 맞게 변환"""
    formatted_rows = []
    trade_list_key = 'tdy_trde_diary' # 실제 키움 API 응답 키
    trade_list = api_response_data.get(trade_list_key, [])

    if not trade_list: # 응답에 거래 내역 리스트가 없으면 빈 리스트 반환
        return formatted_rows

    for item in trade_list:
        stock_code_raw = item.get('stk_cd', '')
        stock_name = item.get('stk_nm', '')

        # 매수 정보 처리
        buy_qty = clean_num_str(item.get('buy_qty', '0'), int)
        if buy_qty > 0:
            buy_price = clean_num_str(item.get('buy_avg_pric', '0'), int)
            buy_amount = clean_num_str(item.get('buy_amt', '0'), int)
            buy_fees = 0 # 키움 API는 매수 시 수수료/세금 정보 제공 안 함
            buy_tax = 0
            stock_code = 'A' + stock_code_raw if stock_code_raw and stock_code_raw.isdigit() and not stock_code_raw.startswith('A') else stock_code_raw
            row = [base_date, "", '키움', 'ISA', stock_code, stock_name, '매수', buy_qty, buy_price, buy_amount, buy_fees, buy_tax, 'Kiwoom API(ka10170)']
            formatted_rows.append(row)

        # 매도 정보 처리
        sell_qty = clean_num_str(item.get('sell_qty', '0'), int)
        if sell_qty > 0:
            sell_price = clean_num_str(item.get('sel_avg_pric', '0'), int)
            sell_amount = clean_num_str(item.get('sell_amt', '0'), int)
            commission_tax = clean_num_str(item.get('cmsn_alm_tax', '0'), int) # 수수료+세금 합계
            sell_fees = 0 # 실제 수수료와 세금을 분리하려면 별도 계산 필요 (여기서는 세금에 합산)
            sell_tax = commission_tax
            stock_code = 'A' + stock_code_raw if stock_code_raw and stock_code_raw.isdigit() and not stock_code_raw.startswith('A') else stock_code_raw
            row = [base_date, "", '키움', 'ISA', stock_code, stock_name, '매도', sell_qty, sell_price, sell_amount, sell_fees, sell_tax, 'Kiwoom API(ka10170)']
            formatted_rows.append(row)

    return formatted_rows

# --- 메인 실행 로직 ---
def main():
    start_time = time.time() # 시작 시간 기록
    print(f"🚀 키움증권 매매일지 기록 시작 (최근 {DEFAULT_FETCH_DAYS}일 기본)")
    total_new_trades = 0 # 새로 추가된 거래 건수

    # 1. API 인증 ( authenticate() 가 True/False 반환 가정 )
    if not auth.authenticate():
        raise ConnectionError("🔥 키움 API 인증 실패! 프로그램 종료.")

    # 2. 구글 시트 연결
    worksheet = setup_google_sheet()
    if not worksheet:
        raise ConnectionError("🔥 구글 시트 연결 실패! 프로그램 종료.")

    # 3. 마지막 기록 날짜 확인 및 시작 날짜 결정
    last_processed_date_str = None # 마지막 기록된 날짜 (YYYY-MM-DD)
    existing_records_keys = set() # 중복 체크용 키: (날짜(YYYY-MM-DD), 종목코드, 매매구분)
    try:
        all_data = worksheet.get_all_values() # 헤더 포함 전체 데이터 가져오기
        if len(all_data) > 1: # 헤더 외 데이터 있는지 확인
            df_sheet = pd.DataFrame(all_data[1:], columns=all_data[0]) # 헤더 제외하고 DF 생성
            required_cols = ['날짜', '종목코드', '매매구분', '증권사']
            if all(col in df_sheet.columns for col in required_cols):
                df_kiwoom = df_sheet[df_sheet['증권사'] == '키움'].copy()
                if not df_kiwoom.empty:
                    df_kiwoom['날짜_dt'] = pd.to_datetime(df_kiwoom['날짜'], errors='coerce')
                    valid_trades = df_kiwoom.dropna(subset=['날짜_dt']).copy() # 날짜 변환 성공한 데이터만
                    if not valid_trades.empty:
                        last_date = valid_trades['날짜_dt'].max().date()
                        last_processed_date_str = last_date.strftime('%Y-%m-%d')
                        print(f"✅ 마지막 '키움' 거래 기록 날짜: {last_processed_date_str}.")
                        recent_start_date = last_date - timedelta(days=DEFAULT_FETCH_DAYS + 5) # 여유있게 범위 설정
                        recent_trades = valid_trades[valid_trades['날짜_dt'].dt.date >= recent_start_date]
                        for index, row in recent_trades.iterrows():
                            key = (row['날짜_dt'].strftime('%Y-%m-%d'), str(row.get('종목코드', '')).strip(), str(row.get('매매구분', '')).strip())
                            existing_records_keys.add(key)
                        print(f"  > 최근 기존 '키움' 기록 {len(existing_records_keys)}건 확인 (중복 체크용)")
                    else: print(f"ℹ️ 시트에 유효한 날짜의 '키움' 거래 기록 없음.")
                else: print(f"ℹ️ 시트에 '키움' 증권사 거래 기록 없음.")
            else: print(f"ℹ️ 시트 헤더에 필요한 컬럼({required_cols}) 없음.")
        else: print(f"ℹ️ 시트가 비어있거나 헤더만 존재.")
    except Exception as e:
        print(f"⚠️ 기존 매매일지 로드/처리 중 오류 발생: {e}. 기본 조회 기간(최근 {DEFAULT_FETCH_DAYS}일)을 사용합니다.")
        traceback.print_exc()
        last_processed_date_str = None # 오류 시 처음부터 조회하도록

    # 4. API 조회 시작/종료 날짜 결정
    today = datetime.now().date()
    end_fetch_date = today # 조회 종료일은 오늘
    if last_processed_date_str:
        start_fetch_date = datetime.strptime(last_processed_date_str, '%Y-%m-%d').date() + timedelta(days=1)
        max_fetch_days = 30
        if (today - start_fetch_date).days > max_fetch_days:
             print(f"⚠️ 마지막 기록일로부터 너무 오래되었습니다. 최대 {max_fetch_days}일 데이터만 조회합니다.")
             start_fetch_date = today - timedelta(days=max_fetch_days -1)
    else:
         start_fetch_date = today - timedelta(days=DEFAULT_FETCH_DAYS - 1)
         print(f"ℹ️ 기록 없음. 시작 날짜를 오늘로부터 {DEFAULT_FETCH_DAYS}일 전({start_fetch_date})으로 설정.")

    if start_fetch_date > end_fetch_date:
        print(f"ℹ️ 조회할 새로운 날짜 범위가 없습니다 (시작: {start_fetch_date}, 종료: {end_fetch_date}). 종료합니다.")
        end_time = time.time()
        elapsed_time = end_time - start_time
        return f"✅ `{SCRIPT_NAME}` 실행 완료 (신규 조회 대상 없음, 소요 시간: {elapsed_time:.2f}초)"

    print(f"🗓️ 키움 매매 내역 API 조회 기간: {start_fetch_date} ~ {end_fetch_date}")

    # 5. 날짜별 API 호출 및 데이터 처리/기록
    all_new_trades_formatted = [] # 새로 추가할 전체 거래 내역 리스트
    current_date = start_fetch_date
    api_call_count = 0
    kr_holidays = {} # 공휴일 정보 초기화
    if holidays:
        try:
            kr_holidays = holidays.KR(years=range(start_fetch_date.year, end_fetch_date.year + 1), observed=True)
            print(f"ℹ️ {start_fetch_date.year}-{end_fetch_date.year}년 공휴일 정보 로드 완료.")
        except Exception as e_holiday:
            print(f"⚠️ 공휴일 정보 로드 중 오류 발생: {e_holiday}. 공휴일 제외 없이 진행합니다.")
            kr_holidays = {} # 오류 시 빈 딕셔너리로 설정

    while current_date <= end_fetch_date:
        date_str_ymd = current_date.strftime("%Y-%m-%d") #<y_bin_46>-MM-DD
        date_yyyymmdd = current_date.strftime("%Y%m%d") #<y_bin_46>MMDD (API 파라미터용)
        weekday = current_date.weekday()
        is_holiday_check = current_date in kr_holidays

        if weekday >= 5 or is_holiday_check:
            day_type = "주말" if weekday >= 5 else "공휴일"
            # print(f"  > {date_str_ymd} ({day_type}): 건너<0xEB><0x81><0x91니다.") # 로그 간소화
        else:
            # --- 영업일에만 API 호출 ---
            print(f"  > {date_str_ymd}: 키움 매매 내역 API 조회 시도...")
            trade_log_result = kiwoom_api.get_daily_trading_log(base_date=date_yyyymmdd, ottks_type='0', cash_credit_type='0')
            api_call_count += 1

            if trade_log_result and trade_log_result.get('success'):
                formatted_trades = format_trade_data(trade_log_result.get('data', {}), date_str_ymd)
                if formatted_trades:
                    print(f"    - {len(formatted_trades)}건의 거래 내역 확인.")
                    added_count = 0
                    for trade_row in formatted_trades:
                        key = (trade_row[0], str(trade_row[4]).strip(), str(trade_row[6]).strip())
                        if key not in existing_records_keys:
                            all_new_trades_formatted.append(trade_row)
                            existing_records_keys.add(key) # 추가된 키도 중복 방지 위해 기록
                            added_count += 1
                    if added_count > 0:
                         print(f"    - {added_count}건의 신규 거래 내역 추가 예정.")
                         total_new_trades += added_count # 총 신규 거래 건수 누적
                else: print(f"    - 해당 날짜 거래 내역 없음.")
            else: print(f"    - API 조회 실패 또는 오류 응답. 건너<0xEB><0x81><0x91니다.")
            time.sleep(0.21) # API 연속 호출 방지 딜레이
            # --- API 호출 종료 ---
        current_date += timedelta(days=1)

    # 6. 구글 시트에 신규 데이터 추가
    if all_new_trades_formatted:
        print(f"\n💾 총 {len(all_new_trades_formatted)} 건의 신규 '키움' 거래 내역을 '{TRADES_WORKSHEET_NAME}' 시트에 추가합니다...")
        try:
            worksheet.append_rows(all_new_trades_formatted, value_input_option='USER_ENTERED')
            print("✅ 데이터 추가 완료!")
        except Exception as e:
            # 데이터 추가 실패 시 오류 발생
            raise IOError(f"❌ 구글 시트 데이터 추가 중 오류 발생: {e}") from e
    else:
        print("\nℹ️ 구글 시트에 추가할 신규 '키움' 거래 내역이 없습니다.")

    end_time = time.time() # 종료 시간 기록
    elapsed_time = end_time - start_time
    print(f"\n🏁 키움증권 매매일지 기록 작업 완료 (API 호출 {api_call_count}회, 소요 시간: {elapsed_time:.2f}초).")
    # 성공 메시지 반환
    return f"✅ `{SCRIPT_NAME}` 실행 완료 (신규 거래 {total_new_trades}건 추가, 소요 시간: {elapsed_time:.2f}초)"

# --- 스크립트 실행 및 텔레그램 알림 ---
if __name__ == '__main__':
    start_run_time = time.time() # 실행 시작 시간 기록
    final_message = ""
    error_occurred = False
    error_details_str = ""

    try:
        # 메인 로직 실행
        main() # main 함수는 이제 성공 메시지를 반환하지 않음
    except ConnectionError as e:
        error_occurred = True
        print(f"🔥 스크립트 실행 중 연결 오류 발생: {e}")
        error_details_str = traceback.format_exc()
    except IOError as e:
        error_occurred = True
        print(f"🔥 스크립트 실행 중 IO 오류 발생: {e}")
        error_details_str = traceback.format_exc()
    except Exception as e:
        error_occurred = True
        print(f"🔥 스크립트 실행 중 예상치 못한 오류 발생: {e}")
        error_details_str = traceback.format_exc()
    finally:
        end_run_time = time.time()
        elapsed_time = end_run_time - start_run_time

        if error_occurred:
            # 실패 메시지 생성 (오류 내용 포함)
            final_message = f"🔥 `{SCRIPT_NAME}` 실행 실패 (소요 시간: {elapsed_time:.2f}초)\n```\n{error_details_str[-1000:]}\n```"
        else:
            # 성공 메시지 생성 (단순화)
            final_message = f"✅ `{SCRIPT_NAME}` 실행 성공 (소요 시간: {elapsed_time:.2f}초)"

        # 최종 결과 알림
        if final_message:
            # print(f"\n📢 텔레그램 알림 발송: {final_message[:100]}...") # 로그 간소화
            telegram_utils.send_telegram_message(final_message)
        else:
            default_msg = f"ℹ️ `{SCRIPT_NAME}` 실행 완료되었으나 최종 상태 메시지 없음."
            print(default_msg)
            telegram_utils.send_telegram_message(default_msg)