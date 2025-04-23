# -*- coding: utf-8 -*-
# daily_batch.py: 매일 모든 계좌의 잔고 및 비중을 집계하여 구글 시트에 기록
# (IRP 종목코드 키 수정, 디버깅 포함)

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, date
import time
import traceback
import os
import sys

# 공휴일 처리
try:
    import holidays
except ImportError:
    print("⚠️ 'holidays' 라이브러리가 설치되지 않았습니다.")
    holidays = None

# --- API 모듈 임포트 ---
import kiwoom_auth_isa as kiwoom_auth
import kiwoom_domstk_isa as kiwoom_api
import kis_auth_pension as kis_auth_pen
import kis_domstk_pension as kis_api_pen
import kis_auth_irp as kis_auth_irp
import kis_domstk_irp as kis_api_irp
# --- ---

# --- 텔레그램 유틸리티 임포트 ---
try:
    import telegram_utils # 또는 from telegram_utils import send_telegram_message
except ModuleNotFoundError:
    print("⚠️ telegram_utils.py 모듈을 찾을 수 없습니다. 텔레그램 알림이 비활성화됩니다.")
    class MockTelegramUtils:
        def send_telegram_message(self, message):
            print("INFO: telegram_utils 모듈 없음 - 텔레그램 메시지 발송 건너<0xEB><0x81><0x91:", message[:100])
    telegram_utils = MockTelegramUtils()


# --- 설정 ---
GOOGLE_SHEET_NAME = 'KYI_자산배분'
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_KEYFILE_PATH = os.path.join(CURRENT_DIR, 'stock-auto-writer-44eaa06c140c.json')
BALANCE_RAW_SHEET = '일별잔고_Raw'
WEIGHTS_RAW_SHEET = '일별비중_Raw'
GOLD_SHEET = '📈금현물 수익률'
SETTINGS_SHEET = '⚙️설정'
BALANCE_HEADER = ['날짜', '계좌명', '총자산']
WEIGHTS_HEADER = ['날짜', '계좌명', '종목코드', '종목명', '자산구분', '국적', '평가금액', '포트폴리오내비중(%)']
ACCOUNTS = {
    '한투연금': {'auth': kis_auth_pen, 'api': kis_api_pen, 'type': 'KIS_PEN'},
    '한투IRP': {'auth': kis_auth_irp, 'api': kis_api_irp, 'type': 'KIS_IRP'},
    '키움ISA': {'auth': kiwoom_auth, 'api': kiwoom_api, 'type': 'KIWOOM_ISA'},
    '금현물': {'auth': None, 'api': None, 'type': 'GOLD'}
}
SCRIPT_NAME = os.path.basename(__file__)
# --- ---

# --- 유틸리티 함수 ---
def clean_num_str(num_str, type_func=int):
    if isinstance(num_str, (int, float)): return num_str
    if not num_str: return type_func(0)
    try:
        cleaned_str = str(num_str).replace(',', '')
        is_negative = cleaned_str.startswith('-')
        cleaned = cleaned_str.lstrip('-').lstrip('0')
        if not cleaned: return type_func(0)
        value = type_func(cleaned)
        return -value if is_negative else value
    except (ValueError, TypeError): return type_func(0)

def setup_google_sheet(sheet_name, worksheet_name, header_columns):
    worksheet = None
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE_PATH, scope)
        gc = gspread.authorize(credentials); spreadsheet = gc.open(sheet_name)
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            print(f"✅ Google Sheet '{sheet_name}/{worksheet_name}' 열기 성공.")
            header = []
            try: header = worksheet.row_values(1)
            except gspread.exceptions.APIError as e_api: print(f"⚠️ 헤더 읽기 중 API 오류: {e_api}"); return None
            if not header or len(header) < len(header_columns) or header[:len(header_columns)] != header_columns:
                 print(f"⚠️ '{worksheet_name}' 헤더가 비어있거나 다릅니다. 업데이트 필요 감지.")
                 all_values = []
                 try: all_values = worksheet.get_all_values()
                 except gspread.exceptions.APIError as e_get_all: print(f"⚠️ 시트 전체 값 읽기 중 API 오류: {e_get_all}"); return None
                 if not all_values: worksheet.append_row(header_columns, value_input_option='USER_ENTERED'); print("✅ 비어있는 시트에 헤더 추가 완료.")
                 else:
                     try:
                         worksheet.update(range_name='A1', values=[header_columns], value_input_option='USER_ENTERED')
                         print(f"✅ 헤더 업데이트 완료 ({worksheet_name}).")
                     except Exception as e_header: print(f"❗️ 헤더 자동 업데이트 실패: {e_header}.")
        except gspread.exceptions.WorksheetNotFound:
            print(f"⚠️ 워크시트 '{worksheet_name}' 생성 및 헤더 추가."); worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols=len(header_columns))
            worksheet.append_row(header_columns, value_input_option='USER_ENTERED')
        return worksheet
    except FileNotFoundError: print(f"❌ 오류: 키 파일({JSON_KEYFILE_PATH}) 없음."); return None
    except gspread.exceptions.APIError as e_conn: print(f"❌ 구글 시트 연결 중 API 오류: {e_conn}"); return None
    except Exception as e: print(f"❌ 시트 연결/설정 오류: {e}"); traceback.print_exc(); return None
# --- ---

# --- 메인 실행 로직 ---
def main():
    start_time = time.time()
    print("🚀 일별 잔고 및 비중 기록 배치 시작")
    # 0. 대상 날짜 결정
    today = datetime.now().date(); target_date_dt = today - timedelta(days=1); kr_holidays = {}
    if holidays:
        try: kr_holidays = holidays.KR(years=target_date_dt.year, observed=True)
        except Exception as e_holiday: print(f"⚠️ 공휴일 정보 로드 오류: {e_holiday}")
    days_to_check = 0
    while days_to_check < 5:
        is_holiday = target_date_dt in kr_holidays if holidays else False
        if target_date_dt.weekday() < 5 and not is_holiday: break
        target_date_dt -= timedelta(days=1); days_to_check += 1
    target_date_str = target_date_dt.strftime("%Y-%m-%d"); target_date_yyyymmdd = target_date_dt.strftime("%Y%m%d")
    print(f"🎯 대상 날짜 (영업일 기준): {target_date_str}")

    # 1. API 인증
    print("\n[인증] 모든 증권사 API 인증 시도...")
    auth_success_map = {}; all_auth_successful = True
    for acc_name, acc_info in ACCOUNTS.items():
        auth_success_map[acc_name] = False
        if acc_info['auth']:
            try:
                auth_function_name = 'auth' if acc_info['type'].startswith('KIS') else 'authenticate'
                auth_func = getattr(acc_info['auth'], auth_function_name, None)
                if auth_func and callable(auth_func):
                    auth_result = auth_func()
                    if auth_result is True: auth_success_map[acc_name] = True; print(f"  > {acc_name} 인증 성공.")
                    elif auth_result is False: print(f"🔥 {acc_name} 인증 실패 (함수 반환값 False)."); all_auth_successful = False
                    else: print(f"🔥 {acc_name} 인증 실패: '{auth_function_name}' 함수가 True/False를 명시적으로 반환하지 않음 (반환값: {auth_result})."); all_auth_successful = False
                else: print(f"🔥 {acc_name} 인증 실패: '{auth_function_name}' 함수 없음."); all_auth_successful = False
            except Exception as e_auth: print(f"🔥 {acc_name} 인증 중 오류: {e_auth}"); traceback.print_exc(); all_auth_successful = False
        else: auth_success_map[acc_name] = True; print(f"  > {acc_name} 인증 불필요.")
    if not all_auth_successful: print("⚠️ 일부 계좌 인증 실패.")

    # 2. 구글 시트 연결
    print("\n[준비] 구글 시트 연결 및 Raw 시트 확인/생성...")
    balance_ws = setup_google_sheet(GOOGLE_SHEET_NAME, BALANCE_RAW_SHEET, BALANCE_HEADER)
    weights_ws = setup_google_sheet(GOOGLE_SHEET_NAME, WEIGHTS_RAW_SHEET, WEIGHTS_HEADER)
    gold_ws = None; settings_ws = None
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE_PATH, scope)
        gc = gspread.authorize(credentials); spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        gold_ws = spreadsheet.worksheet(GOLD_SHEET); settings_ws = spreadsheet.worksheet(SETTINGS_SHEET)
        print(f"✅ 읽기용 시트 ({GOLD_SHEET}, {SETTINGS_SHEET}) 열기 성공.")
    except gspread.exceptions.APIError as e_read_ws: raise ConnectionError(f"❌ 읽기용 구글 시트 열기 중 API 오류: {e_read_ws}") from e_read_ws
    except Exception as e: raise ConnectionError(f"❌ 읽기용 시트 열기 실패: {e}") from e
    if not balance_ws or not weights_ws or not gold_ws or not settings_ws: raise ConnectionError("🔥 필요 시트 준비 실패. 종료합니다.")

    # 3. 기존 Raw 데이터 확인
    existing_balances = {}; existing_weights = set()
    print(f"\n[확인] {target_date_str} 기준 기존 Raw 데이터 확인...")
    try:
        balance_data = balance_ws.get_all_records(expected_headers=BALANCE_HEADER)
        for row in balance_data:
            if str(row.get('날짜')).strip() == target_date_str and row.get('계좌명'): existing_balances[str(row['계좌명']).strip()] = row['총자산']
        print(f"✅ '{BALANCE_RAW_SHEET}' 확인: {len(existing_balances)}개 계좌 데이터 존재.")
    except Exception as e: print(f"⚠️ '{BALANCE_RAW_SHEET}' 읽기 오류: {e}")
    try:
        weights_data = weights_ws.get_all_records(expected_headers=WEIGHTS_HEADER)
        for row in weights_data:
            if str(row.get('날짜')).strip() == target_date_str and row.get('계좌명') and row.get('종목코드'):
                key = (str(row['계좌명']).strip(), str(row['종목코드']).strip())
                existing_weights.add(key)
        print(f"✅ '{WEIGHTS_RAW_SHEET}' 확인: {len(existing_weights)}개 비중 데이터 존재.")
    except Exception as e: print(f"⚠️ '{WEIGHTS_RAW_SHEET}' 읽기 오류: {e}")

    # 4. 일별 잔고 조회 및 기록 준비
    print(f"\n[잔고 조회/기록] {target_date_str} 기준 시작...")
    daily_balances_to_add = []; account_balances = {}; holding_api_results = {}
    for acc_name, acc_info in ACCOUNTS.items():
        balance = 0
        was_already_in_sheet = acc_name in existing_balances
        if was_already_in_sheet: balance = clean_num_str(existing_balances[acc_name]); print(f"  > {acc_name}: 기존 잔고 데이터 사용 ({balance:,} 원)")
        else: print(f"  > {acc_name}: 기존 데이터 없음. 조회/읽기 필요.")
        if acc_info['type'] != 'GOLD' and not auth_success_map.get(acc_name, False):
            print(f"  > {acc_name}: 인증 실패 또는 확인 불가, 잔고 0 처리 및 건너<0xEB><0x81><0x91.")
            balance = 0; holding_api_results[acc_name] = None
        else:
            api_call_needed = (acc_info['type'] != 'GOLD')
            print(f"  > {acc_name}: 잔고 및 보유 현황 조회/읽기 시도 ({'신규' if not was_already_in_sheet else '기존 잔고 있으나 Holdings 확인'}) ...")
            if acc_info['type'] == 'KIWOOM_ISA':
                kiwoom_bal_result = None; kiwoom_holding_result = None
                try: kiwoom_bal_result = kiwoom_api.get_daily_account_profit_loss(target_date_yyyymmdd, target_date_yyyymmdd)
                except Exception as e_kw_bal: print(f"    - API(kt00016) 호출 오류: {e_kw_bal}")
                try: kiwoom_holding_result = kiwoom_api.get_account_evaluation_balance()
                except Exception as e_kw_hold: print(f"    - API(kt00018) 호출 오류: {e_kw_hold}")
                holding_api_results[acc_name] = kiwoom_holding_result
                if kiwoom_bal_result and kiwoom_bal_result.get('success'): balance = clean_num_str(kiwoom_bal_result['data'].get('tot_amt_to', '0')); print(f"    - API(kt00016) 조회 성공: {balance:,} 원")
                else:
                    print(f"    - API(kt00016) 조회 실패 또는 오류 응답.")
                    if kiwoom_holding_result and kiwoom_holding_result.get('success'): balance = clean_num_str(kiwoom_holding_result['data'].get('tot_evlt_amt', '0')); print(f"    - API(kt00018)의 총평가금액으로 대체: {balance:,} 원 (예수금 확인 필요)")
                    else: print(f"    - API(kt00018) 조회도 실패하여 잔고 0 처리.")
            elif acc_info['type'] == 'KIS_PEN':
                pen_bal_result = None
                try: pen_bal_result = kis_api_pen.get_inquire_balance_obj()
                except Exception as e_pen_bal: print(f"    - API(TTTC8434R) 호출 오류: {e_pen_bal}")
                holding_api_results[acc_name] = pen_bal_result
                if pen_bal_result and pen_bal_result.get("rt_cd") == "0": balance = clean_num_str(pen_bal_result.get('output2', [{}])[0].get('tot_evlu_amt', '0')); print(f"    - API(TTTC8434R) 조회 성공: {balance:,} 원")
                else: print(f"    - API(TTTC8434R) 조회 실패 또는 오류 응답.")
            elif acc_info['type'] == 'KIS_IRP':
                df_irp_holdings_bal = None
                try: df_irp_holdings_bal = kis_api_irp.get_inquire_present_balance_irp()
                except Exception as e_irp_bal: print(f"    - API(TTTC2202R) 호출 오류: {e_irp_bal}")
                holding_api_results[acc_name] = df_irp_holdings_bal
                if isinstance(df_irp_holdings_bal, pd.DataFrame) and not df_irp_holdings_bal.empty and 'evlu_amt' in df_irp_holdings_bal.columns:
                    try:
                        df_irp_holdings_bal['evlu_amt_num'] = df_irp_holdings_bal['evlu_amt'].apply(lambda x: clean_num_str(x, int))
                        balance = df_irp_holdings_bal['evlu_amt_num'].sum(); print(f"    - API(TTTC2202R) 조회 성공 (보유 종목 평가액 합계): {balance:,} 원"); print(f"    ⚠️ IRP 예수금 확인 필요.")
                    except Exception as e_irp_sum: print(f"    - 잔고 계산 오류: {e_irp_sum}")
                else: print(f"    - API(TTTC2202R) 조회 실패 또는 빈 결과.")
            elif acc_info['type'] == 'GOLD':
                api_call_needed = False
                try:
                    gold_data = gold_ws.get_all_records(expected_headers=['날짜', '평가액']); df_gold = pd.DataFrame(gold_data)
                    if not df_gold.empty:
                        gold_row = df_gold[df_gold['날짜'] == target_date_str]
                        if not gold_row.empty: balance = clean_num_str(gold_row.iloc[0]['평가액'])
                        else: print(f"    - 시트에서 {target_date_str} 데이터 없음.")
                    else: print(f"    - {GOLD_SHEET} 시트 데이터 없음.")
                    print(f"    - 시트 읽기 값: {balance:,} 원")
                except Exception as e_gold: print(f"    - 시트 읽기 오류: {e_gold}")
            if api_call_needed: time.sleep(0.21)
        try: python_balance_value = int(float(balance))
        except (ValueError, TypeError): python_balance_value = 0
        account_balances[acc_name] = python_balance_value
        if not was_already_in_sheet and python_balance_value >= 0:
            daily_balances_to_add.append([target_date_str, acc_name, python_balance_value])
            print(f"    ➡️ '{acc_name}' 잔고({python_balance_value:,}) 추가 예정 (신규)")
        elif was_already_in_sheet: print(f"    ➡️ '{acc_name}' 잔고는 이미 시트에 존재하여 추가하지 않음")

    # 4-5. 일별 잔고 시트 기록
    if daily_balances_to_add:
        print(f"\n💾 '{BALANCE_RAW_SHEET}' 시트에 {len(daily_balances_to_add)} 건의 신규 잔고 데이터 추가 시도...")
        try: balance_ws.append_rows(daily_balances_to_add, value_input_option='USER_ENTERED'); print("✅ 잔고 데이터 추가 완료!")
        except Exception as e: raise IOError(f"❌ 잔고 데이터 추가 오류: {e}") from e
    else: print(f"\nℹ️ '{BALANCE_RAW_SHEET}' 시트에 추가할 신규 잔고 데이터 없음.")

    # 5. 보유 종목 조회 및 비중 계산/기록 준비
    print(f"\n[비중 계산] {target_date_str} 기준 보유 비중 계산 시작...")
    all_holdings_data = []; total_portfolio_value = sum(v for v in account_balances.values() if v is not None and v >= 0)
    print(f"  > 전체 포트폴리오 가치 (계산 기준): {total_portfolio_value:,} 원")
    if total_portfolio_value <= 0: print("⚠️ 전체 포트폴리오 가치가 0 이하이므로 비중 계산 불가.")
    else:
        # 5-1. 설정 시트 매핑 정보 읽기 ('국적' 헤더 사용)
        asset_map = {}; settings_map_success = False
        try:
            print("  > 설정 시트 데이터 읽는 중..."); settings_values = settings_ws.get_all_values()
            if len(settings_values) > 1:
                header = settings_values[0]
                try:
                    required_cols = ['종목코드', '종목명', '구분', '국적']
                    col_indices = {}
                    missing_cols = []
                    for col in required_cols:
                        try: col_indices[col] = header.index(col)
                        except ValueError: missing_cols.append(col)
                    if missing_cols: raise ValueError(f"설정 시트 헤더 구성 오류: {missing_cols} 누락")
                    code_col, name_col, class_col, nation_col = col_indices['종목코드'], col_indices['종목명'], col_indices['구분'], col_indices['국적']
                    processed_codes_map = {}
                    for i, row in enumerate(settings_values[1:]):
                        max_col_needed = max(code_col, name_col, class_col, nation_col)
                        if len(row) > max_col_needed:
                            try:
                                code_raw = str(row[code_col]).strip()
                                if code_raw:
                                    code_clean = code_raw.split(':')[-1].strip()
                                    asset_class = str(row[class_col]).strip(); nationality = str(row[nation_col]).strip()
                                    name_raw = str(row[name_col]).strip()
                                    if asset_class and nationality:
                                        map_value = {'종목명': name_raw, '자산구분': asset_class, '국적': nationality}
                                        if code_clean.isdigit() and len(code_clean) == 6: processed_codes_map[code_clean] = map_value; processed_codes_map['A' + code_clean] = map_value
                                        elif code_clean == 'GOLD': processed_codes_map['GOLD'] = map_value
                            except Exception as e_row: print(f"    > 설정 {i+2}행 처리 오류: {e_row}")
                    asset_map = processed_codes_map
                    if asset_map: print(f"✅ 설정 시트 {len(asset_map)}개 키-값 매핑 로드 완료."); settings_map_success = True
                    else: print("❌ 설정 시트 유효 매핑 정보 로드 실패.")
                except ValueError as e_col: print(f"❌ 설정 시트 헤더 오류: {e_col}"); raise e_col
            else: print("❌ 설정 시트 데이터 없음 (헤더 제외).")
        except Exception as e_setting: print(f"❌ 설정 시트 읽기/처리 오류: {e_setting}."); traceback.print_exc()
        if not settings_map_success: raise ValueError("설정 시트 로드 실패로 비중 계산 불가")

        # 5-2. 계좌별 보유 종목 조회 및 통합 (디버깅 프린트 추가, IRP 코드 키 수정)
        print("  > 계좌별 보유 종목 통합 중...")
        for acc_name, result in holding_api_results.items():
            print(f"  Processing account: {acc_name}") # 계좌 처리 시작 로그
            if not auth_success_map.get(acc_name, False):
                print(f"    Skipping {acc_name} due to auth failure.")
                continue
            is_result_valid = False
            if isinstance(result, pd.DataFrame): is_result_valid = not result.empty
            elif isinstance(result, dict): is_result_valid = result.get("rt_cd") == "0" or result.get("success") is True
            elif result is None: is_result_valid = False
            print(f"    Result type: {type(result)}, Is valid: {is_result_valid}") # 결과 유효성 로그
            if isinstance(result, pd.DataFrame) and not result.empty: print(f"    DataFrame columns: {result.columns.tolist()}") # DF 컬럼 확인
            elif isinstance(result, dict): print(f"    Dict rt_cd/success: {result.get('rt_cd')}/{result.get('success')}") # Dict 성공 여부 확인

            if not is_result_valid:
                 print(f"    Skipping {acc_name} due to invalid API result.")
                 continue

            acc_type = ACCOUNTS[acc_name]['type']; stock_list = []
            if acc_type == 'KIWOOM_ISA' and isinstance(result, dict) and result.get('success'): stock_list = result['data'].get('acnt_evlt_remn_indv_tot', [])
            elif acc_type == 'KIS_PEN' and isinstance(result, dict) and result.get("rt_cd") == "0": stock_list = result.get('output1', [])
            elif acc_type == 'KIS_IRP' and isinstance(result, pd.DataFrame) and not result.empty:
                try:
                     stock_list = result.to_dict('records')
                     print(f"    DEBUG: 한투IRP stock_list (처음 2개): {stock_list[:2]}") # IRP stock_list 확인
                except Exception as e_todict:
                     print(f"    ERROR converting IRP DataFrame to dict: {e_todict}"); stock_list = []

            print(f"    Stock list length for {acc_name}: {len(stock_list)}") # 리스트 길이 확인

            if stock_list:
                 print(f"    > {acc_name}: {len(stock_list)}개 종목 처리 시작")
                 items_added_for_account = 0
                 for item in stock_list:
                     code = '' ; name = '' ; eval_amt = 0
                     if acc_type == 'KIWOOM_ISA': code_raw = item.get('stk_cd', ''); name = item.get('stk_nm', ''); code = code_raw ; eval_amt = clean_num_str(item.get('evlt_amt', '0'))
                     elif acc_type == 'KIS_PEN': code = item.get('pdno', ''); name = item.get('prdt_name', ''); eval_amt = clean_num_str(item.get('evlu_amt', '0'))
                     elif acc_type == 'KIS_IRP':
                         # *** 수정: 'prdt_cd' -> 'pdno' 로 키 이름 변경 ***
                         code = item.get('pdno', '') # 여기가 수정됨!
                         # *****************************************
                         name = item.get('prdt_name', '')
                         eval_amt = clean_num_str(item.get('evlu_amt_num', item.get('evlu_amt', '0')))

                     print(f"      DEBUG {acc_name} Item: code='{code}', name='{name}', eval_amt={eval_amt}") # 개별 항목 정보

                     if eval_amt > 0 and code: # 코드가 비어있지 않은지 확인
                          all_holdings_data.append({'날짜': target_date_str, '계좌명': acc_name, '종목코드': code.strip(), '종목명': name.strip(), '평가금액': eval_amt})
                          items_added_for_account += 1
                          print(f"        DEBUG: Appended to all_holdings_data: {all_holdings_data[-1]}") # 추가 확인
                     else:
                          print(f"        DEBUG: Skipped item (eval_amt<=0 or no code): code='{code}', eval_amt={eval_amt}") # 건너<0xEB><0x81><0x90> 확인

                 print(f"    > {acc_name}: {items_added_for_account}개 종목 all_holdings_data에 추가 완료")

        # 금현물 추가
        acc_name = '금현물'; gold_value = account_balances.get(acc_name, 0)
        if gold_value > 0: all_holdings_data.append({'날짜': target_date_str, '계좌명': acc_name, '종목코드': 'GOLD', '종목명': '금현물', '평가금액': gold_value})

        # 5-3. 비중 계산 및 최종 데이터 준비 (디버깅 프린트 추가)
        weights_rows_to_add = []
        if all_holdings_data:
             print(f"  > 총 {len(all_holdings_data)} 건 보유 내역 통합. 비중 계산 및 매핑 시작...")
             for holding in all_holdings_data:
                 eval_amount = holding['평가금액']; code_orig = holding['종목코드']
                 acc_name_h = holding['계좌명']; name_h = holding['종목명']
                 current_key = (acc_name_h, code_orig)
                 if current_key in existing_weights:
                      print(f"    Skipping weight calculation (already exists): {current_key}") # 중복 로그 추가
                      continue

                 if acc_name_h == '한투IRP': print(f"    DEBUG Processing IRP Holding for weight: {holding}") # 비중 계산 단계 확인

                 weight = (eval_amount / total_portfolio_value * 100) if total_portfolio_value else 0
                 map_info = asset_map.get(code_orig, None)
                 if not map_info:
                      alt_code = 'A' + code_orig if code_orig.isdigit() and len(code_orig) == 6 else (code_orig[1:] if code_orig.startswith('A') and code_orig[1:].isdigit() else None)
                      if alt_code: map_info = asset_map.get(alt_code, None)

                 if acc_name_h == '한투IRP': print(f"      DEBUG Map Info for {code_orig}: {map_info}") # 매핑 결과 확인

                 asset_class = '미분류'; nationality = '미분류'
                 if map_info:
                      asset_class = map_info.get('자산구분', '미분류')
                      nationality = map_info.get('국적', '미분류')
                 elif code_orig != 'GOLD': print(f"    ⚠️ 매핑 정보 없음: 코드='{code_orig}', 종목명='{name_h}'. '미분류' 처리.")
                 elif code_orig == 'GOLD': map_info_gold = asset_map.get('GOLD', {}); asset_class = map_info_gold.get('자산구분', '대체투자'); nationality = map_info_gold.get('국적', '기타')

                 try: python_eval_amount = int(float(eval_amount))
                 except (ValueError, TypeError): python_eval_amount = 0
                 try: python_weight = float(weight)
                 except (ValueError, TypeError): python_weight = 0.0

                 weights_rows_to_add.append([holding['날짜'], acc_name_h, code_orig, name_h, asset_class, nationality, python_eval_amount, round(python_weight, 2)])

                 if acc_name_h == '한투IRP': print(f"        DEBUG Appended to weights_rows_to_add: {weights_rows_to_add[-1]}") # 최종 추가 확인
        else: print("  > 비중 계산할 통합 보유 내역 없음.")

        # 5-4. 일별 비중 시트 기록
        if weights_rows_to_add:
            print(f"\n💾 '{WEIGHTS_RAW_SHEET}' 시트에 {len(weights_rows_to_add)} 건의 비중 데이터 추가 시도...")
            try: weights_ws.append_rows(weights_rows_to_add, value_input_option='USER_ENTERED'); print("✅ 비중 데이터 추가 완료!")
            except Exception as e: raise IOError(f"❌ 비중 데이터 추가 오류: {e}") from e
        else: print(f"\nℹ️ '{WEIGHTS_RAW_SHEET}' 시트에 추가할 신규 비중 데이터 없음.")

    # main 함수 성공 메시지 반환
    new_balance_count = len(daily_balances_to_add)
    new_weight_count = len(weights_rows_to_add) if 'weights_rows_to_add' in locals() else 0
    elapsed_time = time.time() - start_time
    return f"✅ `{SCRIPT_NAME}` 실행 완료 (대상: {target_date_str}, 신규 잔고: {new_balance_count}건, 신규 비중: {new_weight_count}건, 소요 시간: {elapsed_time:.2f}초)"
# --- ---

# --- 스크립트 실행 및 텔레그램 알림 ---
if __name__ == '__main__':
    start_run_time = time.time()
    final_message = ""
    error_occurred = False
    error_details_str = ""
    try:
        success_message = main()
        final_message = success_message if success_message else f"✅ `{SCRIPT_NAME}` 실행 완료"
    except ConnectionError as e: error_occurred = True; print(f"🔥 연결 오류: {e}"); error_details_str = traceback.format_exc()
    except IOError as e: error_occurred = True; print(f"🔥 IO 오류: {e}"); error_details_str = traceback.format_exc()
    except ValueError as e: error_occurred = True; print(f"🔥 값 오류: {e}"); error_details_str = traceback.format_exc()
    except Exception as e: error_occurred = True; print(f"🔥 예상치 못한 오류: {e}"); error_details_str = traceback.format_exc()
    finally:
        end_run_time = time.time(); elapsed_time = end_run_time - start_run_time
        if error_occurred: final_message = f"🔥 `{SCRIPT_NAME}` 실행 실패 (소요 시간: {elapsed_time:.2f}초)\n```\n{error_details_str[-1000:]}\n```"
        else:
             if not final_message: final_message = f"✅ `{SCRIPT_NAME}` 실행 성공 (소요 시간: {elapsed_time:.2f}초)"
        if final_message: telegram_utils.send_telegram_message(final_message)
        else: default_msg = f"ℹ️ `{SCRIPT_NAME}` 실행 완료 상태 메시지 없음."; print(default_msg); telegram_utils.send_telegram_message(default_msg)