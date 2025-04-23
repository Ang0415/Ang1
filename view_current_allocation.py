# -*- coding: utf-8 -*-
# view_current_allocation.py: 현재 시점의 자산 배분 비율을 사용자가 정의한 상세 분류 기준으로 조회/비교

import time
import traceback
import os
import sys
from datetime import datetime
import pandas as pd

# 외부 라이브러리 임포트
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
except ImportError:
    print("오류: 'gspread' 또는 'oauth2client' 라이브러리가 필요합니다.")
    print("설치: pip install gspread oauth2client")
    sys.exit(1)

# --- API 모듈 임포트 ---
try:
    import kiwoom_auth_isa as kiwoom_auth
    import kiwoom_domstk_isa as kiwoom_api
    import kis_auth_pension as kis_auth_pen
    import kis_domstk_pension as kis_api_pen
    import kis_auth_irp as kis_auth_irp
    import kis_domstk_irp as kis_api_irp
except ModuleNotFoundError as e:
     print(f"오류: 필요한 API 모듈을 찾을 수 없습니다 - {e}")
     sys.exit(1)
# --- ---

# --- 설정 ---
GOOGLE_SHEET_NAME = 'KYI_자산배분'
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_KEYFILE_PATH = os.path.join(CURRENT_DIR, 'stock-auto-writer-44eaa06c140c.json')
GOLD_SHEET = '📈금현물 수익률'
SETTINGS_SHEET = '⚙️설정'

ACCOUNTS = {
    '한투연금': {'auth': kis_auth_pen, 'api': kis_api_pen, 'type': 'KIS_PEN'},
    '한투IRP': {'auth': kis_auth_irp, 'api': kis_api_irp, 'type': 'KIS_IRP'},
    '키움ISA': {'auth': kiwoom_auth, 'api': kiwoom_api, 'type': 'KIWOOM_ISA'},
    '금현물': {'auth': None, 'api': None, 'type': 'GOLD'}
}
# --- ---

# --- 유틸리티 함수 ---
def clean_num_str(num_str, type_func=int):
    """숫자 변환 함수"""
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

def connect_google_sheets():
    """구글 시트 연결 객체 반환"""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE_PATH, scope)
        gc = gspread.authorize(credentials)
        return gc
    except FileNotFoundError: print(f"❌ 오류: 키 파일({JSON_KEYFILE_PATH}) 없음."); return None
    except Exception as e: print(f"❌ 구글 시트 연결 오류: {e}"); traceback.print_exc(); return None

# --- 메인 실행 로직 ---
def main():
    start_time = time.time()
    print(f"--- 현재 자산 배분 비율 조회 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---")

    # 1. API 인증
    print("\n[1단계] API 인증 시도...")
    auth_success_map = {}
    all_auth_successful = True
    for acc_name, acc_info in ACCOUNTS.items():
        if acc_info['type'] == 'GOLD': auth_success_map[acc_name] = True; continue
        auth_success_map[acc_name] = False
        if acc_info['auth']:
            try:
                auth_function_name = 'auth' if acc_info['type'].startswith('KIS') else 'authenticate'
                auth_func = getattr(acc_info['auth'], auth_function_name, None)
                if auth_func and callable(auth_func):
                    if auth_func(): auth_success_map[acc_name] = True; print(f"  ✅ {acc_name} 인증 성공.")
                    else: print(f"  🔥 {acc_name} 인증 실패."); all_auth_successful = False
                else: print(f"  🔥 {acc_name} 인증 실패: 함수 없음."); all_auth_successful = False
            except Exception as e_auth: print(f"  🔥 {acc_name} 인증 오류: {e_auth}"); traceback.print_exc(); all_auth_successful = False
        else: print(f"  ⚠️ {acc_name}: 인증 모듈 정보 없음."); all_auth_successful = False
    if not all_auth_successful: print("\n⚠️ 일부 계좌 인증 실패.")

    # 2. 데이터 조회
    print("\n[2단계] 계좌별 현재 데이터 조회...")
    api_results = {}; account_current_balances = {}
    for acc_name, acc_info in ACCOUNTS.items():
        if acc_info['type'] == 'GOLD' or not auth_success_map.get(acc_name): continue
        print(f"  > {acc_name}: 현재 잔고/보유 현황 조회 중..."); current_result = None; current_balance = 0
        try:
            if acc_info['type'] == 'KIWOOM_ISA':
                current_result = kiwoom_api.get_account_evaluation_balance() # kt00018
                if current_result and current_result.get('success'): current_balance = clean_num_str(current_result['data'].get('tot_evlt_amt', '0')); print(f"    - API(kt00018) 성공. 평가액: {current_balance:,} 원")
                else: print(f"    - API(kt00018) 실패.")
            elif acc_info['type'] == 'KIS_PEN':
                current_result = kis_api_pen.get_inquire_balance_obj() # TTTC8434R
                if current_result and current_result.get("rt_cd") == "0": current_balance = clean_num_str(current_result.get('output2', [{}])[0].get('tot_evlu_amt', '0')); print(f"    - API(TTTC8434R) 성공. 평가액: {current_balance:,} 원")
                else: print(f"    - API(TTTC8434R) 실패.")
            elif acc_info['type'] == 'KIS_IRP':
                current_result = kis_api_irp.get_inquire_present_balance_irp() # TTTC2202R (DataFrame)
                if isinstance(current_result, pd.DataFrame) and not current_result.empty and 'evlu_amt' in current_result.columns:
                    current_result['evlu_amt_num'] = current_result['evlu_amt'].apply(lambda x: clean_num_str(x, int))
                    current_balance = current_result['evlu_amt_num'].sum()
                    print(f"    - API(TTTC2202R) 성공. 보유종목 평가액 합계: {current_balance:,} 원")
                else: print(f"    - API(TTTC2202R) 실패.")
            api_results[acc_name] = current_result; account_current_balances[acc_name] = current_balance
            time.sleep(0.21)
        except Exception as e_fetch: print(f"  🔥 {acc_name} 조회 오류: {e_fetch}"); traceback.print_exc(); api_results[acc_name] = None; account_current_balances[acc_name] = 0

    # 금현물 조회
    print(f"  > 금현물: 최신 평가액 조회 중..."); gold_balance = 0; gc = connect_google_sheets()
    if gc:
        try:
            spreadsheet = gc.open(GOOGLE_SHEET_NAME); gold_ws = spreadsheet.worksheet(GOLD_SHEET)
            gold_data = gold_ws.get_all_records(expected_headers=['날짜', '평가액'])
            if gold_data:
                df_gold = pd.DataFrame(gold_data); df_gold['날짜_dt'] = pd.to_datetime(df_gold['날짜'], errors='coerce')
                latest_gold_row = df_gold.loc[df_gold['날짜_dt'].idxmax()]
                if pd.notna(latest_gold_row['날짜_dt']):
                    gold_balance = clean_num_str(latest_gold_row['평가액']); latest_date_str = latest_gold_row['날짜_dt'].strftime('%Y-%m-%d')
                    print(f"    - 시트 성공. 최신 평가액 ({latest_date_str}): {gold_balance:,} 원")
                else: print("    - 시트 유효 날짜 데이터 없음.")
            else: print(f"    - 시트 '{GOLD_SHEET}' 데이터 없음.")
        except Exception as e_gold: print(f"    - 금현물 시트 오류: {e_gold}")
    else: print("    - 구글 시트 연결 실패.")
    account_current_balances['금현물'] = gold_balance

    # 3. 설정 정보 읽기 (매핑 + 목표 비중 - '종합 분류' 기준)
    print("\n[3단계] 설정 정보 (매핑 + 목표 비중) 로드...")
    asset_map = {}; target_allocation_combined = {} # 목표 비중: {(자산구분, 국적구분): 목표%} 형태
    if gc:
        try:
            if not 'spreadsheet' in locals(): spreadsheet = gc.open(GOOGLE_SHEET_NAME)
            settings_ws = spreadsheet.worksheet(SETTINGS_SHEET); settings_values = settings_ws.get_all_values()
            if len(settings_values) > 1:
                header = settings_values[0]
                col_idx = {'종목코드': 17, '종목명': 16, '구분': 18, '국적구분': 19, '목표비중': 22}
                max_col_idx = max(col_idx.values())
                if max_col_idx >= len(header): print(f"❌ 설정 시트 컬럼 인덱스 오류.")
                else:
                    print(f"    > 사용할 컬럼 인덱스: {col_idx}")
                    processed_codes_map = {}; processed_targets_combined = {}
                    unique_target_keys = set() # 목표 비중 중복 정의 방지용

                    for i, row in enumerate(settings_values[1:]):
                        if len(row) > max_col_idx:
                            try:
                                # 매핑 정보 처리
                                code_raw = str(row[col_idx['종목코드']]).strip()
                                asset_class = str(row[col_idx['구분']]).strip()
                                nationality = str(row[col_idx['국적구분']]).strip()
                                if code_raw and asset_class and nationality: # 코드, 구분, 국적 모두 있어야 유효
                                    code_clean = code_raw.split(':')[-1].strip()
                                    map_value = {'종목명': str(row[col_idx['종목명']]).strip(), '자산구분': asset_class, '국적구분': nationality}
                                    if code_clean.isdigit() and len(code_clean) == 6: processed_codes_map[code_clean] = map_value; processed_codes_map['A' + code_clean] = map_value
                                    elif code_clean == 'GOLD': processed_codes_map['GOLD'] = map_value

                                    # 목표 비중 처리 (종합 분류 기준)
                                    target_perc_str = str(row[col_idx['목표비중']]).strip().replace('%','')
                                    # 자산구분과 국적구분을 조합한 키 생성
                                    combined_key = (asset_class, nationality)

                                    if combined_key and target_perc_str:
                                        # 이 조합의 목표 비중이 아직 설정되지 않았을 때만 저장
                                        if combined_key not in unique_target_keys:
                                             try:
                                                 target_perc = float(target_perc_str)
                                                 processed_targets_combined[combined_key] = target_perc
                                                 unique_target_keys.add(combined_key) # 처리된 키 기록
                                             except ValueError: pass # 숫자 변환 실패 시 무시
                            except Exception as e_row: print(f"    > 설정 {i+2}행 처리 오류: {e_row}")

                    asset_map = processed_codes_map
                    target_allocation_combined = processed_targets_combined
                    if asset_map: print(f"  ✅ 매핑 정보 {len(asset_map)}개 키-값 로드 완료.")
                    else: print("  ❌ 매핑 정보 로드 실패.")
                    if target_allocation_combined: print(f"  ✅ 종합 분류 목표 비중 {len(target_allocation_combined)}개 로드 완료: {target_allocation_combined}")
                    else: print("  ❌ 종합 분류 목표 비중 정보 로드 실패.")
            else: print("  ❌ 설정 시트 데이터 없음 (헤더 제외).")
        except Exception as e_setting: print(f"  ❌ 설정 시트 읽기/처리 오류: {e_setting}.")
    else: print("  ⚠️ 구글 시트 연결 실패.")


    # 4. 데이터 통합 및 비중 계산 ('종합 분류' 기준)
    print("\n[4단계] 데이터 통합 및 비중 계산...")
    all_holdings_data = []
    for acc_name, result in api_results.items():
        is_result_valid = False
        if isinstance(result, pd.DataFrame): is_result_valid = not result.empty
        elif isinstance(result, dict): is_result_valid = result.get("rt_cd") == "0" or result.get("success") is True
        if not is_result_valid: continue
        acc_type = ACCOUNTS[acc_name]['type']; stock_list = []
        if acc_type == 'KIWOOM_ISA' and isinstance(result, dict): stock_list = result['data'].get('acnt_evlt_remn_indv_tot', [])
        elif acc_type == 'KIS_PEN' and isinstance(result, dict): stock_list = result.get('output1', [])
        elif acc_type == 'KIS_IRP' and isinstance(result, pd.DataFrame): stock_list = result.to_dict('records')
        if stock_list:
             for item in stock_list:
                 code = '' ; name = '' ; eval_amt = 0
                 if acc_type == 'KIWOOM_ISA': code_raw = item.get('stk_cd', ''); name = item.get('stk_nm', ''); code = code_raw ; eval_amt = clean_num_str(item.get('evlt_amt', '0'))
                 elif acc_type == 'KIS_PEN': code = item.get('pdno', ''); name = item.get('prdt_name', ''); eval_amt = clean_num_str(item.get('evlu_amt', '0'))
                 elif acc_type == 'KIS_IRP': code = item.get('prdt_cd', ''); name = item.get('prdt_name', ''); eval_amt = clean_num_str(item.get('evlu_amt_num', '0'))
                 if eval_amt > 0 and code: all_holdings_data.append({'계좌명': acc_name, '종목코드': code.strip(), '종목명': name.strip(), '평가금액': eval_amt})
    if gold_balance > 0: all_holdings_data.append({'계좌명': '금현물', '종목코드': 'GOLD', '종목명': '금현물', '평가금액': gold_balance})

    total_portfolio_value = sum(v for v in account_current_balances.values() if v is not None and v >= 0)
    print(f"  📊 현재 총 포트폴리오 평가액: {total_portfolio_value:,.0f} 원")

    holdings_df = pd.DataFrame(all_holdings_data)
    summary_df = pd.DataFrame() # 최종 결과 저장용 DataFrame

    if total_portfolio_value > 0 and not holdings_df.empty:
        holdings_df['평가금액'] = pd.to_numeric(holdings_df['평가금액'], errors='coerce').fillna(0)
        holdings_df['비중(%)'] = (holdings_df['평가금액'] / total_portfolio_value * 100)

        def get_mapping_info(code): # 매핑 함수
            map_info = asset_map.get(code, None)
            if not map_info:
                alt_code = 'A' + code if code.isdigit() and len(code) == 6 else (code[1:] if code.startswith('A') and code[1:].isdigit() else None)
                if alt_code: map_info = asset_map.get(alt_code, None)
            if map_info: return map_info.get('자산구분', '미분류'), map_info.get('국적구분', '미분류')
            if code == 'GOLD': map_info_gold = asset_map.get('GOLD', {}); return map_info_gold.get('자산구분', '대체투자'), map_info_gold.get('국적구분', '기타')
            return '미분류', '미분류'

        map_results = holdings_df['종목코드'].apply(get_mapping_info)
        holdings_df['자산구분'] = map_results.apply(lambda x: x[0])
        holdings_df['국적구분'] = map_results.apply(lambda x: x[1])

        # --- '종합분류' 기준 집계 ---
        # 국적과 자산 구분을 합쳐서 '종합분류' 생성 (예: "미국 주식", "한국 채권")
        # '대체투자'는 국적을 '기타'로 간주하고 합침
        holdings_df['종합분류'] = holdings_df.apply(
            lambda row: f"{row['국적구분']} {row['자산구분']}" if row['자산구분'] != '대체투자' else '대체투자 (금현물)',
            axis=1
        )
        # 현금성 자산 처리 부분 없음 (필요시 추가 구현)

        # '종합분류' 기준으로 그룹화하여 합계 계산
        summary_grouped = holdings_df.groupby('종합분류', observed=False)[['평가금액', '비중(%)']].sum().reset_index()

        # 목표 비중 매핑을 위한 키 생성 함수 (자산구분, 국적구분 tuple)
        def get_combined_key(combined_name):
             if combined_name == '대체투자 (금현물)':
                 return ('대체투자', '기타') # 설정 시트의 키와 맞춤
             parts = combined_name.split(' ')
             if len(parts) == 2:
                 # 순서 주의: (자산구분, 국적구분) -> ('주식', '미국')
                 # 키 생성 시 순서: (asset_class, nationality)
                 # '미국 주식' -> ('주식', '미국')
                 return (parts[1], parts[0])
             return ('미분류', '미분류') # 매칭 안될 경우

        # 목표 비중(%) 컬럼 추가 및 값 매핑
        summary_grouped['목표비중(%)'] = summary_grouped['종합분류'].apply(
             lambda name: target_allocation_combined.get(get_combined_key(name), 0.0)
        )
        # 차이(%) 컬럼 계산
        summary_grouped['차이(%)'] = summary_grouped['비중(%)'] - summary_grouped['목표비중(%)']

        # 소수점 정리
        summary_grouped = summary_grouped.round(2)
        # 금액 포맷팅
        summary_grouped['평가금액'] = summary_grouped['평가금액'].apply(lambda x: f"{x:,.0f}")

        # 최종 결과 DataFrame
        summary_df = summary_grouped[['종합분류', '평가금액', '비중(%)', '목표비중(%)', '차이(%)']]
        # --- ---

    else: print("  ⚠️ 비중 계산 불가.")


    # 5. 결과 출력 (종합 분류 기준)
    print("\n[5단계] 현재 자산 배분 현황 출력 (종합 분류 기준)...")
    print("-" * 70) # 라인 길이 조정
    print(f"  조회 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  총 포트폴리오 평가액: {total_portfolio_value:,.0f} 원")
    print("-" * 70)

    if not summary_df.empty:
        print("\n▶️ 종합 분류별 현황 (목표 대비):")
        # Pandas DataFrame 출력 설정
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
             print(summary_df.to_string(index=False))

        # 상세 내역 출력 (선택적)
        # print("\n▶️ 상세 보유 내역:")
        # holdings_df['평가금액'] = holdings_df['평가금액'].astype(float).apply(lambda x: f"{x:,.0f}") # 상세 출력 전 포맷팅
        # holdings_df['비중(%)'] = holdings_df['비중(%)'].round(2)
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
        #      print(holdings_df[['계좌명', '종목명', '평가금액', '비중(%)', '종합분류']].to_string(index=False))

    else:
        print("\n  표시할 자산 배분 정보가 없습니다.")

    print("-" * 70)
    end_time = time.time()
    print(f"⏱️ 조회 완료 (소요 시간: {end_time - start_time:.2f}초)")
    print("-" * 70)

# --- 스크립트 실행 ---
if __name__ == '__main__':
    main()