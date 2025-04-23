# -*- coding: utf-8 -*-
# telegram_sheet_bot.py
# (텔레그램 알림 수정: 설정 파일 로드 방식, 시작/오류 알림 추가)

import logging
import traceback # 상세 에러 출력을 위해 추가
import re # 정규표현식
from datetime import datetime
import os # os 모듈 추가
import sys # sys 모듈 추가 (종료용)

# --- 텔레그램 유틸리티 임포트 ---
import telegram_utils # 설정 로드 및 상태 알림 발송용
# --- ---

# 텔레그램 라이브러리
try:
    from telegram import Update
    from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
except ImportError:
    print("오류: 'python-telegram-bot' 라이브러리가 설치되지 않았습니다.")
    print("설치 방법: pip install python-telegram-bot")
    sys.exit(1)


# 구글 시트 라이브러리
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
except ImportError:
    print("오류: 'gspread' 또는 'oauth2client' 라이브러리가 설치되지 않았습니다.")
    print("설치 방법: pip install gspread oauth2client")
    sys.exit(1)

# --- 설정 ---
# TELEGRAM_BOT_TOKEN 정의 삭제 -> telegram_utils 사용
GOOGLE_SHEET_NAME = 'KYI_자산배분'                  # 구글 시트 파일 이름
WORKSHEET_NAME = '🗓️매매일지'                       # 작업할 시트 이름
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_KEYFILE_PATH = os.path.join(CURRENT_DIR, 'stock-auto-writer-44eaa06c140c.json')
SCRIPT_NAME = os.path.basename(__file__) # 스크립트 파일명 가져오기
# --- ---

# --- Logging 설정 ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
# --- ---

# --- 구글 시트 설정 함수 ---
def setup_google_sheet():
    """구글 시트에 연결하고 워크시트 객체를 반환합니다."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE_PATH, scope)
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        logger.info(f"Google Sheet '{GOOGLE_SHEET_NAME}/{WORKSHEET_NAME}' 연결 성공.")
        return worksheet
    except FileNotFoundError:
         logger.error(f"오류: 서비스 계정 키 파일({JSON_KEYFILE_PATH})을 찾을 수 없습니다.")
         return None
    except gspread.exceptions.SpreadsheetNotFound:
         logger.error(f"오류: 스프레드시트 '{GOOGLE_SHEET_NAME}'을 찾을 수 없습니다.")
         return None
    except gspread.exceptions.WorksheetNotFound:
         logger.error(f"오류: 워크시트 '{WORKSHEET_NAME}'을 찾을 수 없습니다.")
         return None
    except Exception as e:
        logger.error(f"구글 시트 연결 중 오류 발생: {e}")
        traceback.print_exc() # 상세 오류 출력
        return None
# --- ---

# --- 메시지 분석 함수들 (기존과 동일) ---
# (5a) 한국투자증권 메시지 분석 로직 (기존과 동일)
def parse_hantoo_message(text, lines):
    """한국투자증권 문자 메시지 형식을 분석합니다."""
    logger.info("한국투자증권 형식으로 분석 시도...")
    parsed_data = {}
    action_line = ""
    for line in lines:
        if "매수체결" in line: parsed_data['구분'] = "매수"; action_line = line; break
        elif "매도체결" in line: parsed_data['구분'] = "매도"; action_line = line; break
    if '구분' not in parsed_data: logger.warning("한투 분석 실패: '매수체결'/'매도체결' 없음."); return None
    try:
        action_index = lines.index(action_line)
        if len(lines) > action_index + 2:
            original_name = lines[action_index + 1].strip(); parsed_data['종목명'] = original_name.replace(" ", "")
            logger.info(f"종목명 원본: '{original_name}', 공백 제거: '{parsed_data['종목명']}'")
            code_match = re.search(r"\(([A-Z]?\d+)\)", lines[action_index + 2])
            parsed_data['종목코드'] = code_match.group(1) if code_match else None
        else: logger.warning("한투 분석 실패: 종목명/코드 라인 부족."); return None
        if len(lines) > action_index + 3:
            qty_match = re.search(r"([\d,]+)\s*주", lines[action_index + 3])
            parsed_data['수량'] = int(qty_match.group(1).replace(',', '')) if qty_match else None
        else: parsed_data['수량'] = None
        if parsed_data['수량'] is None: logger.warning("한투 분석 실패: 수량 없음."); return None
        if len(lines) > action_index + 4:
            price_match = re.search(r"([\d,]+)\s*원", lines[action_index + 4])
            parsed_data['단가'] = int(price_match.group(1).replace(',', '')) if price_match else None
        else: parsed_data['단가'] = None
        if parsed_data['단가'] is None: logger.warning("한투 분석 실패: 단가 없음."); return None
        parsed_data['금액'] = parsed_data['수량'] * parsed_data['단가']
        parsed_data['날짜'] = datetime.now().strftime("%Y-%m-%d")
        if parsed_data.get('종목명') == "TIGER미국S&P500":
            parsed_data['계좌'] = "한투_연금"; logger.info("한투 메시지: TIGER미국S&P500 -> '한투_연금' 설정")
        else:
            parsed_data['계좌'] = "한투_IRP"; logger.info(f"한투 메시지: '{parsed_data.get('종목명')}' -> '한투_IRP' 설정")
        logger.info(f"한투 분석 성공: {parsed_data}")
        return parsed_data
    except ValueError: logger.error("한투 분석 오류: 기준 '체결' 라인 못 찾음."); return None
    except Exception as e: logger.error(f"한투 분석 중 예외: {e}"); traceback.print_exc(); return None

# (5b) 키움증권 메시지 분석 로직 (기존과 동일)
def parse_kiwoom_message(text, lines):
    """키움증권 문자 메시지 형식을 분석합니다."""
    logger.info("키움증권 형식으로 분석 시도...")
    parsed_data = {}
    try:
        if len(lines) < 4: logger.warning(f"키움 분석 실패: 라인 부족({len(lines)}). 내용: {lines}"); return None
        original_name = lines[1].strip(); parsed_data['종목명'] = original_name.replace(" ", "")
        logger.info(f"종목명 원본: '{original_name}', 공백 제거: '{parsed_data['종목명']}'")
        parsed_data['종목코드'] = None
        action_qty_line = lines[2].strip(); action_qty_match = re.match(r"(매수|매도)\s*([\d,]+)\s*주", action_qty_line)
        if action_qty_match: parsed_data['구분'] = action_qty_match.group(1); parsed_data['수량'] = int(action_qty_match.group(2).replace(',', ''))
        else: logger.warning(f"키움 분석 실패: 매수/매도/수량 분석 불가 '{action_qty_line}'"); return None
        price_line = lines[3].strip(); price_match = re.search(r"(?:평균)?단가\s*([\d,]+)\s*원?", price_line)
        if price_match: parsed_data['단가'] = int(price_match.group(1).replace(',', ''))
        else: logger.warning(f"키움 분석 실패: 단가 분석 불가 '{price_line}'"); return None
        parsed_data['금액'] = parsed_data['수량'] * parsed_data['단가']
        parsed_data['날짜'] = datetime.now().strftime("%Y-%m-%d")
        parsed_data['계좌'] = "키움_ISA" # 키움 메시지는 ISA 계좌로 고정
        logger.info(f"키움 분석 성공: {parsed_data}")
        return parsed_data
    except Exception as e: logger.error(f"키움 분석 중 예외: {e}"); traceback.print_exc(); return None

# (5c) 메인 분석 함수 (분배 역할) (기존과 동일)
def parse_transaction_message(text):
    """
    수신된 문자 메시지를 분석하여 거래 정보를 추출합니다.
    메시지 내용을 보고 한국투자증권 또는 키움증권 형식을 판단하여 처리합니다.
    """
    logger.info(f"메시지 분석 시작:\n{text}")
    lines = [line.strip() for line in text.strip().split('\n') if line.strip() and "[Web발신]" not in line]
    if not lines: logger.warning("분석 실패: 메시지 내용 없음."); return None
    if "[한투]" in text or "한국투자증권" in text: return parse_hantoo_message(text, lines)
    elif "[키움]" in text or "키움증권" in text: return parse_kiwoom_message(text, lines)
    else: logger.warning("분석 실패: 증권사([한투] 또는 [키움]) 식별 불가."); return None
# --- ---

# --- 구글 시트에 데이터 추가하는 함수 (기존과 동일) ---
def append_to_sheet(worksheet, data):
    """분석된 데이터를 구글 시트에 추가하고, C열과 I열에 수식을 입력합니다."""
    if not worksheet or not data: logger.error("워크시트/데이터 유효하지 않아 추가 불가."); return False
    try:
        row_to_append = [
            data.get("날짜", ""), data.get("종목명", ""), "", data.get("구분", ""),
            data.get("단가", ""), data.get("수량", ""), data.get("금액", ""),
            data.get("계좌", "미분류계좌"), "", data.get("종목코드", "")
        ]
        logger.info(f"시트에 추가할 데이터: {row_to_append}")
        worksheet.append_row(row_to_append, value_input_option='USER_ENTERED')
        logger.info(f"데이터 추가 성공: {row_to_append[:9]}...")
        last_row = len(worksheet.get_all_values()) # 행 번호 확인
        logger.info(f"마지막 행 번호 확인: {last_row}")
        formula_c = f'=IFERROR(VLOOKUP(B{last_row},\'⚙️설정\'!Q:R,2,FALSE),"미분류")'
        worksheet.update_acell(f'C{last_row}', formula_c); logger.info(f"C{last_row} 수식 입력: {formula_c}")
        formula_i = f'=IFERROR(VLOOKUP(B{last_row},\'⚙️설정\'!Q:S,3,FALSE),"미분류")'
        worksheet.update_acell(f'I{last_row}', formula_i); logger.info(f"I{last_row} 수식 입력: {formula_i}")
        return True
    except gspread.exceptions.APIError as e: logger.error(f"구글 시트 API 오류 (추가/업데이트 중): {e}"); return False
    except Exception as e: logger.error(f"구글 시트 데이터 추가/수식 입력 중 오류: {e}"); traceback.print_exc(); return False
# --- ---

# --- 텔레그램 명령어/메시지 처리 함수들 (기존과 동일) ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/start 명령어 수신 시 Greet 메시지 전송"""
    user = update.effective_user
    await update.message.reply_html(
        rf"안녕하세요, {user.mention_html()}님! 👋 증권사 체결 문자를 전달해주시면 '{WORKSHEET_NAME}' 시트에 기록해 드릴게요.",
    )

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """사용자로부터 텍스트 메시지 수신 시 처리"""
    message_text = update.message.text
    user_id = update.effective_user.id
    logger.info(f"사용자({user_id})로부터 메시지 수신: {message_text}")
    worksheet = context.bot_data.get('worksheet')
    if not worksheet:
        logger.error("구글 시트 워크시트가 초기화되지 않았습니다. (message_handler)")
        await update.message.reply_text("⚠️ 내부 오류: 구글 시트에 연결할 수 없습니다. 관리자에게 문의하세요.")
        return
    parsed_data = parse_transaction_message(message_text)
    if parsed_data:
        success = append_to_sheet(worksheet, parsed_data)
        if success:
            await update.message.reply_text(
                f"✅ '{parsed_data.get('종목명', '알수없음')}' "
                f"({parsed_data.get('구분', '')}, 계좌: {parsed_data.get('계좌', '미분류')}) "
                f"내역을 '{WORKSHEET_NAME}' 시트에 성공적으로 추가했습니다!"
            )
        else:
            await update.message.reply_text("❌ 구글 시트에 내역을 추가하는 중 오류가 발생했습니다. 잠시 후 다시 시도해 보거나 관리자에게 문의하세요.")
    else:
        await update.message.reply_text(
             "⚠️ 보내주신 메시지 내용을 이해하기 어렵습니다.\n"
             "증권사에서 받으신 체결 문자 원본 전체를 복사해서 보내주세요.\n"
             "(지원 형식: 한국투자증권, 키움증권)"
        )
# --- ---

# --- 메인 함수 (봇 실행) ---
def main() -> None:
    """텔레그램 봇을 시작하고 실행합니다."""
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"텔레그램 봇 초기화 시작... ({start_time})")

    # 텔레그램 설정 로드
    if not telegram_utils.load_telegram_config():
        logger.critical("치명적 오류: 텔레그램 설정 파일을 로드할 수 없습니다. 프로그램을 종료합니다.")
        return

    # 구글 시트 연결 시도 (봇 시작 시 1회)
    worksheet = setup_google_sheet()
    if not worksheet:
        logger.critical("치명적 오류: 구글 시트에 연결할 수 없습니다. 프로그램을 종료합니다.")
        # 구글 시트 연결 실패 시 텔레그램 알림 발송 시도
        fail_msg = f"🔥 `{SCRIPT_NAME}` 시작 실패: 구글 시트 연결 불가 ({start_time})"
        telegram_utils.send_telegram_message(fail_msg)
        return

    # 구글 시트 연결 성공 후 시작 알림 발송
    start_msg = f"✅ `{SCRIPT_NAME}` 시작됨 ({start_time})"
    telegram_utils.send_telegram_message(start_msg)

    # 텔레그램 봇 토큰 가져오기
    bot_token, _ = telegram_utils.get_telegram_credentials()
    if not bot_token or bot_token == 'YOUR_BOT_TOKEN':
         logger.critical("치명적 오류: 텔레그램 봇 토큰이 유효하지 않습니다! `telegram_config.yaml` 파일을 확인하세요.")
         # 토큰 오류 시 텔레그램 알림 발송 시도
         fail_msg = f"🔥 `{SCRIPT_NAME}` 시작 실패: 유효하지 않은 텔레그램 봇 토큰 ({start_time})"
         telegram_utils.send_telegram_message(fail_msg) # 이 메시지는 발송 안될 수 있음
         return

    try:
        # 텔레그램 Application 객체 생성
        application = Application.builder().token(bot_token).build()

        # 워크시트 객체를 봇 데이터에 저장 (핸들러에서 사용 위함)
        application.bot_data['worksheet'] = worksheet

        # 핸들러 등록
        application.add_handler(CommandHandler("start", start_command)) # /start 명령어 처리
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler)) # 텍스트 메시지 처리

        # 봇 실행 시작 (폴링 방식)
        logger.info("텔레그램 봇을 시작합니다 (폴링 방식)...")
        application.run_polling() # 사용자가 중지(Ctrl+C)할 때까지 실행됨

        logger.info("텔레그램 봇이 정상적으로 종료되었습니다.")
        # 정상 종료 알림 (선택적)
        # end_msg = f"ℹ️ `{SCRIPT_NAME}` 정상 종료됨 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
        # telegram_utils.send_telegram_message(end_msg)

    except Exception as e:
         # 봇 실행 중 예외 발생 시 알림
         logger.critical(f"텔레그램 봇 실행 중 오류 발생: {e}")
         error_details = traceback.format_exc()
         error_msg = f"🔥 `{SCRIPT_NAME}` 실행 중 오류 발생 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}):\n```\n{error_details[-500:]}\n```"
         telegram_utils.send_telegram_message(error_msg)
         # 오류 발생 시에도 프로그램은 종료됨

# --- ---

# --- 스크립트 실행 지점 ---
if __name__ == '__main__':
    main()
# --- ---