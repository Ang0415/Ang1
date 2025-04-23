# telegram_utils.py
# 텔레그램 설정을 로드하고 메시지를 발송하는 유틸리티 모듈

import yaml
import requests
import os
import traceback

# --- 경로 설정 ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(CURRENT_DIR, 'telegram_config.yaml')
# --- ---

# --- 전역 변수 ---
_telegram_config = {}
# --- ---

def load_telegram_config():
    """텔레그램 설정 파일(telegram_config.yaml)을 로드합니다."""
    global _telegram_config
    if _telegram_config: # 이미 로드되었으면 다시 로드하지 않음
        return True
    try:
        with open(CONFIG_PATH, encoding='UTF-8') as f:
            _telegram_config = yaml.load(f, Loader=yaml.FullLoader)
        if not _telegram_config: # 파일은 있으나 내용이 비었을 경우
             print(f"⚠️ 텔레그램 설정 파일({CONFIG_PATH}) 내용은 비어있습니다.")
             _telegram_config = {}
             return False
        # 필수 키 확인
        if 'bot_token' not in _telegram_config or 'chat_id' not in _telegram_config:
            print(f"❌ 텔레그램 설정 파일({CONFIG_PATH})에 'bot_token' 또는 'chat_id' 키가 없습니다.")
            _telegram_config = {}
            return False
        # 플레이스홀더 값 확인
        if _telegram_config.get('bot_token') == 'YOUR_BOT_TOKEN' or \
           _telegram_config.get('chat_id') == 'YOUR_CHAT_ID':
           print(f"⚠️ 텔레그램 설정 파일({CONFIG_PATH})의 토큰 또는 Chat ID가 기본값(YOUR...)입니다. 실제 값으로 변경해주세요.")
           # 이 경우에도 일단 로드는 성공한 것으로 처리하나, 메시지 발송은 안 될 것임
        print(f"✅ 텔레그램 설정 로드 완료: {CONFIG_PATH}")
        return True
    except FileNotFoundError:
        print(f"❌ 텔레그램 설정 파일({CONFIG_PATH})을 찾을 수 없습니다.")
        _telegram_config = {}
        return False
    except yaml.YAMLError as e:
        print(f"❌ 텔레그램 설정 파일({CONFIG_PATH}) 형식 오류: {e}")
        _telegram_config = {}
        return False
    except Exception as e:
        print(f"❌ 텔레그램 설정 파일 로드 중 오류 발생: {e}")
        traceback.print_exc()
        _telegram_config = {}
        return False

def get_telegram_credentials():
    """로드된 텔레그램 설정에서 봇 토큰과 Chat ID를 반환합니다."""
    if not _telegram_config and not load_telegram_config():
        return None, None # 설정 로드 실패 시 None 반환
    return _telegram_config.get('bot_token'), _telegram_config.get('chat_id')

def send_telegram_message(message):
    """설정 파일에서 읽어온 정보로 텔레그램 메시지를 전송합니다."""
    bot_token, chat_id = get_telegram_credentials()

    if not bot_token or bot_token == 'YOUR_BOT_TOKEN':
        print("텔레그램 봇 토큰이 유효하지 않습니다. 알림을 보내지 않습니다.")
        return
    if not chat_id or chat_id == 'YOUR_CHAT_ID':
        print("텔레그램 채팅 ID가 유효하지 않습니다. 알림을 보내지 않습니다.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown' # 간단한 마크다운 사용
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status() # HTTP 오류 발생 시 예외 발생
        print(f"📢 텔레그램 메시지 발송 완료 (Chat ID: {str(chat_id)[:4]}...)") # ID 일부만 로그 출력
    except requests.exceptions.RequestException as e:
        print(f"텔레그램 메시지 발송 실패: {e}")
    except Exception as e:
        print(f"텔레그램 메시지 발송 중 예상치 못한 오류: {e}")

# 스크립트 로드 시 설정 파일 미리 읽기 시도 (선택적)
# load_telegram_config()