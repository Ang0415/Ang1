@echo OFF
REM 스크립트가 있는 폴더로 이동 (안정적인 실행을 위해)
cd /d "%~dp0"

echo portfolio_performance.py 스크립트 실행 중...
python portfolio_performance.py
set PYTHON_EXIT_CODE=%errorlevel%

REM 파이썬 스크립트 실행 성공 여부 확인 (오류 코드 0이 정상이면)
if %PYTHON_EXIT_CODE% neq 0 (
  echo ERROR: portfolio_performance.py 실행 실패 (오류 코드: %PYTHON_EXIT_CODE%). Git 작업을 건너<0xEB><0x81><0x91니다.
  pause
  exit /b %PYTHON_EXIT_CODE%
)

echo.
echo 변경사항 확인 및 Git 푸시 시도 중...
REM 결과 파일들을 스테이징
git add twr_results.csv gain_loss.json

REM 스테이징된 파일 중 변경사항이 있는지 확인
git diff --cached --quiet -- twr_results.csv gain_loss.json
REM errorlevel 1은 변경사항이 있다는 의미
if errorlevel 1 (
    echo 변경사항 감지. 커밋 진행 중...
    REM 현재 날짜와 시간으로 커밋 메시지 설정
    set COMMIT_MSG=Automated update: performance results %date% %time%
    git commit -m "%COMMIT_MSG%"
    if errorlevel 1 (
      echo ERROR: Git commit 실패.
      pause
      exit /b 1
    )
    echo 변경사항 푸시 중...
    REM 중요: 'main'은 실제 사용하는 GitHub 브랜치 이름으로 변경해야 할 수 있습니다 (예: master)
    git push origin main
    if errorlevel 1 (
      echo ERROR: Git push 실패. GitHub 인증 설정을 확인하세요.
      pause
      exit /b 1
    )
    echo Git 푸시 성공.
) else (
    echo 결과 파일에 변경사항이 없습니다. 커밋 및 푸시를 건너<0xEB><0x81><0x91니다.
)

echo.
echo 스크립트 작업 완료.
pause REM 실행 후 창이 바로 닫히지 않도록 잠시 멈춤 (자동 실행 시에는 제거)