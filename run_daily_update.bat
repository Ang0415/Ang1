@echo OFF
REM �� ��ũ��Ʈ�� twr_results.csv�� gain_loss.json ������ ���������
REM Ȯ���ϰ� GitHub�� �ڵ����� Ŀ�� �� Ǫ���մϴ�.
REM ���� ���� portfolio_performance.py�� ���� ����Ǿ�� �մϴ�.

REM ��ũ��Ʈ�� �ִ� ����(KOR_invest)�� �̵�
cd /d "%~dp0"
echo Changed directory to: %CD%

echo.
echo Starting Git operations for result files...

REM ��� ������ �����ϴ��� �⺻���� Ȯ��
if not exist "twr_results.csv" (
    echo ERROR: twr_results.csv not found! Ensure portfolio_performance.py ran first.
    pause
    goto :END_SCRIPT
)
if not exist "gain_loss.json" (
    echo ERROR: gain_loss.json not found! Ensure portfolio_performance.py ran first.
    pause
    goto :END_SCRIPT
)
echo Result files exist.

echo Staging result files...
git add twr_results.csv gain_loss.json
echo Files staged.

echo Checking for local changes in result files...
REM ������¡�� ���Ͽ� ��������� �ִ��� ������ Ȯ��
git diff --cached --quiet -- twr_results.csv gain_loss.json
set GIT_DIFF_EXIT_CODE=%errorlevel%
echo Git diff exit code: %GIT_DIFF_EXIT_CODE% (0 = no changes, 1 = changes)

REM ��������� �����Ǿ����� Ȯ�� (errorlevel 1 = �����)
if %GIT_DIFF_EXIT_CODE% equ 1 goto :COMMIT_CHANGES

REM ������� ����
echo No local changes detected in result files. Skipping commit and push.
goto :END_SCRIPT


:COMMIT_CHANGES
echo Local changes detected. Proceeding with commit and push...

echo Pulling latest changes from remote repository (master branch) using rebase...
REM Ŀ��/Ǫ�� �� �׻� ���� ����ҿ� ����ȭ (�浹 ����)
git pull origin master --rebase
set PULL_EXIT_CODE=%errorlevel%

REM Pull/Rebase ���� �� (��: �浹) ��ũ��Ʈ ����
if %PULL_EXIT_CODE% neq 0 (
    echo ERROR: Git pull --rebase failed (Exit Code: %PULL_EXIT_CODE%). Manual intervention might be needed.
    echo Please run 'git status' and resolve issues manually before running this script again.
    pause
    goto :END_SCRIPT
)
echo Git pull successful or no remote changes to pull.

echo Committing local changes...
REM Ŀ�� �޽����� ����� ���� �ð� (YYYYMMDD_HHMMSS ����)
set CURRENT_DATETIME=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%%time:~6,2%
set COMMIT_MSG=Automated update: performance results %CURRENT_DATETIME%
echo Commit message: "%COMMIT_MSG%"
git commit -m "%COMMIT_MSG%"
set COMMIT_EXIT_CODE=%errorlevel%

REM Ŀ�� ���� ���� Ȯ��
if %COMMIT_EXIT_CODE% neq 0 (
    echo ERROR: Git commit failed (Exit Code: %COMMIT_EXIT_CODE%). Check 'git status'.
    git status
    pause
    goto :END_SCRIPT
)
echo Commit successful.

echo Pushing changes to master branch...
git push origin master
set PUSH_EXIT_CODE=%errorlevel%

REM Ǫ�� ���� ���� Ȯ��
if %PUSH_EXIT_CODE% neq 0 (
    echo ERROR: Git push failed (Exit Code: %PUSH_EXIT_CODE%). Check credentials/connection.
    pause
    goto :END_SCRIPT
)
echo Git push successful.
goto :END_SCRIPT


:END_SCRIPT
echo.
echo Git sync script finished.
REM �۾� �����ٷ����� portfolio_performance.py ���� *�Ŀ�* �� ��ũ��Ʈ�� ������ ���� �Ʒ� pause�� �����ϼ���.
pause