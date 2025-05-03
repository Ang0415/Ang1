@echo OFF
cd /d "%~dp0"

echo Running portfolio_performance.py...
python portfolio_performance.py
set PYTHON_EXIT_CODE=%errorlevel%

if %PYTHON_EXIT_CODE% neq 0 (
    echo ERROR: portfolio_performance.py failed (Exit Code: %PYTHON_EXIT_CODE%). Skipping Git operations.
    pause
    exit /b %PYTHON_EXIT_CODE%
)
echo Python script finished successfully.

echo.
echo Starting Git operations...

REM Check if result files exist
if not exist "twr_results.csv" (
    echo ERROR: twr_results.csv not found!
    pause
    exit /b 1
)
if not exist "gain_loss.json" (
    echo ERROR: gain_loss.json not found!
    pause
    exit /b 1
)
echo Result files exist.

echo Staging files...
git add twr_results.csv gain_loss.json
echo Files staged.

echo Checking for changes...
git diff --cached --quiet -- twr_results.csv gain_loss.json
set GIT_DIFF_EXIT_CODE=%errorlevel%
echo Git diff exit code: %GIT_DIFF_EXIT_CODE% (0 = no changes, 1 = changes)

REM --- Temporarily force commit/push for debugging (REMOVE LATER) ---
REM echo DEBUG: Forcing commit and push regardless of changes...
REM set GIT_DIFF_EXIT_CODE=1
REM --- End of temporary debugging ---

if %GIT_DIFF_EXIT_CODE% equ 1 (
    echo Changes detected. Committing...
    set CURRENT_DATETIME=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%%time:~6,2%
    set COMMIT_MSG=Automated update: performance results %CURRENT_DATETIME%
    echo Commit message: "%COMMIT_MSG%"
    git commit -m "%COMMIT_MSG%"
    set COMMIT_EXIT_CODE=%errorlevel%
    if %COMMIT_EXIT_CODE% neq 0 (
        echo ERROR: Git commit failed (Exit Code: %COMMIT_EXIT_CODE%). Check git status manually.
        git status
        pause
        exit /b %COMMIT_EXIT_CODE%
    )
    echo Commit successful.

    echo Pushing changes to master branch...
    git push origin master
    set PUSH_EXIT_CODE=%errorlevel%
    if %PUSH_EXIT_CODE% neq 0 (
        echo ERROR: Git push failed (Exit Code: %PUSH_EXIT_CODE%). Check GitHub credentials/connection.
        pause
        exit /b %PUSH_EXIT_CODE%
    )
    echo Git push successful.
) else (
    echo No changes detected in result files. Skipping commit and push.
)

echo.
echo Script finished.
pause