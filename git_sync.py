import subprocess
import os
import sys
from datetime import datetime

# --- 설정 ---
# Git 저장소 경로 (스크립트가 있는 폴더로 가정)
repo_path = os.path.dirname(os.path.abspath(__file__))
# 커밋/푸시 대상 파일 목록
files_to_add = ["twr_results.csv", "gain_loss.json"]
# 원격 저장소 이름 및 브랜치
remote_name = "origin"
branch_name = "master"
# --- ---

def run_git_command(command_list):
    """Git 명령어를 실행하고 결과를 반환하는 함수"""
    try:
        print(f"Executing: {' '.join(command_list)}")
        # stderr=subprocess.PIPE 추가하여 오류 메시지 캡처
        result = subprocess.run(command_list, cwd=repo_path, check=False, capture_output=True, text=True, encoding='utf-8')
        print(f"  Return Code: {result.returncode}")
        if result.stdout:
            print(f"  Stdout: {result.stdout.strip()}")
        if result.stderr:
            # 오류 메시지는 항상 출력
            print(f"  Stderr: {result.stderr.strip()}", file=sys.stderr) # 오류 스트림으로 출력
        return result
    except FileNotFoundError:
        print(f"ERROR: 'git' command not found. Is Git installed and in PATH?", file=sys.stderr)
        return None
    except Exception as e:
        print(f"ERROR: Failed to run command {' '.join(command_list)} - {e}", file=sys.stderr)
        return None

def main():
    print("--- Starting Git Sync Script ---")

    # 1. 결과 파일 스테이징
    print("\nStep 1: Staging result files...")
    add_command = ["git", "add"] + files_to_add
    add_result = run_git_command(add_command)
    # add 명령어는 보통 오류가 없으면 returncode 0 반환
    if add_result is None or add_result.returncode != 0:
        print("ERROR: Failed to stage files.", file=sys.stderr)
        return # 오류 시 종료

    # 2. 변경 사항 확인
    print("\nStep 2: Checking for local changes...")
    # --quiet 옵션은 스크립트에서 returncode 외 정보 얻기 어려우므로 제거하고,
    # diff 결과를 직접 확인하거나 returncode만 사용
    diff_command = ["git", "diff", "--cached", "--quiet"] + files_to_add
    diff_result = run_git_command(diff_command)

    # diff --quiet는 변경 없으면 0, 변경 있으면 1, 오류 시 다른 값 반환
    if diff_result is None:
        print("ERROR: Failed to check for changes.", file=sys.stderr)
        return
    elif diff_result.returncode == 0:
        print("INFO: No local changes detected in result files. Skipping commit and push.")
        print("\n--- Git Sync Script Finished (No Changes) ---")
        return # 변경 없으면 종료
    elif diff_result.returncode == 1:
        print("INFO: Local changes detected. Proceeding...")
        # 변경사항 있을 때만 계속 진행
    else:
        print(f"ERROR: 'git diff' command failed with return code {diff_result.returncode}.", file=sys.stderr)
        return

    # 3. 원격 저장소 변경 사항 가져오기 (Pull --rebase)
    print("\nStep 3: Pulling latest changes (rebase)...")
    pull_command = ["git", "pull", remote_name, branch_name, "--rebase"]
    pull_result = run_git_command(pull_command)
    if pull_result is None or pull_result.returncode != 0:
        print("ERROR: Git pull --rebase failed. Manual intervention might be needed.", file=sys.stderr)
        # 필요시 여기서 오류 상세 내용 출력
        if pull_result and pull_result.stderr:
            print(f"DETAILS: {pull_result.stderr.strip()}", file=sys.stderr)
        return

    # 4. 커밋
    print("\nStep 4: Committing local changes...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    commit_message = f"Automated update: performance results {timestamp}"
    print(f"Commit message: {commit_message}")
    commit_command = ["git", "commit", "-m", commit_message]
    commit_result = run_git_command(commit_command)
    # 커밋 성공(returncode 0) 또는 "nothing to commit"(returncode 1일 수 있음, 하지만 diff 통과했으므로 0이어야 정상) 확인
    # rebase 후 변경사항이 사라지는 경우도 있으므로, commit 실패 시 메시지 확인
    if commit_result is None or commit_result.returncode != 0:
         # "nothing to commit" 메시지가 stderr에 있는지 확인 (Git 버전에 따라 다를 수 있음)
        if commit_result and "nothing to commit" in (commit_result.stdout + commit_result.stderr):
             print("INFO: Nothing to commit after pull/rebase. Skipping push.")
             print("\n--- Git Sync Script Finished (No Effective Changes) ---")
             return
        else:
            print("ERROR: Git commit failed.", file=sys.stderr)
            run_git_command(["git", "status"]) # 실패 시 상태 출력
            return

    # 5. 푸시
    print("\nStep 5: Pushing changes...")
    push_command = ["git", "push", remote_name, branch_name]
    push_result = run_git_command(push_command)
    if push_result is None or push_result.returncode != 0:
        print("ERROR: Git push failed.", file=sys.stderr)
        return

    print("\n--- Git Sync Script Finished Successfully ---")

if __name__ == "__main__":
    # 스크립트가 있는 폴더로 작업 디렉토리 변경 (안정성 위해)
    os.chdir(repo_path)
    print(f"Changed working directory to: {os.getcwd()}")
    main()
    # 자동 실행 시에는 아래 input 제거
    # input("Press Enter to exit...")