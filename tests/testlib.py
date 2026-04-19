from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
GIT_BASH = Path(r"C:\Program Files\Git\bin\bash.exe")

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


class CommandError(RuntimeError):
    pass


def ensure_git_bash() -> Path:
    if not GIT_BASH.exists():
        raise CommandError(f"Git Bash not found at {GIT_BASH}")
    return GIT_BASH


def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    timeout: int | None = None,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    process = subprocess.run(
        cmd,
        cwd=str(cwd or REPO_ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=capture_output,
        timeout=timeout,
        check=False,
    )
    if process.returncode != 0:
        raise CommandError(format_failure(cmd, process.returncode, process.stdout, process.stderr))
    return process


def run_streaming(cmd: list[str], *, cwd: Path | None = None, timeout: int | None = None) -> None:
    process = subprocess.Popen(
        cmd,
        cwd=str(cwd or REPO_ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=None,
        stderr=None,
    )
    try:
        process.wait(timeout=timeout)
    except KeyboardInterrupt:
        terminate_process(process)
        raise
    except subprocess.TimeoutExpired:
        terminate_process(process)
        raise CommandError(f"Process timed out after {timeout}s: {' '.join(cmd)}")

    if process.returncode != 0:
        raise CommandError(f"Command failed with exit code {process.returncode}: {' '.join(cmd)}")


def start_process(cmd: list[str], *, cwd: Path | None = None) -> subprocess.Popen[str]:
    return subprocess.Popen(
        cmd,
        cwd=str(cwd or REPO_ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def bash_script(script_name: str, *args: str) -> list[str]:
    bash = ensure_git_bash()
    script_path = (SCRIPTS_DIR / script_name).resolve()
    return [str(bash), str(script_path), *args]


def stop_cluster(remove_volumes: bool = True) -> None:
    args = ["--remove-volumes"] if remove_volumes else []
    run_streaming(bash_script("stopCluster.sh", *args), timeout=180)


def start_cluster(
    num_workers: int,
    master_port: int,
    *,
    skip_build: bool = True,
    dev: bool = True,
    master_memory_gb: int | None = None,
    worker_memory_gb: int | None = None,
) -> None:
    args = [str(num_workers), str(master_port)]
    if dev:
        args.append("--dev")
    if skip_build:
        args.append("--skip-build")
    if master_memory_gb is not None:
        args.extend(["--master-memory-gb", str(master_memory_gb)])
    if worker_memory_gb is not None:
        args.extend(["--worker-memory-gb", str(worker_memory_gb)])
    run_streaming(bash_script("startCluster.sh", *args), timeout=240)


def submit_wordcount(
    master_port: int,
    result_file: Path,
    *,
    skip_build: bool = True,
    dev: bool = True,
    memory_gb: int | None = None,
) -> subprocess.Popen[str]:
    args = [str(master_port)]
    if dev:
        args.append("--dev")
    if skip_build:
        args.append("--skip-build")
    if memory_gb is not None:
        args.extend(["--memory-gb", str(memory_gb)])
    args.extend(["--result-file", result_file.relative_to(REPO_ROOT).as_posix()])
    return start_process(bash_script("scriptExecuteWc.sh", *args))


def wait_process(process: subprocess.Popen[str], *, timeout: int) -> tuple[int, str]:
    try:
        stdout, _ = process.communicate(timeout=timeout)
    except KeyboardInterrupt:
        terminate_process(process)
        raise
    except subprocess.TimeoutExpired:
        terminate_process(process)
        stdout, _ = process.communicate()
        raise CommandError(f"Process timed out after {timeout}s\n{stdout}")
    return process.returncode, stdout


def docker(*args: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return run(["docker", *args], timeout=timeout)


def docker_logs(container: str, *, tail: int = 200) -> str:
    return docker("logs", container, "--tail", str(tail), timeout=60).stdout or ""


def wait_for_log(container: str, needle: str, *, timeout: int = 60, poll_seconds: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if needle in docker_logs(container, tail=400):
                return True
        except CommandError:
            pass
        time.sleep(poll_seconds)
    return False


def kill_container(container: str) -> None:
    docker("rm", "-f", container, timeout=60)


def terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return

    try:
        process.terminate()
        process.wait(timeout=10)
    except Exception:
        try:
            process.kill()
            process.wait(timeout=5)
        except Exception:
            pass


def ensure_files_exist(paths: Iterable[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise CommandError("Missing expected files:\n" + "\n".join(missing))


def print_section(title: str, body: str) -> None:
    print(f"\n=== {title} ===")
    print(body.rstrip() or "<empty>")


def format_failure(cmd: list[str], returncode: int, stdout: str, stderr: str) -> str:
    joined = " ".join(cmd)
    return (
        f"Command failed with exit code {returncode}: {joined}\n"
        f"--- stdout ---\n{stdout}\n"
        f"--- stderr ---\n{stderr}"
    )


def main_header(title: str) -> None:
    print(title)
    print(f"Repository: {REPO_ROOT}")
    sys.stdout.flush()
