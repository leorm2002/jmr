from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from testlib import (
    REPO_ROOT,
    CommandError,
    docker_logs,
    ensure_files_exist,
    kill_container,
    main_header,
    print_section,
    start_cluster,
    stop_cluster,
    submit_wordcount,
    wait_for_log,
    wait_process,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the worker failure integration test.")
    parser.add_argument("--workers", type=int, default=3, help="Number of workers to start.")
    parser.add_argument("--master-port", type=int, default=50051, help="Master gRPC port.")
    parser.add_argument("--worker-to-kill", default="jmr-worker-1", help="Docker container name of the worker to stop.")
    parser.add_argument(
        "--failure-phase",
        choices=("map", "reduce"),
        default="reduce",
        help="Phase during which the worker must be killed.",
    )
    parser.add_argument(
        "--kill-delay-seconds",
        type=float,
        default=1.0,
        help="Delay after the target phase starts before killing the worker.",
    )
    parser.add_argument("--job-timeout", type=int, default=240, help="Timeout in seconds for the job.")
    parser.add_argument("--build", action="store_true", help="Rebuild images/artifacts before starting.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result_file = REPO_ROOT / "docker-output" / "tests" / "worker-failure.ser"
    result_file.unlink(missing_ok=True)

    main_header("Worker failure test")

    try:
        start_cluster(args.workers, args.master_port, skip_build=not args.build)
        process = submit_wordcount(args.master_port, result_file, skip_build=not args.build)

        phase_marker = "Starting job execution" if args.failure_phase == "map" else "Reduce Progress:"
        phase_timeout = 60 if args.failure_phase == "map" else 180
        if not wait_for_log("jmr-master", phase_marker, timeout=phase_timeout):
            raise CommandError(f"Master never reached the {args.failure_phase.upper()} phase.")

        time.sleep(args.kill_delay_seconds)
        kill_container(args.worker_to_kill)

        exit_code, output = wait_process(process, timeout=args.job_timeout)
        print_section("Submit", output)
        print_section("Master tail", docker_logs("jmr-master", tail=200))

        if exit_code != 0:
            raise CommandError(f"Word count submit failed after worker kill. exit code={exit_code}")

        ensure_files_exist([result_file])
        master_logs = docker_logs("jmr-master", tail=5000)
        if "FAULT-TOLERANCE" not in master_logs and "Rescheduled" not in master_logs and "Worker failed" not in master_logs:
            raise CommandError("Worker failure happened, but master logs do not show fault-tolerance activity.")
        if "exceeded retry limit" in master_logs:
            raise CommandError("Worker failure during execution still exhausted retry limits on the master.")
        if "Reduce phase failed" in master_logs:
            raise CommandError("Reduce phase still failed after worker loss.")

        print(f"\nWorker failure test passed during {args.failure_phase.upper()}.")
        return 0
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        return 130
    except CommandError as exc:
        print(f"\nTEST FAILED\n{exc}", file=sys.stderr)
        try:
            print_section("Master tail", docker_logs("jmr-master", tail=300))
        except Exception:
            pass
        return 1
    finally:
        try:
            stop_cluster(remove_volumes=True)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
