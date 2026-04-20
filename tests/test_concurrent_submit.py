from __future__ import annotations

import argparse
import sys
import time

from testlib import (
    REPO_ROOT,
    CommandError,
    docker_logs,
    ensure_files_exist,
    main_header,
    print_section,
    start_cluster,
    stop_cluster,
    submit_wordcount,
    wait_process,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the concurrent submit integration test.")
    parser.add_argument("--workers", type=int, default=2, help="Number of workers to start.")
    parser.add_argument("--master-port", type=int, default=50051, help="Master gRPC port.")
    parser.add_argument("--job-gap-seconds", type=float, default=5.0, help="Delay between the two submissions.")
    parser.add_argument("--job-timeout", type=int, default=180, help="Timeout in seconds for each job.")
    parser.add_argument("--build", action="store_true", help="Rebuild images/artifacts before starting.")
    parser.add_argument("--master-memory-gb", type=int, default=2, help="Memory limit for the master container.")
    parser.add_argument("--worker-memory-gb", type=int, default=2, help="Memory limit for each worker container.")
    parser.add_argument("--submitter-memory-gb", type=int, default=2, help="Memory limit for each submitter container.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result_a = REPO_ROOT / "docker-output" / "tests" / "concurrent-a.ser"
    result_b = REPO_ROOT / "docker-output" / "tests" / "concurrent-b.ser"
    result_c = REPO_ROOT / "docker-output" / "tests" / "concurrent-c.ser"
    result_d = REPO_ROOT / "docker-output" / "tests" / "concurrent-d.ser"

    result_a.unlink(missing_ok=True)
    result_b.unlink(missing_ok=True)
    result_c.unlink(missing_ok=True)
    result_d.unlink(missing_ok=True)

    main_header("Concurrent submit test")

    try:
        print(f"Starting cluster with {args.workers} workers...")
        start_cluster(
            args.workers,
            args.master_port,
            skip_build=not args.build,
            dev=False,
            master_memory_gb=args.master_memory_gb,
            worker_memory_gb=args.worker_memory_gb,
        )

        print(f"Submitting first job (result will be at {result_a})...")
        process_a = submit_wordcount(args.master_port, result_a, skip_build=not args.build, memory_gb=args.submitter_memory_gb)

        time.sleep(args.job_gap_seconds)

        print(f"Submitting second job (result will be at {result_b})...")
        process_b = submit_wordcount(args.master_port, result_b, skip_build=True, memory_gb=args.submitter_memory_gb)

        print(f"Submitting third job (result will be at {result_c})...")
        process_c = submit_wordcount(args.master_port, result_c, skip_build=True, memory_gb=args.submitter_memory_gb)

        print(f"Submitting fourth job (result will be at {result_d})...")
        process_d = submit_wordcount(args.master_port, result_d, skip_build=True, memory_gb=args.submitter_memory_gb)


        code_a, output_a = wait_process(process_a, timeout=args.job_timeout)
        code_b, output_b = wait_process(process_b, timeout=args.job_timeout)
        code_c, output_c = wait_process(process_c, timeout=args.job_timeout)
        code_d, output_d = wait_process(process_d, timeout=args.job_timeout)

        print_section("Submit A", output_a)
        print_section("Submit B", output_b)
        print_section("Submit C", output_c)
        print_section("Submit D", output_d)

        if code_a != 0 or code_b != 0 or code_c != 0 or code_d != 0:
            raise CommandError(f"At least one submit failed. exit codes: first={code_a}, second={code_b}, third={code_c}, fourth={code_d}")

        ensure_files_exist([result_a, result_b, result_c, result_d])
        print_section("Master tail", docker_logs("jmr-master", tail=120))
        print("\nConcurrent submit test passed.")
        return 0
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        return 130
    except CommandError as exc:
        print(f"\nTEST FAILED\n{exc}", file=sys.stderr)
        try:
            print_section("Master tail", docker_logs("jmr-master", tail=200))
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
