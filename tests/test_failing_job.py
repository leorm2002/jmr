from __future__ import annotations

import argparse
import sys

from testlib import CommandError, docker_logs, main_header, print_section, run, start_cluster, stop_cluster, bash_script


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the deterministic failing job integration test.")
    parser.add_argument("--workers", type=int, default=2, help="Number of workers to start.")
    parser.add_argument("--master-port", type=int, default=50051, help="Master gRPC port.")
    parser.add_argument("--failure-phase", choices=["map", "reduce"], default="map", help="Where the job should fail.")
    parser.add_argument("--build", action="store_true", help="Rebuild images/artifacts before starting.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    main_header("Failing job test")

    try:
        start_cluster(args.workers, args.master_port, skip_build=not args.build)
        failing_job_args = [str(args.master_port), args.failure_phase]
        if not args.build:
            # The failing-job launcher lives in the submitter jar, so we rebuild it by default.
            pass
        completed = run(bash_script("scriptExecuteFailingJob.sh", *failing_job_args), timeout=300)

        print_section("Failing submit", completed.stdout)
        master_tail = docker_logs("jmr-master", tail=200)
        print_section("Master tail", master_tail)

        if "FAILED" not in completed.stdout:
            raise CommandError("The failing job runner did not report the expected FAILED status.")

        if "exceeded retry limit" not in master_tail and "failed permanently" not in master_tail and "Map phase failed" not in master_tail \
                and "Reduce phase failed" not in master_tail:
            raise CommandError("Master logs do not show an explicit failure path for the failing job.")

        print("\nFailing job test passed.")
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
