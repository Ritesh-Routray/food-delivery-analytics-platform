"""
run_checks.py — Standalone quality check runner.

Can be invoked directly (CLI) or imported by the Airflow quality_checks_dag.
Runs all 6 check categories and prints a summary report.

Usage:
    python quality/run_checks.py
    python quality/run_checks.py --check freshness
    python quality/run_checks.py --fail-fast
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

# Add project root to path
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from airflow.include.quality_checks import (  # noqa: E402
    check_business_rules,
    check_freshness,
    check_null_rates,
    check_referential_integrity,
    check_schema_stability,
    check_volume_anomaly,
)

ALL_CHECKS = {
    "freshness": check_freshness,
    "volume": check_volume_anomaly,
    "nulls": check_null_rates,
    "schema": check_schema_stability,
    "referential": check_referential_integrity,
    "business_rules": check_business_rules,
}


def run_checks(selected: list[str] | None = None, fail_fast: bool = False) -> int:
    """
    Run all (or selected) quality checks.
    Returns exit code: 0 = all passed, 1 = at least one failed.
    """
    checks_to_run = {k: v for k, v in ALL_CHECKS.items()
                     if selected is None or k in selected}

    results = []
    run_ts = datetime.utcnow().isoformat()

    print(f"\n{'='*60}")
    print(f"  Data Quality Report  —  {run_ts}")
    print(f"{'='*60}")

    for name, fn in checks_to_run.items():
        result = fn()
        result["check"] = name
        results.append(result)

        icon = "✓" if result.get("passed") else "✗"
        status = "PASS" if result.get("passed") else "FAIL"
        print(f"  [{icon}] {name:<25} {status}  —  {result.get('detail', '')}")

        if fail_fast and not result.get("passed"):
            print(f"\n  Stopping early (--fail-fast) on: {name}")
            break

    passed = sum(1 for r in results if r.get("passed"))
    failed = len(results) - passed

    print(f"\n  {'='*56}")
    print(f"  Results: {passed} passed, {failed} failed out of {len(results)} checks")
    print(f"{'='*60}\n")

    # Write results to JSON for downstream consumption
    output_path = os.path.join(os.path.dirname(__file__), "last_run_results.json")
    with open(output_path, "w") as f:
        json.dump({"run_ts": run_ts, "results": results}, f, indent=2)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data quality check runner")
    parser.add_argument("--check", nargs="+", choices=list(ALL_CHECKS.keys()),
                        help="Specific checks to run (default: all)")
    parser.add_argument("--fail-fast", action="store_true",
                        help="Stop on first failure")
    args = parser.parse_args()

    exit_code = run_checks(selected=args.check, fail_fast=args.fail_fast)
    sys.exit(exit_code)
