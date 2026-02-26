#!/usr/bin/env python3
"""
Rebalance Venice CI integration test shards using First-Fit Decreasing bin-packing.

Input modes:
  --artifacts-dir  : Parse JUnit/TestNG XML files from extracted CI artifact archives
  --timing-json    : Parse structured timing JSON files produced by the Gradle timing hook

Output:
  Writes internal/venice-test-common/test-shard-assignments.json

Usage:
  # From extracted CI artifacts (tar.gz already unpacked):
  python scripts/ci/rebalance_test_shards.py --artifacts-dir ci-artifacts/ --target-time 600

  # From structured timing JSON:
  python scripts/ci/rebalance_test_shards.py --timing-dir build/test-timings/ --target-time 600

  # Dry run (print proposed shards without writing):
  python scripts/ci/rebalance_test_shards.py --artifacts-dir ci-artifacts/ --target-time 600 --dry-run
"""

import argparse
import glob
import json
import os
import re
import statistics
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def parse_junit_xml_files(artifacts_dir: str) -> dict[str, float]:
    """Parse TEST-*.xml files and aggregate per-test-class duration."""
    timings: dict[str, list[float]] = defaultdict(list)

    xml_files = glob.glob(os.path.join(artifacts_dir, "**/TEST-*.xml"), recursive=True)
    if not xml_files:
        print(f"WARNING: No TEST-*.xml files found in {artifacts_dir}", file=sys.stderr)
        return {}

    print(f"Found {len(xml_files)} XML test result files")

    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()

            if root.tag == "testsuite":
                name = root.get("name", "")
                time_str = root.get("time", "0")
                if name and name.startswith("com.linkedin"):
                    timings[name].append(float(time_str))

            # Also check for nested testsuites (JUnit format)
            for suite in root.iter("testsuite"):
                name = suite.get("name", "")
                time_str = suite.get("time", "0")
                if name and name.startswith("com.linkedin"):
                    if name not in timings or float(time_str) not in timings[name]:
                        timings[name].append(float(time_str))

        except ET.ParseError as e:
            print(f"WARNING: Failed to parse {xml_file}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"WARNING: Error processing {xml_file}: {e}", file=sys.stderr)

    # Use max duration for each test class (worst case across runs)
    return {name: max(durations) for name, durations in timings.items()}


def parse_timing_json_files(timing_dir: str) -> dict[str, float]:
    """Parse structured timing JSON files from Gradle timing hook."""
    timings: dict[str, float] = {}

    json_files = glob.glob(os.path.join(timing_dir, "**/*.json"), recursive=True)
    if not json_files:
        print(f"WARNING: No JSON timing files found in {timing_dir}", file=sys.stderr)
        return {}

    print(f"Found {len(json_files)} JSON timing files")

    for json_file in json_files:
        try:
            with open(json_file) as f:
                data = json.load(f)
            for entry in data.get("suites", []):
                name = entry.get("name", "")
                duration = entry.get("durationSeconds", 0)
                if name:
                    timings[name] = max(timings.get(name, 0), duration)
        except Exception as e:
            print(f"WARNING: Error processing {json_file}: {e}", file=sys.stderr)

    return timings


def discover_test_classes(repo_root: str) -> set[str]:
    """Scan integrationTest source for classes with @Test annotations."""
    test_dir = os.path.join(
        repo_root, "internal", "venice-test-common", "src", "integrationTest", "java"
    )
    test_classes = set()

    if not os.path.isdir(test_dir):
        print(f"WARNING: Integration test directory not found: {test_dir}", file=sys.stderr)
        return test_classes

    for java_file in glob.glob(os.path.join(test_dir, "**/*.java"), recursive=True):
        try:
            with open(java_file) as f:
                content = f.read()

            # Check for @Test annotation (TestNG or JUnit)
            if re.search(r"@Test\b", content):
                # Derive fully qualified class name from path
                rel_path = os.path.relpath(java_file, test_dir)
                class_name = rel_path.replace(os.sep, ".").replace(".java", "")
                test_classes.add(class_name)
        except Exception as e:
            print(f"WARNING: Error scanning {java_file}: {e}", file=sys.stderr)

    return test_classes


def load_current_assignments(repo_root: str) -> dict[str, list[str]]:
    """Load current shard assignments from JSON file if it exists."""
    json_path = os.path.join(
        repo_root, "internal", "venice-test-common", "test-shard-assignments.json"
    )
    if os.path.exists(json_path):
        with open(json_path) as f:
            data = json.load(f)
        return data.get("shards", {})
    return {}


def bin_pack_ffd(
    test_timings: dict[str, float], target_time: float
) -> dict[str, list[str]]:
    """First-Fit Decreasing bin-packing algorithm.

    Sort tests by duration descending, then assign each to the first shard
    that has enough remaining capacity. Create a new shard if none fit.
    """
    # Sort by duration descending
    sorted_tests = sorted(test_timings.items(), key=lambda x: x[1], reverse=True)

    shards: list[list[str]] = []
    shard_times: list[float] = []

    for test_name, duration in sorted_tests:
        placed = False

        # If a single test exceeds the target, it gets its own shard
        if duration > target_time:
            shards.append([test_name])
            shard_times.append(duration)
            continue

        # Try to fit into an existing shard
        for i in range(len(shards)):
            if shard_times[i] + duration <= target_time:
                shards[i].append(test_name)
                shard_times[i] += duration
                placed = True
                break

        if not placed:
            shards.append([test_name])
            shard_times.append(duration)

    # Convert to numbered dict (1-indexed)
    return {str(i + 1): tests for i, tests in enumerate(shards)}


def print_shard_summary(
    shards: dict[str, list[str]], timings: dict[str, float], target_time: float
):
    """Print a summary of shard assignments."""
    print(f"\n{'='*70}")
    print(f"Shard Summary (target: {target_time}s = {target_time/60:.1f}min)")
    print(f"{'='*70}")

    total_time = 0
    max_time = 0
    shard_durations = []

    for shard_id in sorted(shards.keys(), key=lambda x: int(x)):
        tests = shards[shard_id]
        shard_time = sum(timings.get(t, 0) for t in tests)
        shard_durations.append(shard_time)
        total_time += shard_time
        max_time = max(max_time, shard_time)
        over = " [OVER TARGET]" if shard_time > target_time else ""
        print(
            f"  Shard {shard_id:>3}: {len(tests):>3} tests, "
            f"{shard_time:>7.1f}s ({shard_time/60:.1f}min){over}"
        )

    print(f"\n  Total shards: {len(shards)}")
    print(f"  Total test time: {total_time:.1f}s ({total_time/60:.1f}min)")
    print(f"  Max shard time: {max_time:.1f}s ({max_time/60:.1f}min)")
    if shard_durations:
        print(f"  Mean shard time: {statistics.mean(shard_durations):.1f}s")
        if len(shard_durations) > 1:
            print(f"  Stdev: {statistics.stdev(shard_durations):.1f}s")
    print(f"  Wall-clock estimate: {max_time/60:.1f}min (longest shard)")


def write_assignments(
    repo_root: str,
    shards: dict[str, list[str]],
    target_time: float,
):
    """Write shard assignments to JSON file."""
    output_path = os.path.join(
        repo_root, "internal", "venice-test-common", "test-shard-assignments.json"
    )

    data = {
        "_comment": "Auto-generated by scripts/ci/rebalance_test_shards.py. Do not edit manually.",
        "_generated_at": datetime.now(timezone.utc).isoformat(),
        "_target_shard_time_seconds": target_time,
        "shards": shards,
    }

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")

    print(f"\nWrote shard assignments to {output_path}")
    print(f"  {len(shards)} shards, {sum(len(v) for v in shards.values())} tests total")


def main():
    parser = argparse.ArgumentParser(
        description="Rebalance Venice CI integration test shards"
    )

    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "--artifacts-dir",
        help="Directory containing extracted CI artifact XML files",
    )
    input_group.add_argument(
        "--timing-dir",
        help="Directory containing structured timing JSON files",
    )

    parser.add_argument(
        "--target-time",
        type=float,
        default=600,
        help="Target max time per shard in seconds (default: 600 = 10min)",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root directory (auto-detected if not specified)",
    )
    parser.add_argument(
        "--timing-overrides",
        help="JSON file with manual timing overrides: {\"class.name\": seconds, ...}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print proposed shards without writing files",
    )

    args = parser.parse_args()

    # Auto-detect repo root
    if args.repo_root:
        repo_root = args.repo_root
    else:
        # Walk up from script location to find repo root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.dirname(os.path.dirname(script_dir))
        if not os.path.exists(os.path.join(repo_root, "build.gradle")):
            print("ERROR: Could not auto-detect repo root. Use --repo-root.", file=sys.stderr)
            sys.exit(1)

    print(f"Repo root: {repo_root}")
    print(f"Target shard time: {args.target_time}s ({args.target_time/60:.1f}min)")

    # 1. Collect timing data
    timings: dict[str, float] = {}

    if args.artifacts_dir:
        timings = parse_junit_xml_files(args.artifacts_dir)
        print(f"Parsed timing data for {len(timings)} test classes from XML artifacts")
    elif args.timing_dir:
        timings = parse_timing_json_files(args.timing_dir)
        print(f"Parsed timing data for {len(timings)} test classes from JSON timing files")
    else:
        print("No timing data source specified. Will use current assignments with estimated durations.")

    # 1b. Apply timing overrides (for split classes or manual corrections)
    if args.timing_overrides:
        with open(args.timing_overrides) as f:
            overrides = json.load(f)
        print(f"Applying {len(overrides)} timing overrides from {args.timing_overrides}")
        for name, duration in overrides.items():
            timings[name] = duration

    # 2. Discover all integration test classes
    discovered = discover_test_classes(repo_root)
    print(f"Discovered {len(discovered)} test classes from source")

    # 3. Build the set of all known test classes
    current_assignments = load_current_assignments(repo_root)
    assigned_tests = set()
    for tests in current_assignments.values():
        assigned_tests.update(tests)

    all_tests = assigned_tests | discovered
    print(f"Total unique test classes: {len(all_tests)}")

    # 4. Identify tests with no timing data and assign median estimate
    tests_with_timing = set(timings.keys()) & all_tests
    tests_without_timing = all_tests - set(timings.keys())

    if tests_with_timing:
        known_durations = [timings[t] for t in tests_with_timing]
        median_duration = statistics.median(known_durations)
    else:
        # No timing data at all - use a conservative estimate
        median_duration = 120.0  # 2 minutes
        print(f"No timing data available. Using default estimate of {median_duration}s per test.")

    if tests_without_timing:
        print(f"  {len(tests_without_timing)} tests without timing data (using median estimate: {median_duration:.1f}s)")
        for test in tests_without_timing:
            timings[test] = median_duration

    # Ensure all tests have timing entries
    for test in all_tests:
        if test not in timings:
            timings[test] = median_duration

    # 5. Run bin-packing
    # Only pack tests that are in our all_tests set
    pack_timings = {t: timings[t] for t in all_tests}
    shards = bin_pack_ffd(pack_timings, args.target_time)

    # 6. Report
    print_shard_summary(shards, timings, args.target_time)

    # Report new tests not in current assignments
    new_tests = discovered - assigned_tests
    if new_tests:
        print(f"\n  New tests not in previous assignments ({len(new_tests)}):")
        for t in sorted(new_tests):
            print(f"    + {t}")

    removed_tests = assigned_tests - discovered
    if removed_tests:
        print(f"\n  Tests in assignments but not found in source ({len(removed_tests)}):")
        for t in sorted(removed_tests):
            print(f"    - {t}")

    # 7. Write output
    if args.dry_run:
        print("\n[DRY RUN] No files written.")
    else:
        write_assignments(repo_root, shards, args.target_time)


if __name__ == "__main__":
    main()
