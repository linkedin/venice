#!/usr/bin/env python3
"""Check that no E2E test class exceeds the duration limit.

Reads watchdog marker files (written by VeniceSuiteListener when a test is
killed) and JUnit XML reports (for tests that completed but were slow).

Exit code 1 if any violations are found, 0 otherwise.
Output is written to both stdout and $GITHUB_STEP_SUMMARY (if set).
"""

import glob
import os
import sys
import xml.etree.ElementTree as ET

THRESHOLD_SECONDS = int(os.environ.get("TEST_DURATION_THRESHOLD_SECONDS", "600"))


def main():
    search_root = sys.argv[1] if len(sys.argv) > 1 else "."
    violations = []

    # Check watchdog marker files (test was killed by the watchdog)
    for f in sorted(glob.glob(os.path.join(search_root, "**/build/test-watchdog-timeouts/*.timeout"), recursive=True)):
        with open(f) as fh:
            parts = fh.read().strip().split("|")
            if len(parts) == 3:
                try:
                    duration = float(parts[1])
                except (ValueError, IndexError):
                    print(f"Warning: malformed watchdog marker '{f}': {parts}", file=sys.stderr)
                    violations.append((parts[0] if parts else f, 0.0, "invalid watchdog marker"))
                    continue
                violations.append((parts[0], duration, "killed by watchdog"))
            else:
                print(f"Warning: malformed watchdog marker '{f}': expected 3 fields, got {len(parts)}", file=sys.stderr)
                violations.append((parts[0] if parts else f, 0.0, "invalid watchdog marker"))

    # Check JUnit XML reports (test completed but exceeded the threshold)
    for f in sorted(glob.glob(os.path.join(search_root, "**/build/test-results/integrationTest*/TEST-*.xml"), recursive=True)):
        try:
            root = ET.parse(f).getroot()
            name = root.get("name", "")
            time = float(root.get("time", "0"))
            if time > THRESHOLD_SECONDS:
                violations.append((name, time, "completed"))
        except ET.ParseError as e:
            print(f"Warning: failed to parse JUnit XML report '{f}': {e}", file=sys.stderr)
            violations.append((f, 0.0, "junit report parse error"))

    if not violations:
        sys.exit(0)

    lines = [
        "## Test Class Duration Violations",
        "",
        "| Test Class | Duration | Status |",
        "|------------|----------|--------|",
    ]
    for name, time, status in sorted(violations, key=lambda x: -x[1]):
        mins = int(time // 60)
        secs = int(time % 60)
        lines.append(f"| {name} | {mins}m{secs}s | {status} |")
    lines.append("")
    lines.append(
        f"**{len(violations)} test class(es) exceeded the "
        f"{THRESHOLD_SECONDS // 60}-minute limit.** "
        f"Split slow test classes or optimize test methods."
    )

    output = "\n".join(lines)
    print(output)

    # Write to GitHub Actions step summary if available
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(output + "\n")

    sys.exit(1)


if __name__ == "__main__":
    main()
