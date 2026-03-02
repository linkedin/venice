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

THRESHOLD_SECONDS = 600  # 10 minutes


def main():
    violations = []

    # Check watchdog marker files (test was killed by the watchdog)
    for f in sorted(glob.glob("**/build/test-watchdog-timeouts/*.timeout", recursive=True)):
        with open(f) as fh:
            parts = fh.read().strip().split("|")
            if len(parts) == 3:
                violations.append((parts[0], float(parts[1]), "killed by watchdog"))

    # Check JUnit XML reports (test completed but exceeded the threshold)
    for f in sorted(glob.glob("**/build/test-results/integrationTest*/TEST-*.xml", recursive=True)):
        try:
            root = ET.parse(f).getroot()
            name = root.get("name", "")
            time = float(root.get("time", "0"))
            if time > THRESHOLD_SECONDS:
                violations.append((name, time, "completed"))
        except ET.ParseError:
            pass

    lines = []
    if violations:
        lines.append("## Test Class Duration Violations")
        lines.append("")
        lines.append("| Test Class | Duration | Status |")
        lines.append("|------------|----------|--------|")
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
    else:
        lines.append("## Test Class Duration Check")
        lines.append("")
        lines.append("All test classes completed within the time limit.")

    output = "\n".join(lines)
    print(output)

    # Write to GitHub Actions step summary if available
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(output + "\n")

    sys.exit(1 if violations else 0)


if __name__ == "__main__":
    main()
