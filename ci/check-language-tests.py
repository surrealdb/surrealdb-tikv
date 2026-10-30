#!/usr/bin/env python3
"""Compare the failures in a language-test run's output with the known failures.

usage: check-language-tests.py <run-output> <known-failures>

Exits 1 when a test failed that is not listed, or when the run reports no results at all. A
listed test that passed is reported and does not fail the check, because the failures listed
here include races that pass on some runs.
"""

import re
import sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
FINISHED = re.compile(r"^Finished (\S+) on \S+ (Success|Error)")


def main(output_path, known_path):
    with open(known_path) as known_file:
        known = {line.strip() for line in known_file if line.strip() and not line.startswith("#")}
    passed, failed = set(), set()
    with open(output_path, errors="replace") as output:
        for line in output:
            match = FINISHED.match(ANSI.sub("", line).strip())
            if match:
                (passed if match.group(2) == "Success" else failed).add(match.group(1))
    if not passed and not failed:
        print("the run produced no results")
        return 1
    unexpected = sorted(failed - known)
    recovered = sorted(known & passed)
    print(f"{len(passed)} passed, {len(failed)} failed, {len(failed & known)} of them known")
    if recovered:
        print("known failures that passed (remove them from the list):")
        for name in recovered:
            print(f"  {name}")
    if unexpected:
        print("unexpected failures:")
        for name in unexpected:
            print(f"  {name}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:3]))
