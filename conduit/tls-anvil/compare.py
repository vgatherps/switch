"""Compares a TLS-Anvil report with the known failures (make docker-tls-anvil).

usage: compare.py <known-failures.txt> <results-dir>

<results-dir> holds TLS-Anvil's results/<test-id>/_testRun.json. A test failed if its result is
PARTIALLY_FAILED, FULLY_FAILED or TEST_SUITE_ERROR. Prints the failures not in the known list
and the known failures that passed (or did not run); exits 1 iff there is a new failure.
"""

import glob
import json
import os
import sys

FAILED = {"PARTIALLY_FAILED", "FULLY_FAILED", "TEST_SUITE_ERROR"}


def known_failures(path):
    """Test id -> reason, from lines `<test-id> <reason>`; `#` starts a comment."""
    known = {}
    with open(path) as lines:
        for line in lines:
            line = line.split("#", 1)[0].strip()
            if line:
                test_id, reason = line.split(None, 1)
                known[test_id] = reason
    return known


def results(results_dir):
    """Test id -> (result, test name) for every test run in the report."""
    runs = {}
    for path in glob.glob(os.path.join(results_dir, "results", "*", "_testRun.json")):
        with open(path) as f:
            run = json.load(f)
        name = run["TestClass"].rsplit(".", 1)[-1] + "." + run["TestMethod"]
        runs[run["TestId"]] = (run["Result"], name)
    return runs


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    known = known_failures(sys.argv[1])
    runs = results(sys.argv[2])
    if not runs:
        sys.exit(f"compare.py: no test results under {sys.argv[2]}")
    failed = {test_id: name for test_id, (result, name) in runs.items() if result in FAILED}
    new = sorted(set(failed) - set(known))
    fixed = sorted(set(known) - set(failed))
    for test_id in new:
        print(f"NEW FAILURE {test_id} {failed[test_id]} ({runs[test_id][0]})")
    for test_id in fixed:
        result = runs[test_id][0] if test_id in runs else "not run"
        print(f"known failure no longer fails: {test_id} {result}: {known[test_id]}")
    print(
        f"compare.py: {len(runs)} tests, {len(failed)} failed: "
        f"{len(failed) - len(new)} known, {len(new)} new; {len(fixed)} known failures did not fail"
    )
    sys.exit(1 if new else 0)


if __name__ == "__main__":
    main()
