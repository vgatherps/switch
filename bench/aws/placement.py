#!/usr/bin/env python3
"""Bounded remote /proc snapshot and local thread-affinity checks; no AWS dependencies."""
import argparse
import json
import os
from pathlib import Path
import time


def cpus(text):
    result = set()
    for item in text.split(","):
        if not item.strip():
            continue
        first, _, last = item.partition("-")
        result.update(range(int(first), int(last or first) + 1))
    return result


def expected_processes(role, backend, kernel=False):
    expected = ["kernel-serve" if kernel else "serve"] if role == "receiver" else ["gen"]
    if role == "sender0":
        expected.append("sweep")
    if backend == "junction" and not kernel:
        expected.append("junctiond")
    return expected


def check(snapshot, expected, backend):
    assignment = snapshot["assignment"]
    app, junction = int(assignment["APP_CPU"]), int(assignment["JUNCTION_CPU"])
    isolated = cpus(snapshot["isolated"])
    errors = list(snapshot.get("errors", []))
    required = {app} | ({junction} if "junctiond" in expected else set())
    if not required <= isolated:
        errors.append(f"assigned process CPUs {sorted(required)} are not isolated ({snapshot['isolated']})")
    if cpus(assignment["HOUSEKEEPING"]) & isolated:
        errors.append("housekeeping CPUs overlap isolated CPUs")
    processes = snapshot["processes"]
    for kind in expected:
        count = sum(process["kind"] == kind for process in processes)
        if count != 1:
            errors.append(f"expected one {kind} process, found {count}")
    for process in processes:
        kind, pid = process["kind"], process["pid"]
        if not any(thread["tid"] == pid for thread in process["threads"]):
            errors.append(f"{kind} {pid}: main thread missing")
        for thread in process["threads"]:
            tid, comm = thread["tid"], thread["comm"]
            allowed, psr = cpus(thread["Cpus_allowed_list"]), thread["psr"]
            prefix = f"{kind} {pid}/{tid} {comm}"
            if tid == pid and kind in expected and kind in ("serve", "kernel-serve", "gen", "junctiond"):
                cpu = junction if kind == "junctiond" else app
                if allowed != {cpu} or psr != cpu:
                    errors.append(f"{prefix}: must be pinned to CPU {cpu}, allowed {sorted(allowed)}, psr {psr}")
            else:
                forbidden = isolated
                if kind in expected and kind in ("serve", "gen") and backend == "junction" and comm == "junction-ctl":
                    forbidden = isolated - {app}
                if allowed & forbidden or psr in forbidden:
                    errors.append(f"{prefix}: may run on isolated CPU(s), allowed {sorted(allowed)}, psr {psr}")
    return errors


def processes():
    """All benchmark processes, this housekeeping collector, and benchmark child processes."""
    entries = {}
    for path in Path("/proc").iterdir():
        if not path.name.isdigit():
            continue
        try:
            argv = (path / "cmdline").read_bytes().decode(errors="replace").split("\0")[:-1]
            if not argv:
                continue
            name = os.path.basename(argv[0])
            kind = (argv[1] if name == "zhida-bench" and len(argv) > 1 else name)
            ours = int(path.name) == os.getpid() or name.startswith("zhida-") or name == "junctiond"
            stat = (path / "stat").read_text().rsplit(")", 1)[1].split()
            entries[int(path.name)] = (int(stat[1]), kind if ours else "other", ours)
        except (FileNotFoundError, ProcessLookupError):
            continue
    selected = {pid for pid, (_, _, ours) in entries.items() if ours}
    while True:
        children = {pid for pid, (parent, _, _) in entries.items() if parent in selected}
        if children <= selected:
            break
        selected |= children
    result = []
    for pid in sorted(selected):
        threads = []
        try:
            for task in (Path("/proc") / str(pid) / "task").iterdir():
                status = (task / "status").read_text().splitlines()
                allowed = next(line.split(":", 1)[1].strip() for line in status if line.startswith("Cpus_allowed_list:"))
                stat = (task / "stat").read_text().rsplit(")", 1)[1].split()
                threads.append({"tid": int(task.name), "comm": (task / "comm").read_text().strip(),
                                "Cpus_allowed_list": allowed, "psr": int(stat[36])})
            result.append({"pid": pid, "kind": entries[pid][1], "threads": threads})
        except (FileNotFoundError, ProcessLookupError):
            continue
    return result


def capture(assignment, expected, timeout=30):
    deadline = time.monotonic() + timeout
    while True:
        found = processes()
        if set(expected) <= {process["kind"] for process in found}:
            time.sleep(1)  # Let control and EAL helper threads start before observing their masks.
            found = processes()
            errors = []
            break
        if time.monotonic() >= deadline:
            errors = [f"timed out waiting for processes: {', '.join(expected)}"]
            break
        time.sleep(0.1)
    return {"assignment": assignment, "isolated": Path("/sys/devices/system/cpu/isolated").read_text().strip(),
            "processes": found, "errors": errors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assignment", type=json.loads, required=True)
    parser.add_argument("--expect", action="append", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    snapshot = capture(args.assignment, args.expect)
    temporary = args.out.with_suffix(args.out.suffix + ".tmp")
    temporary.write_text(json.dumps(snapshot, indent=2) + "\n")
    temporary.replace(args.out)
    return 1 if snapshot["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
