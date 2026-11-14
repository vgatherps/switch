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
    if cpus(assignment["HOUSEKEEPING"]) & required:
        errors.append("housekeeping CPUs overlap isolated CPUs")
    processes = snapshot["processes"]
    for kind in expected:
        count = sum(process["kind"] == kind for process in processes)
        if count != 1:
            errors.append(f"expected one {kind} process, found {count}")
    for process in processes:
        kind, pid = process["kind"], process["pid"]
        if kind != "external" and not any(thread["tid"] == pid for thread in process["threads"]):
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
                forbidden = required
                if kind in expected and kind in ("serve", "gen") and backend == "junction" and comm == "junction-ctl":
                    forbidden = required - {app}
                if allowed & forbidden or (kind != "external" and psr in forbidden):
                    errors.append(f"{prefix}: may run on isolated CPU(s), allowed {sorted(allowed)}, psr {psr}")
    return errors


def processes():
    """All userspace tasks, marking benchmark processes and their descendants separately."""
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
    for pid in sorted(entries):
        threads = []
        try:
            for task in (Path("/proc") / str(pid) / "task").iterdir():
                status = (task / "status").read_text().splitlines()
                allowed = next(line.split(":", 1)[1].strip() for line in status if line.startswith("Cpus_allowed_list:"))
                stat = (task / "stat").read_text().rsplit(")", 1)[1].split()
                threads.append({"tid": int(task.name), "comm": (task / "comm").read_text().strip(),
                                "Cpus_allowed_list": allowed, "psr": int(stat[36])})
            result.append({"pid": pid, "kind": entries[pid][1] if pid in selected else "external",
                           "threads": threads})
        except (FileNotFoundError, ProcessLookupError):
            continue
    return result


def ready(found, assignment, expected):
    for kind in expected:
        matches = [process for process in found if process["kind"] == kind]
        if len(matches) != 1:
            return False
        if kind in ("serve", "kernel-serve", "gen", "junctiond"):
            cpu = int(assignment["JUNCTION_CPU" if kind == "junctiond" else "APP_CPU"])
            main = next((thread for thread in matches[0]["threads"] if thread["tid"] == matches[0]["pid"]), None)
            if main is None or cpus(main["Cpus_allowed_list"]) != {cpu}:
                return False
    return True


def capture(assignment, expected, timeout=60, before_ns=None):
    deadline = time.monotonic() + timeout
    while True:
        found = processes()
        if ready(found, assignment, expected):
            errors = []
            break
        if time.monotonic() >= deadline:
            errors = [f"timed out waiting for processes and pinned main threads: {', '.join(expected)}"]
            break
        time.sleep(0.1)
    snapshot = {"assignment": assignment,
                "isolated": Path("/sys/devices/system/cpu/isolated").read_text().strip(),
                "processes": found, "errors": errors,
                "interrupts": Path("/proc/interrupts").read_text(), "unix_ns": time.time_ns()}
    if before_ns is not None and snapshot["unix_ns"] >= before_ns:
        snapshot["errors"].append("end snapshot missed the final measurement window")
    return snapshot


def interrupt_deltas(first, last, used_cpus, allowed_device_irqs=None):
    def parse(text):
        lines = text.splitlines()
        columns = [column.removeprefix("CPU") for column in lines[0].split()]
        result = {}
        for line in lines[1:]:
            irq, separator, rest = line.partition(":")
            values = rest.split()
            if separator and len(values) >= len(columns) and all(value.isdigit() for value in values[:len(columns)]):
                result[irq.strip()] = (" ".join(values[len(columns):]),
                                       dict(zip(columns, map(int, values[:len(columns)]))))
        return result
    before, after = parse(first["interrupts"]), parse(last["interrupts"])
    isolated = cpus(first["isolated"])
    rows, errors = [], []
    for irq, (name, counts) in after.items():
        previous = before.get(irq, ("", {}))[1]
        deltas = {str(cpu): counts[str(cpu)] - previous.get(str(cpu), 0) for cpu in sorted(isolated)}
        rows.append({"irq": irq, "name": name, "cpus": deltas})
        if any(delta < 0 and int(cpu) in used_cpus for cpu, delta in deltas.items()):
            errors.append(f"interrupt {irq}: counters decreased")
        allowed = (allowed_device_irqs or {}).get(irq, ())
        local_timer = name.split()[-1:] == ["arch_timer"]
        if irq.isdigit() and not local_timer and any(
                delta > 0 and int(cpu) in used_cpus and int(cpu) not in allowed for cpu, delta in deltas.items()):
            errors.append(f"device IRQ {irq} {name} fired on isolated CPUs: {deltas}")
    return rows, errors


def wait_steps(path, count, step_ms, timeout=60, done=None):
    """Observe the penultimate flushed result; its mtime bounds the final measurement."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            with path.open() as file:
                stamp = os.fstat(file.fileno()).st_mtime_ns
                rows = [json.loads(line) for line in file if line.endswith("\n")]
            steps = sum(row.get("kind") == "step" for row in rows)
            if any(row.get("kind") == "summary" for row in rows) or steps > count:
                raise RuntimeError("sweep ended before the end snapshot was triggered")
            if steps == count:
                return stamp + step_ms * 1_000_000
        except FileNotFoundError:
            pass
        if done is not None and done.exists():
            raise RuntimeError("coordinator exited before the end snapshot was triggered")
        if time.monotonic() >= deadline:
            raise RuntimeError("timed out waiting for the penultimate step")
        time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assignment", type=json.loads)
    parser.add_argument("--expect", action="append")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--before-ns", type=int)
    parser.add_argument("--wait-steps", type=Path)
    parser.add_argument("--count", type=int)
    parser.add_argument("--step-ms", type=int)
    parser.add_argument("--done", type=Path)
    args = parser.parse_args()
    if args.wait_steps:
        try:
            print(wait_steps(args.wait_steps, args.count, args.step_ms, args.timeout, args.done))
            return 0
        except (RuntimeError, ValueError) as error:
            print(error)
            return 1
    if args.assignment is None or args.expect is None or args.out is None:
        parser.error("--assignment, --expect and --out are required for snapshots")
    snapshot = capture(args.assignment, args.expect, args.timeout, args.before_ns)
    temporary = args.out.with_suffix(args.out.suffix + ".tmp")
    temporary.write_text(json.dumps(snapshot, indent=2) + "\n")
    temporary.replace(args.out)
    return 1 if snapshot["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
