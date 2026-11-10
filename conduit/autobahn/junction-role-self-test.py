#!/usr/bin/env python3
"""Check role startup, CPU pinning, retirement order and both child exit statuses."""
import os
from pathlib import Path
import subprocess
import tempfile

ROLE = Path(__file__).resolve().with_name("junction-role.sh")
FAKE = r'''#!/usr/bin/env python3
import os
from pathlib import Path
import signal
import socket
import sys
import time

args = sys.argv[1:]
assert args[0] == "-c"
cpu, binary, args = args[1], Path(args[2]).name, args[3:]
root = Path(os.environ["TEST_ROOT"])
if binary == "junctiond":
    assert cpu == "3"
    assert args[args.index("--control-cpus") + 1] == "4"
    sock = socket.socket(socket.AF_UNIX)
    sock.bind(str(Path(args[args.index("--dir") + 1]) / "control.sock"))
    def stop(*_):
        assert (root / "finished").exists(), "daemon stopped before app retired"
        (root / "stopped").write_text("yes")
        sys.exit(int(os.environ["HOST_STATUS"]))
    signal.signal(signal.SIGTERM, stop)
    while True:
        time.sleep(.01)
else:
    assert binary == "relay-autobahn" and cpu == "4"
    assert args[args.index("--backend") + 1] == "junction"
    assert Path(args[args.index("--junction-socket") + 1]).is_socket()
    (root / "finished").write_text("yes")
    sys.exit(int(os.environ["APP_STATUS"]))
'''

for app, host, success in [(0, 0, True), (7, 0, False), (0, 9, False)]:
    with tempfile.TemporaryDirectory(prefix="junction-role-test-") as directory:
        root = Path(directory)
        taskset = root / "taskset"
        taskset.write_text(FAKE)
        taskset.chmod(0o755)
        result = subprocess.run(
            ["sh", str(ROLE), "server", "3", "4", "--expect-connections", "517"],
            cwd=root,
            env={**os.environ, "PATH": f"{root}:{os.environ['PATH']}", "TEST_ROOT": directory,
                 "APP_STATUS": str(app), "HOST_STATUS": str(host)},
            capture_output=True, text=True, timeout=10,
        )
        assert (result.returncode == 0) == success, result
        assert (root / "stopped").exists(), result
        assert f"relay-autobahn exit {app}" in result.stderr, result
        assert f"junctiond exit {host}" in result.stderr, result

# Exercise make's actual expanded dispatch, including a failing server.
dry = subprocess.run(
    ["make", "--no-print-directory", "-n", "docker-autobahn-junction"], cwd=ROLE.parent.parent,
    capture_output=True, text=True, check=True,
).stdout
dispatch = dry[dry.rindex('case "docker-autobahn-junction" in'):]
for failure in ["none", "server", "client", "both"]:
    result = subprocess.run(
        ["sh", "-c", 'role() { echo "$1"; [ "$FAILURE" != "$1" ] && [ "$FAILURE" != both ]; }; ' + dispatch],
        env={**os.environ, "FAILURE": failure}, capture_output=True, text=True, timeout=10,
    )
    assert result.stdout.splitlines() == ["server", "client"], result
    assert (result.returncode == 0) == (failure == "none"), result

print("junction-role: self-test PASS")
