#!/bin/sh
# Run in the DPDK container after building switch-bench and switch-junctiond. Two available CPUs required.
set -eu
export JUNCTION_CPU=${JUNCTION_CPU:-0} APP_CPU=${APP_CPU:-2}
export BENCH_BIN=${BENCH_BIN:-/work/target/debug/switch-bench}
export JUNCTION_BIN=${JUNCTION_BIN:-/work/target/debug/switch-junctiond}
python3 - <<'PY'
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import tempfile
import threading
import time

def recv_echo(peer, size):
    echoed = b""
    while len(echoed) < size:
        data = peer.recv(4096)
        if not data:
            raise EOFError(f"echo peer closed after {len(echoed)}/{size} bytes")
        echoed += data
    return echoed

with tempfile.TemporaryDirectory(prefix="bench-junction-") as root:
    root = Path(root)
    config = root / "nic.json"
    config.write_text(json.dumps({
        "version": 1, "device": {"vdev": "net_tap_benchjunction,iface=benchjunction"},
        "mac": None, "ipv4": "10.76.0.2/24", "gateway": None, "mtu": 1500,
        "paths": [], "offloads": "auto", "rx_timestamps": False, "descriptors": 512,
        "eal": {"file_prefix": "benchjunction", "extra": ["--no-huge", "-m", "512", "--no-shconf"]},
    }))
    ipc = root / "ipc"
    logs = (root / "daemon.log").open("w+")
    daemon = subprocess.Popen([
        "taskset", "-c", os.environ["JUNCTION_CPU"], os.environ["JUNCTION_BIN"],
        "--config", str(config), "--dir", str(ipc), "--control-cpus", os.environ["APP_CPU"],
    ], stdout=logs, stderr=logs)
    app = None
    try:
        deadline = time.monotonic() + 20
        while not (ipc / "control.sock").is_socket() or not Path("/sys/class/net/benchjunction").exists():
            assert daemon.poll() is None, "switch-junctiond exited before readiness"
            assert time.monotonic() < deadline, "switch-junctiond startup timed out"
            time.sleep(0.01)
        subprocess.run(["ip", "address", "add", "10.76.0.1/24", "dev", "benchjunction"], check=True)
        subprocess.run(["ip", "link", "set", "benchjunction", "up"], check=True)
        base = ["taskset", "-c", os.environ["APP_CPU"], os.environ["BENCH_BIN"]]
        backend = ["--backend", "junction", "--junction-socket", str(ipc / "control.sock")]
        app_log = (root / "app.log").open("w+")
        for proto in ("tcp", "ws"):
            # Delay real daemon replies so rejection cannot be consumed in the first Net round.
            # Without this, scheduler timing can hide the asynchronous startup-error regression.
            proxy_path = str(root / f"delayed-{proto}.sock")
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(proxy_path)
                listener.listen(1)
                def proxy():
                    downstream, _ = listener.accept()
                    with downstream, socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as upstream:
                        upstream.connect(str(ipc / "control.sock"))
                        try:
                            while True:
                                for source in select.select([downstream, upstream], [], [], 20)[0]:
                                    data = source.recv(65536)
                                    if not data:
                                        return
                                    if source is upstream:
                                        time.sleep(0.1)
                                    (upstream if source is downstream else downstream).sendall(data)
                        except (BrokenPipeError, ConnectionResetError):
                            return
                proxy_thread = threading.Thread(target=proxy, daemon=True)
                proxy_thread.start()
                app = subprocess.Popen(base + ["serve", "--backend", "junction", "--junction-socket",
                    proxy_path, "--proto", proto, "--port", "0"], stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, text=True)
                _, rejected = app.communicate(timeout=15)
                assert app.returncode != 0 and "tcp accept:" in rejected, (app.returncode, rejected)
                proxy_thread.join(timeout=5)
                assert not proxy_thread.is_alive(), "control proxy did not close"
            print(f"serve --backend junction {proto}: delayed invalid listener rejected", flush=True)
            app = None
        for proto in ("udp", "tcp"):
            app = subprocess.Popen(base + ["serve"] + backend + ["--proto", proto, "--port", "9000"],
                                   stdout=app_log, stderr=app_log, env={**os.environ, "SWITCH_BENCH_XSTATS": "1"})
            deadline = time.monotonic() + 10
            while True:
                app_log.flush()
                if "echo on port" in (root / "app.log").read_text():
                    break
                assert app.poll() is None, "bench serve exited before readiness"
                assert time.monotonic() < deadline, "bench serve startup timed out"
                time.sleep(0.01)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM if proto == "udp" else socket.SOCK_STREAM) as peer:
                peer.settimeout(5)
                peer.bind(("10.76.0.1", 0))
                peer.connect(("10.76.0.2", 9000))
                for i in range(16):
                    payload = f"junction-{proto}-{i:02d}".encode()
                    peer.sendall(payload)
                    echoed = recv_echo(peer, len(payload))
                    assert echoed == payload, (proto, echoed)
                print(f"serve --backend junction {proto}: 16/16 echoes", flush=True)
            app.send_signal(signal.SIGTERM)
            assert app.wait(timeout=15) == 0, "serve shutdown failed"
            assert "port xstats absent (backend junction)" in (root / "app.log").read_text()
            print("port xstats absent (backend junction)", flush=True)
            app = None
            app_log.seek(0)
            app_log.truncate()

        # Exercise generator startup, real payload traffic, stdin steps and session retirement too.
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as peer:
            peer.bind(("10.76.0.1", 9001))
            peer.settimeout(0.1)
            stop = threading.Event()
            def echo():
                while not stop.is_set():
                    try:
                        data, addr = peer.recvfrom(4096)
                    except socket.timeout:
                        continue
                    peer.sendto(data, addr)
            thread = threading.Thread(target=echo)
            thread.start()
            try:
                app = subprocess.Popen(base + ["gen"] + backend + ["--proto", "udp", "--target", "10.76.0.1:9001",
                    "--senders", "1", "--size", "64"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=app_log, text=True)
                def reply():
                    assert select.select([app.stdout], [], [], 15)[0], "generator reply timed out"
                    return json.loads(app.stdout.readline())
                assert reply()["kind"] == "ready"
                app.stdin.write(json.dumps({"kind": "step", "step": 0, "rate_pps": 100,
                    "start_unix_ns": time.time_ns() + 100_000_000, "warmup_ms": 100,
                    "step_ms": 200, "drain_ms": 200, "budget": None}) + "\n")
                app.stdin.flush()
                result = reply()
                assert result["kind"] == "step" and result["received"] > 0, result
                print(f'gen --backend junction udp: received {result["received"]}/{result["measured"]}', flush=True)
                app.stdin.close()
                assert app.wait(timeout=15) == 0, "gen shutdown failed"
                app = None
            finally:
                stop.set()
                thread.join()
        app_log.seek(0)
        print(app_log.read(), end="")
    finally:
        if app is not None and app.poll() is None:
            app.kill()
            app.wait()
        daemon.terminate()
        try:
            assert daemon.wait(timeout=15) == 0, "switch-junctiond shutdown failed"
        finally:
            if daemon.poll() is None:
                daemon.kill()
                daemon.wait()
            logs.seek(0)
            print(logs.read(), end="")
PY
