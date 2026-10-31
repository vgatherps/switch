#!/usr/bin/env python3
"""Tests of zhida_aws_bench.py with fake AWS clients, hosts and local commands (no AWS, no boto3):
python3 -m unittest discover -s bench/aws"""
import argparse
import contextlib
import datetime
import io
import json
import os
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import janitor  # noqa: E402
import zhida_aws_bench as z  # noqa: E402

UTC = datetime.timezone.utc
NOW = datetime.datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
RECEIVER_DATA_IP = "10.80.1.100"
HOST_ENV = ("MGMT_IF=ens5\nDATA_IF=ens6\nDATA_PCI=0000:00:06.0\nDATA_MAC=0a:00:00:00:00:01\nNUMA_NODE=0\n"
            "APP_CPU=7\nISOLATED=7\nHOUSEKEEPING=0-6\n")
CHRONY_OK = """Reference ID    : A9FEA97B (169.254.169.123)
Stratum         : 4
System time     : 0.000002164 seconds slow of NTP time
Leap status     : Normal
"""


def write_exe(path, body):
    with open(path, "w") as f:
        f.write("#!/bin/sh\n" + body)
    os.chmod(path, 0o755)


# ---------------------------------------------------------------- pure parts


class PureTest(unittest.TestCase):
    def test_durations(self):
        self.assertEqual([z.parse_duration(t) for t in ("90m", "2h", "45s", "120", "1.5h")], [5400, 7200, 45, 120, 5400])

    def test_fixed_matrix_tags(self):
        self.assertEqual(z.sweep_tags(), ["udp-n20-s64-uncapped", "tcp-n20-s64-b50", "ws-n20-s64-b50"])

    def test_every_run_resource_carries_the_run_id_and_expiry(self):
        expires = datetime.datetime(2026, 11, 1, 13, 30, tzinfo=UTC)
        tags = {t["Key"]: t["Value"] for t in z.run_tags("r1", expires)}
        self.assertEqual(tags[janitor.RUN_TAG], "r1")
        self.assertEqual(datetime.datetime.fromisoformat(tags[janitor.EXPIRES_TAG]), expires)

    def test_user_data_powers_off_at_the_deadline_and_survives_reboot(self):
        deadline = NOW + datetime.timedelta(minutes=30)
        ud = z.user_data(deadline, NOW)
        self.assertIn(f"OnCalendar={deadline.strftime('%Y-%m-%d %H:%M:%S')} UTC", ud)
        self.assertIn("Persistent=true", ud)
        self.assertIn("systemctl enable --now zhida-deadman.timer", ud)
        self.assertIn("ExecStart=/usr/bin/systemctl poweroff", ud)
        self.assertIn("shutdown -h +30\n", ud)

    def test_host_env_needs_every_key(self):
        env = z.parse_host_env(HOST_ENV + "# comment\n")
        self.assertEqual((env["APP_CPU"], env["HOUSEKEEPING"]), ("7", "0-6"))
        self.assertEqual(z.parse_host_env("APP_CPU='3'\n" + HOST_ENV.replace("APP_CPU=7\n", ""))["APP_CPU"], "3")
        with self.assertRaisesRegex(ValueError, "HOUSEKEEPING"):
            z.parse_host_env(HOST_ENV.replace("HOUSEKEEPING=0-6\n", ""))

    def test_data_ip_from_dut_json(self):
        self.assertEqual(z.data_ip('{"version": 1, "ipv4": "10.80.1.23/24"}'), "10.80.1.23")

    def test_chrony_guard(self):
        self.assertEqual(z.parse_chrony(CHRONY_OK), (True, "offset -2.2 us"))
        fast = CHRONY_OK.replace("0.000002164 seconds slow", "0.001500000 seconds fast")
        self.assertEqual(z.parse_chrony(fast), (False, "offset 1500 us"))
        slow = CHRONY_OK.replace("0.000002164", "0.002000000")
        self.assertFalse(z.parse_chrony(slow)[0])
        unsync = CHRONY_OK.replace("Leap status     : Normal", "Leap status     : Not synchronised")
        self.assertEqual(z.parse_chrony(unsync), (False, "leap status Not synchronised"))
        self.assertEqual(z.parse_chrony(""), (False, "no chronyc tracking output"))

    def test_cache_keys_name_versions_not_amis(self):
        self.assertEqual(z.cache_keys("s3://b/cache", "aarch64", "2023", "abc123", "1.93.0"), (
            "s3://b/cache/dpdk-25.11.3-aarch64-al2023-2023.tar.gz",
            "s3://b/cache/bench-abc123-aarch64-dpdk25.11.3-rust1.93.0.tar.gz"))
        self.assertEqual(z.rust_toolchain(), "1.93.0")

    def test_ssh_config_lists_the_five_senders(self):
        cfg = z.ssh_config([f"10.80.1.{10 + i}" for i in range(5)])
        for i in range(5):
            self.assertIn(f"Host sender{i}\n    HostName 10.80.1.{10 + i}\n", cfg)
        for line in ("User ec2-user", "BatchMode yes", f"IdentityFile {z.REMOTE}/id_run",
                     f"UserKnownHostsFile {z.REMOTE}/known_hosts"):
            self.assertIn(line, cfg)

    def test_plan_prints_six_hosts_and_the_sweeps(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(z.main(["plan"]), 0)
        text = out.getvalue()
        self.assertIn("6 x c7gn.2xlarge on-demand", text)
        for role in z.ROLES:
            self.assertRegex(text, rf"\n  {role} +")
        for t in z.sweep_tags():
            self.assertIn(f"\n  {t}", text)
        self.assertIn("L-1216C47A with 48 vCPUs free", text)


# ---------------------------------------------------------------- worker/coordinator command quoting


class CommandTest(unittest.TestCase):
    """Runs the coordinator command and the template it hands to the coordinator through real `sh`,
    with fake ssh/sudo/taskset/zhida-bench on PATH recording what each layer received."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bin, self.calls = os.path.join(self.tmp, "bin"), os.path.join(self.tmp, "calls")
        os.makedirs(self.bin)
        for d in ("etc", "var"):
            os.makedirs(os.path.join(self.tmp, d))
        with open(os.path.join(self.tmp, "etc", "host.env"), "w") as f:
            f.write(HOST_ENV)
        # ssh -F CFG senderN REMOTE: the remote login shell parses REMOTE
        write_exe(os.path.join(self.bin, "ssh"), f'echo "ssh $1 $2 $3" >>{self.calls}\nexec sh -c "$4"\n')
        # sudo sh -c CMD: CMD with the host paths moved into the sandbox
        write_exe(os.path.join(self.bin, "sudo"),
                  f'[ "$1 $2" = "sh -c" ] || exit 99\necho "sudo $1 $2" >>{self.calls}\n'
                  f'exec sh -c "$(printf %s "$3" | sed "s#/etc/zhida/host.env#{self.tmp}/etc/host.env#g;'
                  f' s#{z.REMOTE}/#{self.tmp}/var/#g")"\n')
        write_exe(os.path.join(self.bin, "taskset"),
                  f'echo "taskset $1 $2 MIMALLOC=${{MIMALLOC_ALLOW_LARGE_OS_PAGES:-unset}}" >>{self.calls}\n'
                  'shift 2\nexec "$@"\n')
        # zhida-bench: `sweep` expands and runs each worker like the coordinator (sh -c, {i}, {senders})
        write_exe(os.path.join(self.bin, "zhida-bench"), f"""python3 - "$@" <<'EOF'
import subprocess, sys
argv = sys.argv[1:]
with open({self.calls!r}, "a") as f:
    f.write("zhida-bench " + " ".join(a for a in argv if "{{" not in a) + "\\n")
if argv[0] == "sweep":
    gens, senders = int(argv[argv.index("--gens") + 1]), int(argv[argv.index("--senders") + 1])
    tmpl = argv[argv.index("--gen-cmd") + 1]
    for i in range(gens):
        subprocess.run(["sh", "-c", tmpl.replace("{{i}}", str(i)).replace("{{senders}}", str(senders // gens))], check=True)
    print('{{"kind": "summary", "complete": true}}')
else:
    print("gen-stderr", file=sys.stderr)
EOF
""")

    def run_sh(self, cmd):
        env = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}")
        env.pop("MIMALLOC_ALLOW_LARGE_OS_PAGES", None)
        r = subprocess.run(["sh", "-c", cmd.replace(f"{z.REMOTE}/", f"{self.tmp}/var/")], env=env,
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        with open(self.calls) as f:
            return f.read().splitlines()

    def test_sweep_command_and_template_expand_for_every_sender(self):
        calls = self.run_sh(z.sweep_command("tcp", 50, RECEIVER_DATA_IP, "0-6"))
        self.assertEqual(calls[0], "taskset -c 0-6 MIMALLOC=unset")
        self.assertEqual(calls[1], "zhida-bench sweep --proto tcp --gens 5 --senders 20 --size 64 --rate-min 10000"
                                   " --rate-max 5000000 --budget-us 50 --gen-cmd")
        gen = ("zhida-bench gen --config /etc/zhida/dut.json --proto tcp --target 10.80.1.100:7000 --senders 4"
               " --size 64 --local-port 40000")
        for i in range(5):
            self.assertEqual(calls[2 + 4 * i: 6 + 4 * i], [
                f"ssh -F {self.tmp}/var/ssh_config sender{i}", "sudo sh -c", "taskset -c 7 MIMALLOC=1", gen])
        self.assertEqual(len(calls), 22)
        with open(os.path.join(self.tmp, "var", "tcp-n20-s64-b50.jsonl")) as f:
            self.assertEqual(json.loads(f.read())["kind"], "summary")
        with open(os.path.join(self.tmp, "var", "gen-tcp-n20-s64-b50.log")) as f:
            self.assertEqual(f.read(), "gen-stderr\n" * 5)

    def test_udp_runs_uncapped(self):
        calls = self.run_sh(z.sweep_command("udp", None, RECEIVER_DATA_IP, "0-6"))
        self.assertNotIn("--budget-us", calls[1])
        self.assertTrue(os.path.exists(os.path.join(self.tmp, "var", "udp-n20-s64-uncapped.coord.log")))


# ---------------------------------------------------------------- provisioning


class FakeWaiter:
    def __init__(self, calls, name):
        self.calls, self.name = calls, name

    def wait(self, **kw):
        self.calls.append(("wait", self.name, kw))


class FakeLaunchEc2:
    def __init__(self):
        self.calls, self.n = [], 0

    def __getattr__(self, name):
        def call(**kw):
            self.calls.append((name, kw))
            if name == "run_instances":
                # out of launch-index order on purpose
                return {"Instances": [{"InstanceId": f"i-{k}", "AmiLaunchIndex": k} for k in (3, 0, 5, 1, 4, 2)]}
            if name == "create_network_interface":
                self.n += 1
                return {"NetworkInterface": {"NetworkInterfaceId": f"eni-{self.n}", "MacAddress": f"m{self.n}",
                                             "PrivateIpAddress": f"10.80.1.{100 + self.n}"}}
            if name == "attach_network_interface":
                return {"AttachmentId": f"att-{kw['NetworkInterfaceId']}"}
            return {}
        return call

    def get_waiter(self, name):
        return FakeWaiter(self.calls, name)

    def get_paginator(self, name):
        calls = self.calls

        class P:
            def paginate(self, **kw):
                calls.append(("paginate", name, kw))
                ids = kw["InstanceIds"]
                for chunk in (ids[:2], ids[2:]):  # two pages
                    yield {"Reservations": [{"Instances": [
                        {"InstanceId": i, "PublicIpAddress": f"pub-{i}", "PrivateIpAddress": f"priv-{i}"}
                        for i in chunk]}]}
        return P()


class FakeSsm:
    def get_parameter(self, Name):
        assert Name == z.AMI_PARAM
        return {"Parameter": {"Value": "ami-arm64"}}


SHARED_INFO = {"vpc": "vpc-1", "subnet": "subnet-1", "zone": "apne1-az4", "sg": "sg-1", "key_path": "/k.pem",
               "bucket": "b", "host_profile": "zhida-bench-host"}


class LaunchTest(unittest.TestCase):
    def test_one_run_instances_of_six_into_the_placement_group_then_eni_attach(self):
        ec2 = FakeLaunchEc2()
        aws = z.Aws("ap-northeast-1", {"ec2": ec2, "iam": None, "s3": None, "ssm": FakeSsm(), "sts": None})
        hosts = aws.launch_hosts(SHARED_INFO, "r1", NOW + datetime.timedelta(hours=2), NOW)
        names = [c[0] for c in ec2.calls]
        self.assertEqual(names.count("run_instances"), 1)
        ri = ec2.calls[names.index("run_instances")][1]
        self.assertEqual((ri["MinCount"], ri["MaxCount"], ri["InstanceType"]), (6, 6, "c7gn.2xlarge"))
        self.assertEqual(ri["Placement"], {"GroupName": "zhida-bench", "AvailabilityZone": "apne1-az4"})
        self.assertNotIn("InstanceMarketOptions", ri, "on-demand, not spot")
        self.assertEqual(ri["InstanceInitiatedShutdownBehavior"], "terminate")
        self.assertEqual({t["ResourceType"] for t in ri["TagSpecifications"]}, {"instance", "volume", "network-interface"})
        self.assertFalse(any("EnaSrdSpecification" in c[1] for c in ec2.calls if isinstance(c[1], dict)),
                         "no ENA Express call")
        wait = names.index("wait")
        eni_calls = [c for c in ec2.calls[wait + 1:] if c[0] in (
            "create_network_interface", "attach_network_interface", "modify_network_interface_attribute")]
        self.assertEqual([c[0] for c in eni_calls], ["create_network_interface", "attach_network_interface",
                                                     "modify_network_interface_attribute"] * 6)
        for k in range(6):
            att, mod = eni_calls[3 * k + 1][1], eni_calls[3 * k + 2][1]
            self.assertEqual((att["InstanceId"], att["DeviceIndex"]), (f"i-{k}", 1))
            self.assertEqual(mod["Attachment"], {"AttachmentId": f"att-eni-{k + 1}", "DeleteOnTermination": True})
        self.assertLess(names.index("run_instances"), wait)
        self.assertEqual([(h["role"], h["id"], h["public_ip"]) for h in hosts],
                         [(r, f"i-{k}", f"pub-i-{k}") for k, r in enumerate(z.ROLES)])
        self.assertEqual(hosts[0]["data_eni"], {"id": "eni-1", "mac": "m1", "ip": "10.80.1.101"})


# ---------------------------------------------------------------- Host (real class, fake subprocess)


class Proc:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


class HostTest(unittest.TestCase):
    def host(self, run):
        return z.Host({"role": "sender3", "id": "i-4", "public_ip": "1.2.3.4"}, "/k.pem", "/rd", run=run,
                      sleep=lambda s: None, clock=iter(range(0, 100000, 5)).__next__)

    def test_failed_scp_is_reported_not_ignored(self):
        h = self.host(lambda argv, **kw: Proc(1, stderr="scp: /var/tmp/zhida/x.jsonl: No such file or directory"))
        err = h.fetch("/var/tmp/zhida/x.jsonl", "/rd")
        self.assertIn("sender3:/var/tmp/zhida/x.jsonl: scp exited 1", err)
        self.assertIn("No such file", err)
        self.assertIsNone(self.host(lambda argv, **kw: Proc(0)).fetch("/f", "/rd"))

    def test_setup_retries_verify_through_the_apply_reboot(self):
        seen = []

        def run(argv, **kw):
            cmd = argv[-1]
            seen.append(cmd)
            if "uname -m" in cmd:
                return Proc(0, "aarch64\n2023\n")
            if "hft-host.sh apply" in cmd:
                return Proc(255, "reboot: rebooting now for: cmdline\n", "Connection closed")
            if "hft-host.sh verify" in cmd:  # host down, then booted
                return Proc(0, "cmdline: ok\n") if sum("verify" in c for c in seen) >= 3 else Proc(255)
            return Proc(0)

        ok, text = self.host(run).setup(b"tar", "s3://b/cache", "tree1", "1.93.0")
        self.assertTrue(ok, text)
        boot = next(c for c in seen if "bootstrap.sh" in c)
        self.assertIn("DPDK_KEY=s3://b/cache/dpdk-25.11.3-aarch64-al2023-2023.tar.gz", boot)
        self.assertIn("BENCH_KEY=s3://b/cache/bench-tree1-aarch64-dpdk25.11.3-rust1.93.0.tar.gz", boot)
        order = [k for c in seen for k in ("bootstrap.sh", "apply", "verify") if k in c]
        self.assertEqual(order, ["bootstrap.sh", "apply", "verify", "verify", "verify"])
        self.assertIn("cmdline: ok", text)

    def test_failed_verify_fails_setup(self):
        def run(argv, **kw):
            cmd = argv[-1]
            if "uname -m" in cmd:
                return Proc(0, "aarch64\n2023\n")
            if "hft-host.sh verify" in cmd:
                return Proc(1, "hugepages: FAIL node 0\n")
            return Proc(0, "b\n")

        ok, text = self.host(run).setup(b"tar", "s3://b/cache", "t", "1.93.0")
        self.assertFalse(ok)
        self.assertIn("hugepages: FAIL", text)


# ---------------------------------------------------------------- six-host orchestration


class World:
    def __init__(self):
        self.calls = []        # (role, command)
        self.fetched = []      # (role, remote)
        self.sweep_exit = {}   # tag -> coordinator exit code
        self.fetch_fail = set()
        self.verify_fail = set()
        self.chrony = {}       # role -> chronyc output
        self.put = {}          # (role, path) -> text
        self.now = NOW
        self.report_code = 0
        self.local_calls = []
        self.clean_calls = []


class FakeHost:
    def __init__(self, world, info, run_dir):
        self.w, self.info, self.role, self.run_dir = world, info, info["role"], run_dir

    def wait_ssh(self):
        pass

    def setup(self, archive, cache_prefix, tree, rust):
        self.w.calls.append((self.role, f"setup {cache_prefix} {tree} {rust}"))
        return self.role not in self.w.verify_fail, f"[{self.role}] verify: {'FAIL' if self.role in self.w.verify_fail else 'ok'}"

    def root(self, command, check=True, **kw):
        self.w.calls.append((self.role, "root " + command))
        if command == "cat /etc/zhida/host.env":
            return Proc(0, HOST_ENV)
        if command == "cat /etc/zhida/dut.json":
            return Proc(0, json.dumps({"ipv4": RECEIVER_DATA_IP + "/24"}))
        if command.startswith("systemctl is-active"):
            return Proc(0, "active\n")
        return Proc(0)

    def ssh(self, command, check=True, timeout=None, input=None):
        self.w.calls.append((self.role, command))
        if command == "chronyc tracking":
            return Proc(0, self.w.chrony.get(self.role, CHRONY_OK))
        if "zhida-bench sweep" in command:
            t = command.split(f"> {z.REMOTE}/")[1].split(".jsonl")[0]
            self.w.now += datetime.timedelta(minutes=5)
            return Proc(self.w.sweep_exit.get(t, 0))
        return Proc(0)

    def put_text(self, remote, text, mode):
        self.w.put[(self.role, remote)] = text

    def fetch(self, remote, local):
        self.w.fetched.append((self.role, remote))
        if (self.role, remote) in self.w.fetch_fail:
            return f"{self.role}:{remote}: scp exited 1"
        with open(os.path.join(local, os.path.basename(remote)), "w") as f:
            f.write("{}\n")
        return None


class FakeAws:
    def __init__(self, world):
        self.w, self.ec2 = world, object()

    def ensure_shared(self, my_ip):
        self.w.calls.append(("aws", f"ensure_shared {my_ip}"))
        return dict(SHARED_INFO)

    def launch_hosts(self, shared, run_id, expires, now):
        self.w.calls.append(("aws", "launch_hosts"))
        return [{"role": r, "id": f"i-{k}", "public_ip": f"pub{k}", "private_ip": f"10.80.1.{10 + k}",
                 "data_eni": {"id": f"eni-{k}", "mac": f"m{k}", "ip": RECEIVER_DATA_IP if k == 0 else f"10.80.1.{110 + k}"}}
                for k, r in enumerate(z.ROLES)]


class RunTest(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.out = tempfile.mkdtemp()

    def local(self, argv, **kw):
        self.w.local_calls.append(argv)
        if argv[0] == "git":
            return Proc(0, b"TAR" if "archive" in argv else ("tree1\n" if "rev-parse" in argv else ""))
        if argv[0] == "ssh-keygen":
            key = argv[argv.index("-f") + 1]
            with open(key, "w") as f:
                f.write("PRIVATE\n")
            with open(key + ".pub", "w") as f:
                f.write("ssh-ed25519 AAAA zhida-bench-r\n")
            return Proc(0)
        if argv[1].endswith("report.py"):
            return Proc(self.w.report_code)
        raise AssertionError(argv)

    def run_bench(self):
        w = self.w
        env = z.Env(aws=lambda region: FakeAws(w), host=lambda info, key, rd: FakeHost(w, info, rd),
                    local=self.local, my_ip=lambda: "203.0.113.9", now=lambda: w.now, sleep=lambda s: None)
        args = argparse.Namespace(region="ap-northeast-1", ttl="2h", out=self.out)

        def clean(ec2, run_id=None, log=print, **kw):
            w.clean_calls.append(run_id)
            return {}

        with mock.patch.object(z.janitor, "clean", clean), mock.patch.object(z.signal, "signal"), \
                contextlib.redirect_stdout(io.StringIO()):
            try:
                code = z.run(args, env)
            except Exception as e:
                code = e
        self.run_dir = os.path.join(self.out, os.listdir(self.out)[0])
        return code

    def sweeps_run(self):
        return [c for r, c in self.w.calls if "zhida-bench sweep" in c]

    def notes(self):
        with open(os.path.join(self.run_dir, "runner.txt")) as f:
            return f.read()

    def test_six_hosts_end_to_end(self):
        self.assertEqual(self.run_bench(), 0)
        w = self.w
        self.assertEqual(sorted(r for r, c in w.calls if c.startswith("setup s3://b/cache tree1 1.93.0")), sorted(z.ROLES))
        cfg = w.put[("sender0", f"{z.REMOTE}/ssh_config")]
        self.assertEqual([l for l in cfg.splitlines() if l.startswith("Host sender")],
                         [f"Host sender{i}" for i in range(5)])
        self.assertIn("HostName 10.80.1.11", cfg)
        self.assertEqual(w.put[("sender0", f"{z.REMOTE}/id_run")], "PRIVATE\n")
        for i in range(5):
            self.assertTrue(any(r == f"sender{i}" and "authorized_keys" in c for r, c in w.calls))
        sweeps = self.sweeps_run()
        self.assertEqual(len(sweeps), 3)
        self.assertTrue(all(r == "sender0" for r, c in w.calls if "zhida-bench sweep" in c))
        for cmd, t in zip(sweeps, z.sweep_tags()):
            argv = shlex.split(cmd.split(" > ")[0])
            self.assertEqual(argv[:3], ["taskset", "-c", "0-6"], "coordinator on HOUSEKEEPING")
            self.assertEqual(argv[argv.index("--gens") + 1], "5")
            tmpl = argv[argv.index("--gen-cmd") + 1]
            for i in range(5):
                self.assertTrue(tmpl.replace("{i}", str(i)).startswith(f"ssh -F {z.REMOTE}/ssh_config sender{i} "))
            self.assertIn(f"--target {RECEIVER_DATA_IP}:7000", tmpl)
            self.assertIn(f"> {z.REMOTE}/{t}.jsonl 2> {z.REMOTE}/{t}.coord.log", cmd)
        serves = [c for r, c in w.calls if r == "receiver" and "zhida-bench serve" in c]
        self.assertEqual(len(serves), 3)
        self.assertTrue(all("taskset -c 7 zhida-bench serve" in c for c in serves))
        for t in z.sweep_tags():
            want = {("sender0", f"{z.REMOTE}/{t}.jsonl"), ("sender0", f"{z.REMOTE}/{t}.coord.log"),
                    ("receiver", f"{z.REMOTE}/dut-counters-{t}.jsonl")}
            want |= {(f"sender{i}", f"{z.REMOTE}/gen-{t}.log") for i in range(5)}
            self.assertLessEqual(want, set(w.fetched))
            self.assertTrue(os.path.exists(os.path.join(self.run_dir, "sender4", f"gen-{t}.log")))
        with open(os.path.join(self.run_dir, "expected.json")) as f:
            self.assertEqual(json.load(f), {"sweeps": z.sweep_tags()})
        report = [a for a in w.local_calls if a[1].endswith("report.py")]
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0][2], self.run_dir)
        self.assertEqual(w.clean_calls, [os.path.basename(self.run_dir)])
        with open(os.path.join(self.run_dir, "host-setup.txt")) as f:
            setup = f.read()
        self.assertIn("[sender4 host.env]", setup)
        self.assertFalse(os.path.exists(os.path.join(self.run_dir, "fetch-errors.txt")))

    def test_failing_sweep_does_not_stop_later_sweeps_and_is_fetched(self):
        self.w.sweep_exit["udp-n20-s64-uncapped"] = 1
        self.w.report_code = 1  # report.py: INCOMPLETE
        self.assertEqual(self.run_bench(), 1)
        self.assertEqual(len(self.sweeps_run()), 3)
        self.assertIn(("sender0", f"{z.REMOTE}/udp-n20-s64-uncapped.jsonl"), self.w.fetched)
        self.assertIn("udp-n20-s64-uncapped: coordinator exit 1", self.notes())
        self.assertEqual(len(self.w.clean_calls), 1)

    def test_scp_failure_is_recorded_and_fails_the_run(self):
        self.w.fetch_fail.add(("sender2", f"{z.REMOTE}/gen-tcp-n20-s64-b50.log"))
        self.assertEqual(self.run_bench(), 1)
        with open(os.path.join(self.run_dir, "fetch-errors.txt")) as f:
            self.assertEqual(f.read(), f"sender2:{z.REMOTE}/gen-tcp-n20-s64-b50.log: scp exited 1\n")
        self.assertEqual(len(self.sweeps_run()), 3)

    def test_ttl_guard_stops_before_a_sweep_that_cannot_finish(self):
        # 2 h TTL; each fake sweep takes 5 min; push the clock so only one sweep fits
        self.w.now = NOW
        orig = FakeHost.ssh

        def slow(host, command, **kw):
            r = orig(host, command, **kw)
            if "zhida-bench sweep" in command:
                host.w.now += datetime.timedelta(minutes=100)
            return r

        self.w.report_code = 1
        with mock.patch.object(FakeHost, "ssh", slow):
            self.assertEqual(self.run_bench(), 1)
        self.assertEqual(len(self.sweeps_run()), 1)
        self.assertIn("stop before tcp-n20-s64-b50: 900 s of TTL left", self.notes())
        self.assertEqual(len(self.w.clean_calls), 1)

    def test_clock_guard_refuses_to_measure(self):
        self.w.chrony["sender3"] = CHRONY_OK.replace("Normal", "Not synchronised")
        self.w.report_code = 1
        self.assertEqual(self.run_bench(), 1)
        self.assertEqual(self.sweeps_run(), [])
        self.assertIn("stop before udp-n20-s64-uncapped: clocks not synchronised within 1 ms: sender3: leap status"
                      " Not synchronised", self.notes())
        self.assertEqual(len([a for a in self.w.local_calls if a[1].endswith("report.py")]), 1)

    def test_failed_verify_aborts_with_teardown(self):
        self.w.verify_fail.add("sender2")
        code = self.run_bench()
        self.assertIsInstance(code, RuntimeError)
        self.assertIn("host setup failed on sender2", str(code))
        self.assertEqual(self.sweeps_run(), [])
        self.assertEqual(len(self.w.clean_calls), 1)
        self.assertEqual([a for a in self.w.local_calls if a[1].endswith("report.py")], [])

    def test_dirty_tree_refused_before_any_aws_call(self):
        def local(argv, **kw):
            return Proc(0, " M bench/src/main.rs\n")

        env = z.Env(aws=lambda r: self.fail("AWS touched"), local=local, now=lambda: NOW)
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(SystemExit, "commit these"):
            z.run(argparse.Namespace(region="r", ttl="2h", out=self.out), env)


class BootstrapTest(unittest.TestCase):
    def test_shell_syntax(self):
        subprocess.run(["sh", "-n", os.path.join(os.path.dirname(os.path.abspath(__file__)), "bootstrap.sh")],
                       check=True)


if __name__ == "__main__":
    unittest.main()
