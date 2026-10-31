#!/usr/bin/env python3
"""Tests with AWS/host fakes and isolated localhost OpenSSH authentication (no AWS, no boto3):
python3 -m unittest discover -s bench/aws"""
import argparse
import contextlib
import datetime
import io
import json
import os
import pwd
import shlex
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import janitor  # noqa: E402
import zhida_aws_bench as z  # noqa: E402

UDP, TCP, WS = z.sweep_tags()

UTC = datetime.timezone.utc
NOW = datetime.datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
RECEIVER_DATA_IP = "10.80.1.100"
HOST_ENV = ("MGMT_IF=ens5\nDATA_IF=ens6\nDATA_PCI=0000:00:06.0\nDATA_MAC=0a:00:00:00:00:01\nNUMA_NODE=0\n"
            "APP_CPU=7\nISOLATED=7\nHOUSEKEEPING=0-6\n")


# zhida-nic's lines (conduit/crates/nic-runner/src/main.rs `line`: "zhida-nic: {key}: {value}"); the
# feed_latency value is binance-feed's Latencies Display, its own test's example
# (latencies_print_nonempty_buckets in conduit/crates/binance-feed/src/lib.rs).
FEED_OUT = """run-binance: 2026-11-01T12:00:00Z zhida-nic feed --seconds 30 on CPU 3 with /etc/zhida/binance.json
zhida-nic: resolved: stream.binance.com -> 54.249.1.2:9443
zhida-nic: feed_connected: wss://stream.binance.com:9443/ws/btcusdt@bookTicker local_port=50000 TLS 1.3
zhida-nic: feed_count: 8
zhida-nic: feed_latency: no_stamp=0 not_ready=0 impossible=1 max_mapping_bound_ns=40 <=0:2 8192..16384:5 1073741824..:1
zhida-nic: feed_update_ids: 100..107
zhida-nic: feed_close: WebSocket close handshake, TLS close_notify, FIN
zhida-nic: port_close: ok
"""
PORT_OPEN_FAILED = """run-binance: 2026-11-01T12:00:00Z zhida-nic feed --seconds 30 on CPU 3 with /etc/zhida/binance.json
zhida-nic: resolved: stream.binance.com -> 54.249.1.2:9443
zhida-nic: port_open_error: PMD lacks the requested RX timestamp offload
"""


def write_exe(path, body):
    with open(path, "w") as f:
        f.write("#!/bin/sh\n" + body)
    os.chmod(path, 0o755)


# ---------------------------------------------------------------- pure parts


class PureTest(unittest.TestCase):
    def test_durations(self):
        self.assertEqual([z.parse_duration(t) for t in ("90m", "2h", "45s", "120", "1.5h")], [5400, 7200, 45, 120, 5400])

    def test_every_run_resource_carries_the_run_id_and_expiry(self):
        expires = datetime.datetime(2026, 11, 1, 13, 30, tzinfo=UTC)
        tags = {t["Key"]: t["Value"] for t in z.run_tags("r1", expires)}
        self.assertEqual(tags[janitor.RUN_TAG], "r1")
        self.assertEqual(datetime.datetime.fromisoformat(tags[janitor.EXPIRES_TAG]), expires)

    def test_user_data_powers_off_at_the_deadline_and_survives_reboot(self):
        deadline = NOW + datetime.timedelta(minutes=30)
        ud = z.user_data(deadline)
        self.assertIn(f"OnCalendar={deadline.strftime('%Y-%m-%d %H:%M:%S')} UTC", ud)
        self.assertIn("Persistent=true", ud)
        self.assertIn("systemctl enable --now zhida-deadman.timer", ud)
        self.assertIn("ExecStart=/usr/bin/systemctl poweroff", ud)

    def test_host_env_needs_every_key(self):
        env = z.parse_host_env(HOST_ENV + "# comment\n")
        self.assertEqual((env["APP_CPU"], env["HOUSEKEEPING"]), ("7", "0-6"))
        self.assertEqual(z.parse_host_env("APP_CPU='3'\n" + HOST_ENV.replace("APP_CPU=7\n", ""))["APP_CPU"], "3")
        with self.assertRaisesRegex(ValueError, "HOUSEKEEPING"):
            z.parse_host_env(HOST_ENV.replace("HOUSEKEEPING=0-6\n", ""))

    def test_data_ip_from_dut_json(self):
        self.assertEqual(z.data_ip('{"version": 1, "ipv4": "10.80.1.23/24"}'), "10.80.1.23")

    def test_ssh_config_lists_the_senders(self):
        cfg = z.ssh_config([f"10.80.1.{10 + i}" for i in range(5)])
        for i in range(5):
            self.assertIn(f"Host sender{i}\n    HostName 10.80.1.{10 + i}\n", cfg)
        for line in ("User ec2-user", "BatchMode yes", f"IdentityFile {z.REMOTE}/id_run",
                     f"UserKnownHostsFile {z.REMOTE}/known_hosts"):
            self.assertIn(line, cfg)


class FeedSummaryTest(unittest.TestCase):
    def test_count_and_latency_buckets_from_the_feed_lines(self):
        # 8 samples: p50 is the 4th (8192..16384), p99 and p99.9 the 8th (the >= 2^30 ns bucket)
        self.assertEqual(z.feed_summary(FEED_OUT, 0), [
            "run-binance.sh exit 0", "messages: 8",
            "latency: 8 samples, no_stamp=0 not_ready=0 impossible=1 max_mapping_bound_ns=40, p50 8192..16384 ns,"
            " p99 1073741824.. ns, p99.9 1073741824.. ns, max 1073741824.. ns"])

    def test_percentile_ranks(self):
        # 1000 samples: p50 = 500th, p99 = 990th, p99.9 = 999th
        text = z.latency_text("no_stamp=3 not_ready=7 impossible=0 max_mapping_bound_ns=12 4096..8192:500 8192..16384:490"
                              " 16384..32768:9 65536..131072:1")
        self.assertEqual(text, "1000 samples, no_stamp=3 not_ready=7 impossible=0 max_mapping_bound_ns=12, p50 4096..8192 ns,"
                               " p99 8192..16384 ns, p99.9 16384..32768 ns, max 65536..131072 ns")

    def test_no_samples(self):
        self.assertEqual(z.latency_text("no_stamp=8 not_ready=0 impossible=0 max_mapping_bound_ns=0"),
                         "0 samples, no_stamp=8 not_ready=0 impossible=0 max_mapping_bound_ns=0")

    def test_clock_jump_is_the_reason(self):
        text = FEED_OUT.replace(FEED_OUT.splitlines()[4], "zhida-nic: feed_latency: latency invalid: NIC clock jumped")
        self.assertEqual(z.feed_summary(text, 0)[2], "latency: latency invalid: NIC clock jumped")

    def test_no_feed_latency_line(self):
        text = "".join(l + "\n" for l in FEED_OUT.splitlines() if "feed_latency" not in l)
        self.assertEqual(z.feed_summary(text, 0), ["run-binance.sh exit 0", "messages: 8", "no feed_latency line"])

    def test_a_failed_feed_shows_its_error(self):
        self.assertEqual(z.feed_summary(PORT_OPEN_FAILED, 3), [
            "run-binance.sh exit 3", "no feed_latency line", "feed output ends:", *PORT_OPEN_FAILED.splitlines()])
        self.assertEqual(z.feed_summary(None, None), ["run-binance.sh did not finish", "no feed output fetched"])


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
                  f'echo "taskset $1 $2" >>{self.calls}\n'
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
        r = subprocess.run(["sh", "-c", cmd.replace(f"{z.REMOTE}/", f"{self.tmp}/var/")], env=env,
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        with open(self.calls) as f:
            return f.read().splitlines()

    def test_sweep_command_and_template_expand_for_every_sender(self):
        calls = self.run_sh(z.sweep_command("tcp", 50, RECEIVER_DATA_IP, "0-6"))
        self.assertEqual(calls[0], "taskset -c 0-6")
        self.assertEqual(calls[1], "zhida-bench sweep --proto tcp --gens 3 --senders 12 --rate-min 10000"
                                   " --rate-max 5000000 --budget-us 50"
                                   " --step-ms 500 --warmup-ms 250 --drain-ms 200 --gen-cmd")
        gen = ("zhida-bench gen --config /etc/zhida/dut.json --proto tcp --target 10.80.1.100:7000 --senders 4"
               " --size 64 --local-port 40000")
        for i in range(z.SENDERS):
            self.assertEqual(calls[2 + 4 * i: 6 + 4 * i], [
                f"ssh -F {self.tmp}/var/ssh_config sender{i}", "sudo sh -c", "taskset -c 7", gen])
        self.assertEqual(len(calls), 2 + 4 * z.SENDERS)
        with open(os.path.join(self.tmp, "var", f"{TCP}.jsonl")) as f:
            self.assertEqual(json.loads(f.read())["kind"], "summary")
        with open(os.path.join(self.tmp, "var", f"gen-{TCP}.log")) as f:
            self.assertEqual(f.read(), "gen-stderr\n" * z.SENDERS)

    def test_udp_runs_uncapped(self):
        calls = self.run_sh(z.sweep_command("udp", None, RECEIVER_DATA_IP, "0-6"))
        self.assertNotIn("--budget-us", calls[1])
        self.assertTrue(os.path.exists(os.path.join(self.tmp, "var", f"{UDP}.coord.log")))


# ---------------------------------------------------------------- provisioning


class FakeWaiter:
    def __init__(self, calls, name):
        self.calls, self.name = calls, name

    def wait(self, **kw):
        self.calls.append(("wait", self.name, kw))


class FakeLaunchEc2:
    """Records every call; `record` (the runner's Created.add) lands in the same list."""

    CREATED = {"create_vpc": {"Vpc": {"VpcId": "vpc-1"}}, "create_subnet": {"Subnet": {"SubnetId": "subnet-1"}},
               "create_internet_gateway": {"InternetGateway": {"InternetGatewayId": "igw-1"}},
               "create_route_table": {"RouteTable": {"RouteTableId": "rtb-1"}},
               "create_security_group": {"GroupId": "sg-1"}, "create_key_pair": {"KeyMaterial": "PRIVATE\n"},
               "allocate_address": {"AllocationId": "eipalloc-1"},
               "describe_instance_type_offerings": {"InstanceTypeOfferings": [{"Location": "zone-b"},
                                                                              {"Location": "zone-a"}]}}

    def __init__(self):
        self.calls, self.n = [], 0

    def record(self, kind, rid):
        self.calls.append(("record", (kind, rid)))

    def __getattr__(self, name):
        def call(**kw):
            self.calls.append((name, kw))
            if name == "run_instances":
                # out of launch-index order on purpose
                return {"Instances": [{"InstanceId": f"i-{k}", "AmiLaunchIndex": k}
                                      for k in sorted(range(kw["MinCount"]), key=lambda k: (k % 2, -k))]}
            if name == "create_network_interface":
                self.n += 1
                return {"NetworkInterface": {"NetworkInterfaceId": f"eni-{self.n}", "MacAddress": f"m{self.n}",
                                             "PrivateIpAddress": f"10.80.1.{100 + self.n}"}}
            if name == "attach_network_interface":
                return {"AttachmentId": f"att-{kw['NetworkInterfaceId']}"}
            return self.CREATED.get(name, {})
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


NET = {"name": "zhida-bench-r1", "subnet": "subnet-1", "zone": "zone-a", "sg": "sg-1"}


class LaunchTest(unittest.TestCase):
    def test_network_is_per_run_tagged_and_recorded_as_created(self):
        ec2 = FakeLaunchEc2()
        key = os.path.join(tempfile.mkdtemp(), "key.pem")
        net = launch_aws(ec2).create_network("r1", NOW, "203.0.113.9", ec2.record, key, "c7gn.2xlarge", "cluster")
        self.assertEqual(net, NET)
        creates = [c[:2] for c in ec2.calls if c[0].startswith("create_") and c[0] != "create_route"]
        for n, kw in creates:
            tags = {t["Key"]: t["Value"] for t in kw["TagSpecifications"][0]["Tags"]}
            self.assertEqual(tags[janitor.RUN_TAG], "r1", n)
        # each ID recorded right after its create, before anything uses it
        recorded = {"create_vpc": ("vpc", "vpc-1"), "create_subnet": ("subnet", "subnet-1"),
                    "create_internet_gateway": ("internet-gateway", "igw-1"),
                    "create_route_table": ("route-table", "rtb-1"), "create_security_group": ("security-group", "sg-1"),
                    "create_placement_group": ("placement-group", "zhida-bench-r1"),
                    "create_key_pair": ("key-pair", "zhida-bench-r1")}
        names = [c[0] for c in ec2.calls]
        for create, rec in recorded.items():
            i = names.index(create)
            self.assertEqual(ec2.calls[i + 1], ("record", rec))
        self.assertEqual(len(creates), len(recorded))
        sg_rules = ec2.calls[names.index("authorize_security_group_ingress")][1]["IpPermissions"]
        self.assertEqual(sg_rules[1]["IpRanges"], [{"CidrIp": "203.0.113.9/32"}])
        self.assertEqual(ec2.calls[names.index("create_placement_group")][1]["Strategy"], "cluster")
        self.assertEqual(oct(os.stat(key).st_mode & 0o777), "0o600")
        with open(key) as f:
            self.assertEqual(f.read(), "PRIVATE\n")

    def test_one_run_instances_of_every_host_into_the_placement_group_then_eni_attach(self):
        ec2 = FakeLaunchEc2()
        aws = launch_aws(ec2)
        hosts = aws.launch_hosts(NET, "r1", NOW + datetime.timedelta(hours=2), ec2.record, "c7gn.2xlarge", z.ROLES)
        names = [c[0] for c in ec2.calls]
        self.assertEqual(names.count("run_instances"), 1)
        ri = ec2.calls[names.index("run_instances")][1]
        self.assertEqual((ri["MinCount"], ri["MaxCount"], ri["InstanceType"]), (4, 4, "c7gn.2xlarge"))
        self.assertEqual(ri["Placement"], {"GroupName": "zhida-bench-r1", "AvailabilityZone": "zone-a"})
        self.assertEqual(ri["KeyName"], "zhida-bench-r1")
        self.assertNotIn("InstanceMarketOptions", ri, "on-demand, not spot")
        self.assertEqual(ri["InstanceInitiatedShutdownBehavior"], "terminate")
        self.assertEqual({t["ResourceType"] for t in ri["TagSpecifications"]}, {"instance", "volume", "network-interface"})
        self.assertFalse(any("EnaSrdSpecification" in c[1] for c in ec2.calls if isinstance(c[1], dict)),
                         "no ENA Express call")
        self.assertNotIn("create_tags", names, "tags only via TagSpecifications")
        wait = names.index("wait")
        self.assertEqual(ec2.calls[names.index("run_instances") + 1: wait],
                         [("record", ("instance", f"i-{k}")) for k in range(len(z.ROLES))], "recorded before the waiter")
        self.assertEqual([c[1][1] for c in ec2.calls[wait:] if c[0] == "record"],
                         [f"eni-{k}" for k in range(1, len(z.ROLES) + 1)])
        eni_calls = [c for c in ec2.calls[wait + 1:] if c[0] in (
            "create_network_interface", "attach_network_interface", "modify_network_interface_attribute")]
        self.assertEqual([c[0] for c in eni_calls], ["create_network_interface", "attach_network_interface",
                                                     "modify_network_interface_attribute"] * len(z.ROLES))
        for k in range(len(z.ROLES)):
            att, mod = eni_calls[3 * k + 1][1], eni_calls[3 * k + 2][1]
            self.assertEqual((att["InstanceId"], att["DeviceIndex"]), (f"i-{k}", 1))
            self.assertEqual(mod["Attachment"], {"AttachmentId": f"att-eni-{k + 1}", "DeleteOnTermination": True})
        self.assertLess(names.index("run_instances"), wait)
        self.assertEqual([(h["role"], h["id"], h["public_ip"]) for h in hosts],
                         [(r, f"i-{k}", f"pub-i-{k}") for k, r in enumerate(z.ROLES)])
        self.assertEqual(hosts[0]["data_eni"], {"id": "eni-1", "mac": "m1", "ip": "10.80.1.101"})

    def test_one_instance_in_a_precision_time_group_with_an_elastic_ip_on_its_data_eni(self):
        ec2 = FakeLaunchEc2()
        aws = launch_aws(ec2)
        key = os.path.join(tempfile.mkdtemp(), "key.pem")
        expires = NOW + datetime.timedelta(hours=2)
        net = aws.create_network("r1", expires, "203.0.113.9", ec2.record, key, "m7g.xlarge", "precision-time")
        [host] = aws.launch_hosts(net, "r1", expires, ec2.record, "m7g.xlarge", ["binance"])
        aws.attach_eip(host["data_eni"], "r1", expires, ec2.record)
        names = [c[0] for c in ec2.calls]
        offer = ec2.calls[names.index("describe_instance_type_offerings")][1]
        self.assertEqual(offer["Filters"], [{"Name": "instance-type", "Values": ["m7g.xlarge"]}])
        pg = ec2.calls[names.index("create_placement_group")][1]
        self.assertEqual((pg["GroupName"], pg["Strategy"]), ("zhida-bench-r1", "precision-time"))
        ri = ec2.calls[names.index("run_instances")][1]
        self.assertEqual((ri["MinCount"], ri["MaxCount"], ri["InstanceType"]), (1, 1, "m7g.xlarge"))
        self.assertEqual(ri["Placement"], {"GroupName": "zhida-bench-r1", "AvailabilityZone": "zone-a"})
        self.assertLess(names.index("create_placement_group"), names.index("run_instances"))
        self.assertIn(f"OnCalendar={expires.strftime('%Y-%m-%d %H:%M:%S')} UTC", ri["UserData"])
        self.assertEqual((host["role"], host["id"]), ("binance", "i-0"))
        tail = [c for c in ec2.calls[names.index("run_instances") + 2:] if c[0] != "paginate"]
        self.assertEqual(tail, [
            ("wait", "instance_running", {"InstanceIds": ["i-0"]}),
            ("create_network_interface", {"SubnetId": "subnet-1", "Groups": ["sg-1"], "TagSpecifications": [
                {"ResourceType": "network-interface", "Tags": z.run_tags("r1", expires)}]}),
            ("record", ("network-interface", "eni-1")),
            ("attach_network_interface", {"NetworkInterfaceId": "eni-1", "InstanceId": "i-0", "DeviceIndex": 1}),
            ("modify_network_interface_attribute", {"NetworkInterfaceId": "eni-1", "Attachment": {
                "AttachmentId": "att-eni-1", "DeleteOnTermination": True}}),
            ("allocate_address", {"Domain": "vpc", "TagSpecifications": [
                {"ResourceType": "elastic-ip", "Tags": z.run_tags("r1", expires)}]}),
            ("record", ("address", "eipalloc-1")),
            ("associate_address", {"AllocationId": "eipalloc-1", "NetworkInterfaceId": "eni-1",
                                   "PrivateIpAddress": "10.80.1.101"}),
        ])
        self.assertFalse(any("EnaSrdSpecification" in c[1] for c in ec2.calls if isinstance(c[1], dict)),
                         "no ENA Express call")
        self.assertEqual([c[1] for c in ec2.calls if c[0] == "record"], [
            ("vpc", "vpc-1"), ("subnet", "subnet-1"), ("internet-gateway", "igw-1"), ("route-table", "rtb-1"),
            ("security-group", "sg-1"), ("placement-group", "zhida-bench-r1"), ("key-pair", "zhida-bench-r1"),
            ("instance", "i-0"), ("network-interface", "eni-1"), ("address", "eipalloc-1")])


# ---------------------------------------------------------------- Host (real class, fake subprocess)


class Proc:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


class OpenSshIdentityTest(unittest.TestCase):
    def test_run_key_authenticates_despite_unrelated_agent_keys(self):
        tools = {name: shutil.which(name) for name in ("ssh", "scp", "sshd", "ssh-keygen", "ssh-agent", "ssh-add")}
        missing = [name for name, path in tools.items() if path is None]
        if missing:
            self.skipTest("required OpenSSH executable unavailable: " + ", ".join(missing))
        with tempfile.TemporaryDirectory() as tmp:
            def command(name, *args, **kwargs):
                return subprocess.run([tools[name], *args], capture_output=True, text=True,
                                      check=True, timeout=10, **kwargs)

            keys = [os.path.join(tmp, name) for name in ("host", "id_run", "unrelated1", "unrelated2")]
            for key in keys:
                command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", key)
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            user = pwd.getpwuid(os.getuid()).pw_name
            config = os.path.join(tmp, "sshd_config")
            with open(config, "w") as f:
                f.write(f"ListenAddress 127.0.0.1\nPort {port}\nHostKey {keys[0]}\n"
                        f"AuthorizedKeysFile {keys[1]}.pub\nPidFile {tmp}/sshd.pid\nAllowUsers {user}\n"
                        "StrictModes no\nUsePAM no\nUseDNS no\nPermitRootLogin yes\n"
                        "PasswordAuthentication no\nKbdInteractiveAuthentication no\n"
                        "PubkeyAuthentication yes\nMaxAuthTries 1\nSubsystem sftp internal-sftp\n")
            empty_config = os.path.join(tmp, "empty_config")
            with open(empty_config, "w"):
                pass
            agent_socket = os.path.join(tmp, "agent.sock")
            agent_env = dict(os.environ, SSH_AUTH_SOCK=agent_socket)
            agent_env.pop("SSH_AGENT_PID", None)
            agent = subprocess.Popen([tools["ssh-agent"], "-D", "-a", agent_socket],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            server = None
            try:
                server = subprocess.Popen([tools["sshd"], "-D", "-e", "-f", config],
                                          stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    for process in (agent, server):
                        if process.poll() is not None:
                            self.fail(f"OpenSSH fixture exited {process.returncode}: "
                                      f"{process.stderr.read().decode(errors='replace')}")
                    if os.path.exists(agent_socket):
                        try:
                            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                                break
                        except OSError:
                            pass
                    time.sleep(0.02)
                else:
                    self.fail("isolated SSH server/agent did not become ready")
                command("ssh-add", *keys[2:], env=agent_env)
                unrestricted = False

                def run(argv, **kwargs):
                    name = argv[0]
                    args = [arg.replace("ec2-user@", f"{user}@", 1) for arg in argv[1:]]
                    if unrestricted:
                        args = [arg.replace("IdentitiesOnly=yes", "IdentitiesOnly=no") for arg in args]
                    # Only redirect the production client's username/port to this local fixture.
                    return subprocess.run([tools[name], "-F", empty_config,
                                           "-p" if name == "ssh" else "-P", str(port), *args],
                                          env=agent_env, **kwargs)

                host = z.Host({"role": "sender0", "id": "i-0", "public_ip": "127.0.0.1"},
                              keys[1], tmp, run=run)
                remote = os.path.join(tmp, "source")
                local = os.path.join(tmp, "fetched")
                with open(remote, "w") as f:
                    f.write("run-key authenticated transfer\n")
                unrestricted = True
                rejected = host.ssh("printf authenticated", check=False)
                self.assertNotEqual(rejected.returncode, 0)
                self.assertIn("Too many authentication failures", rejected.stderr)
                self.assertIsNotNone(host.fetch(remote, local))
                self.assertFalse(os.path.exists(local))
                unrestricted = False
                self.assertEqual(host.ssh("printf authenticated").stdout, "authenticated")
                self.assertIsNone(host.fetch(remote, local))
                with open(local) as f:
                    self.assertEqual(f.read(), "run-key authenticated transfer\n")
                sender_config = os.path.join(tmp, "sender_config")
                with mock.patch.object(z, "REMOTE", tmp):
                    with open(sender_config, "w") as f:
                        f.write(z.ssh_config(["127.0.0.1"]))
                sender = command("ssh", "-F", sender_config, "-p", str(port), "-l", user,
                                 "sender0", "printf sender-authenticated", env=agent_env)
                self.assertEqual(sender.stdout, "sender-authenticated")
            finally:
                for process in (server, agent):
                    if process is None:
                        continue
                    if process.poll() is None:
                        process.terminate()
                    try:
                        process.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate(timeout=5)


class HostTest(unittest.TestCase):
    def host(self, run):
        return z.Host({"role": "sender3", "id": "i-4", "public_ip": "1.2.3.4"}, "/k.pem", "/rd", run=run,
                      sleep=lambda s: None, clock=iter(range(0, 100000, 5)).__next__)

    def test_wait_ssh_deadline_reports_last_exit_and_stderr(self):
        run = mock.Mock(side_effect=[
            Proc(255, stderr="Connection refused"),
            Proc(255, stderr="Permission denied (publickey).\n"),
        ])
        h = self.host(run)
        h._sleep = mock.Mock()
        with self.assertRaises(RuntimeError) as raised:
            h.wait_ssh(limit=15)
        self.assertIn("sender3: no SSH within 15 s", str(raised.exception))
        self.assertIn("SSH exited 255: Permission denied (publickey).", str(raised.exception))
        self.assertNotIn("Connection refused", str(raised.exception))
        self.assertEqual(run.call_count, 2)
        self.assertEqual(h._sleep.call_args_list, [mock.call(5), mock.call(5)])
        self.assertTrue(all(call.args[0][-1] == "true" for call in run.call_args_list))
        self.assertTrue(all(call.kwargs["timeout"] == z.SSH_S for call in run.call_args_list))

    def test_wait_ssh_recovers_without_changing_success_or_retry_delay(self):
        run = mock.Mock(side_effect=[Proc(255, stderr="Connection refused"), Proc(0)])
        h = self.host(run)
        h._sleep = mock.Mock()
        self.assertIsNone(h.wait_ssh(limit=15))
        self.assertEqual(run.call_count, 2)
        h._sleep.assert_called_once_with(5)

    def test_wait_ssh_preserves_individual_ssh_timeout(self):
        timeout = subprocess.TimeoutExpired("ssh", z.SSH_S)
        run = mock.Mock(side_effect=timeout)
        h = self.host(run)
        h._sleep = mock.Mock()
        with self.assertRaises(subprocess.TimeoutExpired) as raised:
            h.wait_ssh()
        self.assertIs(raised.exception, timeout)
        run.assert_called_once()
        h._sleep.assert_not_called()

    def test_streamed_progress_arrives_before_the_remote_command_exits(self):
        with tempfile.TemporaryDirectory() as tmp:
            ack = os.path.join(tmp, "ack")
            write_exe(os.path.join(tmp, "ssh"),
                      f'echo "processes synced" >&2\n'
                      f'while [ ! -e {shlex.quote(ack)} ]; do sleep 0.05; done\nexit 7\n')
            code = (
                f"import sys; sys.path.insert(0, {os.path.dirname(z.__file__)!r}); "
                "import zhida_aws_bench as z; "
                "h = z.Host({'role':'sender0','public_ip':'unused'}, '/key', '/run'); "
                "sys.exit(h.ssh('sweep', check=False, timeout=10, stream=True).returncode)"
            )
            env = dict(os.environ, PATH=f"{tmp}:{os.environ['PATH']}")
            with subprocess.Popen([sys.executable, "-c", code], env=env,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as child:
                try:
                    readable, _, _ = select.select([child.stdout], [], [], 5)
                    self.assertTrue(readable, "progress was buffered until command exit")
                    self.assertEqual(child.stdout.readline(), b"processes synced\n")
                    self.assertIsNone(child.poll(), "command must still be waiting")
                finally:
                    with open(ack, "w"):
                        pass
                    child.communicate(timeout=15)
                self.assertEqual(child.returncode, 7, "streaming must preserve sweep failure")

    def test_failed_scp_is_reported_not_ignored(self):
        h = self.host(lambda argv, **kw: Proc(1, stderr="scp: /var/tmp/zhida/x.jsonl: No such file or directory"))
        err = h.fetch("/var/tmp/zhida/x.jsonl", "/rd")
        self.assertIn("sender3:/var/tmp/zhida/x.jsonl: scp exited 1", err)
        self.assertIn("No such file", err)
        self.assertIsNone(self.host(lambda argv, **kw: Proc(0)).fetch("/f", "/rd"))

    def test_a_hung_ssh_or_scp_is_bounded_by_its_timeout(self):
        bin_dir = tempfile.mkdtemp()
        for exe in ("ssh", "scp"):
            write_exe(os.path.join(bin_dir, exe), "exec sleep 30\n")
        h = z.Host({"role": "receiver", "id": "i-0", "public_ip": "1.2.3.4"}, "/k.pem", "/rd")  # real subprocess.run
        with mock.patch.dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}"), mock.patch.object(z, "SSH_S", 0.5):
            start = time.monotonic()
            self.assertEqual(h.fetch(f"{z.REMOTE}/x.jsonl", "/rd"), f"receiver:{z.REMOTE}/x.jsonl: scp timed out after 0.5 s")
            with self.assertRaises(subprocess.TimeoutExpired):
                h.root(f"systemctl stop {z.RECEIVER_UNIT}", check=False)
            self.assertLess(time.monotonic() - start, 5)

    def test_setup_retries_verify_through_the_apply_reboot_then_builds(self):
        seen = []

        def run(argv, **kw):
            cmd = argv[-1]
            seen.append(cmd)
            if "hft-host.sh apply" in cmd:
                return Proc(255, "reboot: rebooting now for: cmdline\n", "Connection closed")
            if "hft-host.sh verify" in cmd:
                attempt = sum("verify" in c for c in seen)
                if attempt == 1:
                    return Proc(1, "cmdline: FAIL reboot needed\n")
                if attempt == 2:
                    return Proc(255, stderr="Connection refused")
                if attempt == 3:
                    raise subprocess.TimeoutExpired(argv, z.SSH_S)
                return Proc(0, "cmdline: ok\n")
            if "systemctl is-failed" in cmd:
                return Proc(1, "inactive\n")
            return Proc(0)

        ok, text = self.host(run).setup(b"tar")
        self.assertTrue(ok, text)
        order = [k for c in seen for k in ("apply", "verify", "bootstrap.sh") if k in c]
        self.assertEqual(order, ["apply", "verify", "verify", "verify", "verify", "bootstrap.sh"])
        self.assertEqual(sum("systemctl is-failed" in c for c in seen), 1)
        self.assertFalse(any("journalctl" in c for c in seen))
        self.assertIn(f"SRC={z.SRC_DIR} sh {z.SRC_DIR}/bench/aws/bootstrap.sh", seen[-1])
        self.assertIn("cmdline: ok", text)

    def test_failed_verify_fails_setup_without_building(self):
        seen = []

        def run(argv, **kw):
            seen.append(argv[-1])
            if "hft-host.sh verify" in argv[-1]:
                return Proc(1, "hugepages: FAIL node 0\n")
            return Proc(0, "b\n")

        ok, text = self.host(run).setup(b"tar")
        self.assertFalse(ok)
        self.assertIn("hugepages: FAIL", text)
        self.assertFalse(any("bootstrap.sh" in c for c in seen))

    def test_apply_failure_collects_diagnostics_without_verifying_or_building(self):
        h = self.host(lambda argv, **kw: Proc())
        h.root = mock.Mock(side_effect=[
            Proc(1, "apply output\n", "apply error\n"), Proc(0, "boot service journal\n"),
        ])
        ok, text = h.setup(b"tar")
        self.assertFalse(ok)
        self.assertIn("apply exited 1: apply error", text)
        self.assertIn("boot service journal", text)
        self.assertEqual(h.root.call_count, 2)

    def test_failed_boot_service_reports_journal_promptly_without_building(self):
        h = self.host(lambda argv, **kw: Proc())
        h._sleep = mock.Mock()
        h.root = mock.Mock(side_effect=[
            Proc(100, "reboot requested\n"),
            Proc(1, "data: FAIL data NIC on ena\n", "verify stderr\n"),
            Proc(0, "failed\n"),
            Proc(0, "zhida-host.service: Failed\nbind: incompatible vfio module\n"
                    "loaded vfio_pci_core srcversion:\nOLD\non-disk vfio_pci_core srcversion:\nNEW\n"),
        ])
        ok, text = h.setup(b"tar")
        self.assertFalse(ok)
        self.assertIn("zhida-host.service failed", text)
        self.assertIn("data: FAIL data NIC on ena", text)
        self.assertIn("verify stderr", text)
        self.assertIn("bind: incompatible vfio module", text)
        self.assertIn("srcversion:\nOLD", text)
        self.assertIn("srcversion:\nNEW", text)
        h._sleep.assert_not_called()
        self.assertEqual(h.root.call_count, 4)  # no benchmark build
        command = h.root.call_args.args[0]
        for expected in ("systemctl status zhida-host.service --no-pager",
                         "journalctl -u zhida-host.service -b --no-pager",
                         "cat /sys/module/vfio_pci_core/srcversion",
                         "modinfo -F srcversion vfio_pci_core"):
            self.assertIn(expected, command)

    def test_transient_service_probe_ssh_failures_still_allow_success(self):
        for transient in (Proc(255, "failed\n", "Connection closed"),
                          subprocess.TimeoutExpired("ssh", z.SSH_S)):
            with self.subTest(transient=transient):
                h = self.host(lambda argv, **kw: Proc())
                h.root = mock.Mock(side_effect=[
                    Proc(100), Proc(1, "data: FAIL on ena\n"), transient,
                    Proc(0, "data: ok vfio-pci\n"), Proc(0),
                ])
                ok, text = h.setup(b"tar")
                self.assertTrue(ok, text)
                self.assertIn("bootstrap.sh", h.root.call_args.args[0])
                self.assertNotIn("setup diagnostics", text)

    def test_verify_timeout_without_failed_service_collects_diagnostics(self):
        h = self.host(lambda argv, **kw: Proc())
        h._clock = iter((0, z.SETUP_REBOOT_S)).__next__
        h.root = mock.Mock(side_effect=[
            Proc(100), Proc(1, "data: FAIL on ena\n"), Proc(1, "activating\n"),
            Proc(1, "service status and journal\n", "modinfo: module not found\n"),
        ])
        ok, text = h.setup(b"tar")
        self.assertFalse(ok)
        self.assertIn(f"verify did not pass within {z.SETUP_REBOOT_S} s", text)
        self.assertIn("service status and journal", text)
        self.assertIn("modinfo: module not found", text)
        self.assertNotIn("zhida-host.service failed", text)
        self.assertEqual(h.root.call_count, 4)

    def test_diagnostics_ssh_failure_preserves_verify_failure(self):
        for failure in (Proc(255, stderr="Connection refused"),
                        subprocess.TimeoutExpired("ssh", z.SSH_S)):
            with self.subTest(failure=failure):
                h = self.host(lambda argv, **kw: Proc())
                h.root = mock.Mock(side_effect=[
                    Proc(100), Proc(1, "data: FAIL on ena\n"), Proc(0, "failed\n"), failure,
                ])
                ok, text = h.setup(b"tar")
                self.assertFalse(ok)
                self.assertIn("data: FAIL on ena", text)
                self.assertIn("setup diagnostics", text)
                self.assertIn("Connection refused" if isinstance(failure, Proc) else "SSH timed out", text)

    def test_verify_ssh_timeout_at_deadline_reports_diagnostics(self):
        h = self.host(lambda argv, **kw: Proc())
        h._clock = iter((0, z.SETUP_REBOOT_S)).__next__
        h.root = mock.Mock(side_effect=[
            Proc(255), subprocess.TimeoutExpired("ssh", z.SSH_S), Proc(0, "boot journal\n"),
        ])
        ok, text = h.setup(b"tar")
        self.assertFalse(ok)
        self.assertIn("verify SSH timed out", text)
        self.assertIn("boot journal", text)
        self.assertEqual(h.root.call_count, 3)  # no service probe on an unreachable host


# ---------------------------------------------------------------- six-host orchestration


class World:
    def __init__(self):
        self.calls = []        # (role, command)
        self.fetched = []      # (role, remote)
        self.sweep_exit = {}   # tag -> coordinator exit code
        self.sweep_timeouts = []
        self.fetch_fail = set()
        self.verify_fail = set()
        self.put = {}          # (role, path) -> text
        self.report_code = 0
        self.local_calls = []
        self.clean_calls = []  # (run id, recorded IDs)
        self.left_after_teardown = []
        self.network_fails = False
        self.interrupt_sweep = None     # tag whose coordinator gets the first Ctrl-C
        self.interrupt_teardown = False  # a second Ctrl-C inside teardown
        self.teardown_done = False
        self.hang = False               # receiver stop and every scp time out
        self.offered = True             # the instance type is offered in the region
        self.feed_code = 0              # run-binance.sh's exit code
        self.feed_timeouts = []
        self.feed_hang = False          # the run-binance.sh ssh times out
        self.feed_text = FEED_OUT


class FakeHost:
    def __init__(self, world, info, run_dir):
        self.w, self.info, self.role, self.run_dir = world, info, info["role"], run_dir

    def wait_ssh(self):
        pass

    def install(self, archive):
        self.w.calls.append((self.role, f"install {archive!r}"))
        return self.role not in self.w.verify_fail, f"[{self.role}] verify: {'FAIL' if self.role in self.w.verify_fail else 'ok'}"

    def setup(self, archive):
        self.w.calls.append((self.role, f"setup {archive!r}"))
        return self.role not in self.w.verify_fail, f"[{self.role}] verify: {'FAIL' if self.role in self.w.verify_fail else 'ok'}"

    def root(self, command, check=True, **kw):
        self.w.calls.append((self.role, "root " + command))
        if command == "cat /etc/zhida/host.env":
            return Proc(0, HOST_ENV)
        if command == "cat /etc/zhida/dut.json":
            return Proc(0, json.dumps({"ipv4": RECEIVER_DATA_IP + "/24"}))
        if command.startswith("systemctl is-active"):
            return Proc(0, "active\n")
        if command.startswith("systemctl stop") and self.w.hang:
            raise subprocess.TimeoutExpired(command, z.SSH_S)
        if "run-binance.sh" in command:
            self.w.feed_timeouts.append(kw["timeout"])
            if self.w.feed_hang:
                raise subprocess.TimeoutExpired(command, kw["timeout"])
            return Proc(self.w.feed_code)
        return Proc(0)

    def ssh(self, command, check=True, timeout=None, input=None, stream=False):
        self.w.calls.append((self.role, command))
        if "zhida-bench sweep" in command:
            t = command.split(f"> {z.REMOTE}/")[1].split(".jsonl")[0]
            self.w.sweep_timeouts.append(timeout)
            if t == self.w.interrupt_sweep:
                os.kill(os.getpid(), signal.SIGINT)
                time.sleep(0.01)
            return Proc(self.w.sweep_exit.get(t, 0))
        return Proc(0)

    def put_text(self, remote, text, mode):
        self.w.put[(self.role, remote)] = text

    def fetch(self, remote, local):
        self.w.fetched.append((self.role, remote))
        if (self.role, remote) in self.w.fetch_fail:
            return f"{self.role}:{remote}: scp exited 1"
        if self.w.hang:
            return f"{self.role}:{remote}: scp timed out after {z.SSH_S} s"
        with open(os.path.join(local, os.path.basename(remote)), "w") as f:
            f.write(self.w.feed_text if remote == z.FEED_OUT else "{}\n")
        return None


class FakeSts:
    def __init__(self, error=None):
        self.error = error

    def get_caller_identity(self):
        if self.error:
            raise self.error
        return {"Account": "123456789012", "Arn": "arn:aws:iam::123456789012:user/owner"}


class FakeQuotas:
    def __init__(self, value, error=None):
        self.value, self.error = value, error

    def get_service_quota(self, ServiceCode, QuotaCode):
        assert (ServiceCode, QuotaCode) == ("ec2", "L-1216C47A")
        if self.error:
            raise self.error
        return {"Quota": {"Value": float(self.value)}}


def running(itype, vcpus, spot=False):
    i = {"InstanceType": itype, "CpuOptions": {"CoreCount": vcpus, "ThreadsPerCore": 1}}
    if spot:
        i["InstanceLifecycle"] = "spot"
    return i


class FakeQuotaEc2:
    """describe_instances (running/pending) over two pages; the zones offering every type."""

    def __init__(self, instances, zones=("zone-a",)):
        self.instances, self.zones = instances, zones

    def describe_instance_type_offerings(self, LocationType, Filters):
        assert LocationType == "availability-zone"
        return {"InstanceTypeOfferings": [{"Location": z} for z in self.zones]}

    def get_paginator(self, name):
        assert name == "describe_instances"
        instances = self.instances

        class P:
            def paginate(self, Filters):
                assert Filters == [{"Name": "instance-state-name", "Values": ["pending", "running"]}]
                yield {"Reservations": [{"Instances": instances[:1]}]}
                yield {"Reservations": [{"Instances": instances[1:]}]}
        return P()


class ClientError(Exception):
    def __init__(self, code, message):
        super().__init__(f"An error occurred ({code}): {message}")
        self.response = {"Error": {"Code": code, "Message": message}}


def launch_aws(ec2):
    return z.Aws("ap-northeast-1", {"ec2": ec2, "ssm": FakeSsm(), "sts": None, "service-quotas": None})


class PreflightTest(unittest.TestCase):
    def check(self, quota=96, instances=(), sts_error=None, quota_error=None, local=None, zones=("zone-a",),
              itype="c7gn.2xlarge", vcpus=48):
        aws = z.Aws("ap-northeast-1", {"ec2": FakeQuotaEc2(list(instances), zones), "ssm": None,
                                       "sts": FakeSts(sts_error), "service-quotas": FakeQuotas(quota, quota_error)})
        env = z.Env(profile_region=lambda: "ap-southeast-2", aws=lambda region: aws, local=local or (lambda argv, **kw: Proc(0, "")))
        return {name: (ok, detail) for name, ok, detail in z.preflight(env, "ap-northeast-1", itype, vcpus)[0]}

    def test_all_pass(self):
        r = self.check(quota=96, instances=[running("c7g.4xlarge", 16), running("m5.large", 2, spot=True)])
        self.assertEqual(r["source"], (True, "clean, HEAD has deploy/hft-host.sh"))
        self.assertEqual(r["credentials"], (True, "account 123456789012 as arn:aws:iam::123456789012:user/owner"))
        self.assertEqual(r["quota"], (True, "L-1216C47A 96 vCPUs, 16 in use, 80 free, need 48"))
        self.assertEqual(r["offering"], (True, "c7gn.2xlarge in zone-a"))

    def test_binance_needs_its_type_offered_and_four_vcpus(self):
        r = self.check(quota=5, zones=("zone-c", "zone-b"), itype="m7g.xlarge", vcpus=4)
        self.assertEqual(r["offering"], (True, "m7g.xlarge in zone-b"))
        self.assertEqual(r["quota"], (True, "L-1216C47A 5 vCPUs, 0 in use, 5 free, need 4"))
        self.assertFalse(self.check(quota=3, itype="m7g.xlarge", vcpus=4)["quota"][0])
        self.assertEqual(self.check(zones=(), itype="m7g.xlarge", vcpus=4)["offering"],
                         (False, "no zone in ap-northeast-1 offers m7g.xlarge"))

    def test_quota_minus_running_on_demand_vcpus(self):
        # 16 on-demand vCPUs in use (spot is another quota): 64 leaves exactly 48, 63 not
        instances = [running("c7gn.2xlarge", 8), running("t3.large", 2), running("r7g.xlarge", 4),
                     running("g5.large", 2), running("c7g.8xlarge", 32, spot=True)]
        self.assertEqual(self.check(quota=64, instances=instances)["quota"],
                         (True, "L-1216C47A 64 vCPUs, 16 in use, 48 free, need 48"))
        ok, detail = self.check(quota=63, instances=instances)["quota"]
        self.assertFalse(ok)
        self.assertTrue(detail.startswith("L-1216C47A 63 vCPUs, 16 in use, 47 free, need 48: request"), detail)

    def test_fresh_account_quota_fails_with_the_fix(self):
        ok, detail = self.check(quota=5)["quota"]
        self.assertFalse(ok)
        self.assertEqual(detail, "L-1216C47A 5 vCPUs, 0 in use, 5 free, need 48: request an increase in Service"
                                 " Quotas (ap-northeast-1, EC2, Running On-Demand Standard)")

    def test_no_credentials(self):
        r = self.check(sts_error=Exception("Unable to locate credentials"))
        self.assertEqual(r["credentials"], (False, "Unable to locate credentials"))
        self.assertEqual(r["quota"], (False, "not checked without credentials"))
        self.assertEqual(r["offering"], (False, "not checked without credentials"))

    def test_quota_api_denied(self):
        r = self.check(quota_error=ClientError("AccessDeniedException", "not authorized: servicequotas:GetServiceQuota"))
        self.assertFalse(r["quota"][0])
        self.assertIn("servicequotas:GetServiceQuota", r["quota"][1])

    def test_dirty_tree_and_missing_hft_host(self):
        def local(argv, **kw):
            if "status" in argv:
                return Proc(0, " M bench/src/main.rs\n")
            return Proc(128 if "cat-file" in argv else 0, "")

        ok, detail = self.check(local=local)["source"]
        self.assertFalse(ok)
        self.assertIn("commit these tracked changes first:\nM bench/src/main.rs", detail)
        self.assertIn("HEAD has no deploy/hft-host.sh", detail)

    def test_preflight_command_fails_clearly(self):
        aws = z.Aws("r", {"ec2": None, "ssm": None, "sts": FakeSts(Exception("expired token")), "service-quotas": None})
        env = z.Env(profile_region=lambda: "ap-southeast-2", aws=lambda region: aws, local=lambda argv, **kw: Proc(0, ""))
        out = io.StringIO()
        with contextlib.redirect_stdout(out), self.assertRaisesRegex(SystemExit, "preflight failed; nothing was created"):
            z.main(["preflight"], env)
        self.assertIn("preflight credentials: FAIL expired token", out.getvalue())

    def test_region_defaults_to_the_profiles_and_fails_clearly_without_one(self):
        """Project accounts are locked to one region: --region defaults to the AWS profile's."""
        regions = []

        def aws(region):
            regions.append(region)
            return z.Aws(region, {"ec2": None, "ssm": None, "sts": FakeSts(Exception("stop here")),
                                  "service-quotas": None})
        local = lambda argv, **kw: Proc(0, "")
        for argv, profile, want in ((["preflight"], "ap-southeast-2", "ap-southeast-2"),
                                    (["preflight", "--region", "us-east-1"], "ap-southeast-2", "us-east-1"),
                                    (["preflight", "--region", "us-east-1"], None, "us-east-1")):
            env = z.Env(profile_region=lambda: profile, aws=aws, local=local)
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
                z.main(argv, env)
            self.assertEqual(regions.pop(), want)
        env = z.Env(profile_region=lambda: None, aws=aws, local=local)
        for cmd in (["preflight"], ["run"], ["binance"], ["cleanup", "--run-id", "x"]):
            with self.assertRaisesRegex(SystemExit, "no region: pass --region, or set one in the AWS profile"):
                z.main(cmd, env)
        self.assertEqual(regions, [], "nothing touched AWS without a region")


class FakeAws:
    def __init__(self, world):
        self.w, self.ec2 = world, FakeQuotaEc2([])
        self.sts, self.quotas = FakeSts(), FakeQuotas(96)

    def create_network(self, run_id, expires, my_ip, record, key_path, instance_type, strategy):
        self.w.calls.append(("aws", f"create_network {my_ip} {instance_type} {strategy}"))
        record("vpc", "vpc-1")
        if self.w.network_fails:
            raise ClientError("RequestLimitExceeded", "slow down")
        record("subnet", "subnet-1")
        with open(key_path, "w") as f:
            f.write("PRIVATE\n")
        return dict(NET)

    def zone_offering(self, instance_type):
        if not self.w.offered:
            raise RuntimeError(f"no zone in ap-northeast-1 offers {instance_type}")
        return "zone-a"

    def launch_hosts(self, net, run_id, expires, record, instance_type, roles):
        self.w.calls.append(("aws", f"launch_hosts {instance_type} {' '.join(roles)}"))
        for k in range(len(roles)):
            record("instance", f"i-{k}")
        return [{"role": r, "id": f"i-{k}", "public_ip": f"pub{k}", "private_ip": f"10.80.1.{10 + k}",
                 "data_eni": {"id": f"eni-{k}", "mac": f"m{k}", "ip": RECEIVER_DATA_IP if k == 0 else f"10.80.1.{110 + k}"}}
                for k, r in enumerate(roles)]

    def attach_eip(self, eni, run_id, expires, record):
        self.w.calls.append(("aws", f"attach_eip {eni['id']} {eni['ip']}"))
        record("address", "eipalloc-1")


class RunTest(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.out = tempfile.mkdtemp()

    def local(self, argv, **kw):
        self.w.local_calls.append(argv)
        if argv[0] == "git":
            return Proc(0, b"TAR" if "archive" in argv else "")
        if argv[1].endswith("report.py"):
            return Proc(self.w.report_code)
        raise AssertionError(argv)

    def run_bench(self):
        w = self.w
        env = z.Env(profile_region=lambda: "ap-southeast-2", aws=lambda region: FakeAws(w), host=lambda info, key, rd: FakeHost(w, info, rd),
                    local=self.local, my_ip=lambda: "203.0.113.9", now=lambda: NOW, sleep=lambda s: None)
        args = argparse.Namespace(region="ap-northeast-1", ttl="2h", out=self.out)

        def clean_run(ec2, run_id, recorded, log=print, **kw):
            self.assertEqual(kw["journal_path"], os.path.join(self.out, run_id, "created.json"))
            w.clean_calls.append((run_id, {k: list(v) for k, v in recorded.items()}))
            if w.interrupt_teardown:
                os.kill(os.getpid(), signal.SIGINT)
                time.sleep(0.01)
            w.teardown_done = True
            return list(w.left_after_teardown)

        handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
        self.log = io.StringIO()
        with mock.patch.object(z.janitor, "clean_run", clean_run), contextlib.redirect_stdout(self.log):
            try:
                code = z.run(args, env)
            except BaseException as e:  # SystemExit from the Ctrl-C handler too
                code = e
            finally:
                for s, h in handlers.items():
                    signal.signal(s, h)
        self.run_dir = os.path.join(self.out, os.listdir(self.out)[0])
        return code

    def sweeps_run(self):
        return [c for r, c in self.w.calls if "zhida-bench sweep" in c]

    def test_six_hosts_end_to_end(self):
        self.assertEqual(self.run_bench(), 0)
        w = self.w
        self.assertEqual([c for r, c in w.calls if r == "aws"], [
            f"create_network 203.0.113.9 {z.HOST_TYPE} cluster", f"launch_hosts {z.HOST_TYPE} " + " ".join(z.ROLES)])
        self.assertEqual(sorted(r for r, c in w.calls if c == "setup b'TAR'"), sorted(z.ROLES))
        cfg = w.put[("sender0", f"{z.REMOTE}/ssh_config")]
        self.assertEqual([l for l in cfg.splitlines() if l.startswith("Host sender")],
                         [f"Host sender{i}" for i in range(z.SENDERS)])
        self.assertIn("HostName 10.80.1.11", cfg)
        self.assertEqual(w.put[("sender0", f"{z.REMOTE}/id_run")], "PRIVATE\n")
        sweeps = self.sweeps_run()
        self.assertEqual(len(sweeps), 3)
        self.assertEqual(w.sweep_timeouts, [z.SWEEP_WORST_S] * 3)
        self.assertTrue(all(r == "sender0" for r, c in w.calls if "zhida-bench sweep" in c))
        for cmd, t in zip(sweeps, z.sweep_tags()):
            inner = shlex.split(cmd)[2]
            argv = shlex.split(inner.split(" > ")[0])
            self.assertEqual(argv[:3], ["taskset", "-c", "0-6"], "coordinator on HOUSEKEEPING")
            self.assertEqual(argv[argv.index("--gens") + 1], str(z.SENDERS))
            tmpl = argv[argv.index("--gen-cmd") + 1]
            for i in range(z.SENDERS):
                self.assertTrue(tmpl.replace("{i}", str(i)).startswith(f"ssh -F {z.REMOTE}/ssh_config sender{i} "))
            self.assertIn(f"--target {RECEIVER_DATA_IP}:7000", tmpl)
            self.assertIn(f"> {z.REMOTE}/{t}.jsonl 2> >(tee {z.REMOTE}/{t}.coord.log >&2)", inner)
        serves = [c for r, c in w.calls if r == "receiver" and "zhida-bench serve" in c]
        self.assertEqual(len(serves), 3)
        for c, (proto, _) in zip(serves, z.SWEEPS):
            self.assertTrue(c.endswith(f"taskset -c 7 zhida-bench serve --config /etc/zhida/dut.json --proto {proto}"
                                       " --port 7000"), c)
        for t in z.sweep_tags():
            want = {("sender0", f"{z.REMOTE}/{t}.jsonl"), ("sender0", f"{z.REMOTE}/{t}.coord.log")}
            want |= {(f"sender{i}", f"{z.REMOTE}/gen-{t}.log") for i in range(z.SENDERS)}
            self.assertEqual(want, {f for f in w.fetched if t in f[1]})
            self.assertTrue(os.path.exists(os.path.join(self.run_dir, f"sender{z.SENDERS - 1}", f"gen-{t}.log")))
        with open(os.path.join(self.run_dir, "expected.json")) as f:
            self.assertEqual(json.load(f), {"sweeps": z.sweep_tags()})
        report = [a for a in w.local_calls if a[1].endswith("report.py")]
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0][2], self.run_dir)
        run_id = os.path.basename(self.run_dir)
        recorded = {"vpc": ["vpc-1"], "subnet": ["subnet-1"], "instance": [f"i-{k}" for k in range(len(z.ROLES))]}
        self.assertEqual(w.clean_calls, [(run_id, recorded)])
        self.assertEqual(janitor.read_created(os.path.join(self.run_dir, "created.json")), recorded)
        with open(os.path.join(self.run_dir, "host-setup.txt")) as f:
            setup = f.read()
        self.assertIn(f"[sender{z.SENDERS - 1} host.env]", setup)
        self.assertFalse(os.path.exists(os.path.join(self.run_dir, "fetch-errors.txt")))

    def test_failed_host_setup_never_claims_code_built_or_starts_a_sweep(self):
        self.w.verify_fail.add("sender1")
        self.assertIsInstance(self.run_bench(), RuntimeError)
        output = self.log.getvalue()
        self.assertIn("hosts up:", output)
        self.assertNotIn("code built:", output)
        self.assertEqual(self.sweeps_run(), [])
        self.assertIn("end: failed", output)

    def test_failing_sweep_does_not_stop_later_sweeps_and_is_fetched(self):
        self.w.sweep_exit[UDP] = 1
        self.w.report_code = 1  # report.py: INCOMPLETE
        self.assertEqual(self.run_bench(), 1)
        self.assertEqual(len(self.sweeps_run()), 3)
        self.assertIn(("sender0", f"{z.REMOTE}/{UDP}.jsonl"), self.w.fetched)
        self.assertIn(f"{UDP}: coordinator exit 1", self.log.getvalue())
        self.assertEqual(len(self.w.clean_calls), 1)

    def test_one_failed_sweep_fails_the_run_even_if_the_report_passes(self):
        """Regression: a generator-bound udp sweep (coordinator exit 1) next to good tcp/ws sweeps
        gave run exit 0."""
        self.w.sweep_exit[UDP] = 1
        self.assertEqual(self.run_bench(), 1)
        self.assertIn(f"generator-bound: {UDP}", self.log.getvalue())

    def test_scp_failure_is_recorded_and_fails_the_run(self):
        self.w.fetch_fail.add(("sender2", f"{z.REMOTE}/gen-{TCP}.log"))
        self.assertEqual(self.run_bench(), 1)
        with open(os.path.join(self.run_dir, "fetch-errors.txt")) as f:
            self.assertEqual(f.read(), f"sender2:{z.REMOTE}/gen-{TCP}.log: scp exited 1\n")
        self.assertEqual(len(self.sweeps_run()), 3)

    def test_failed_verify_aborts_with_teardown(self):
        self.w.verify_fail.add("sender2")
        code = self.run_bench()
        self.assertIsInstance(code, RuntimeError)
        self.assertIn("host setup failed on sender2", str(code))
        self.assertEqual(self.sweeps_run(), [])
        self.assertEqual(len(self.w.clean_calls), 1)
        self.assertEqual([a for a in self.w.local_calls if a[1].endswith("report.py")], [])

    def test_resources_left_after_teardown_fail_the_run(self):
        self.w.left_after_teardown = ["network-interface eni-9 (in-use, run r)"]
        self.assertEqual(self.run_bench(), 1)
        self.assertEqual(len(self.sweeps_run()), 3)

    def test_a_failed_create_tears_down_what_was_recorded(self):
        self.w.network_fails = True
        self.assertIsInstance(self.run_bench(), ClientError)
        self.assertEqual(self.w.clean_calls, [(os.path.basename(self.run_dir), {"vpc": ["vpc-1"]})])

    def test_a_second_ctrl_c_during_teardown_does_not_interrupt_it(self):
        self.w.interrupt_sweep = TCP
        self.w.interrupt_teardown = True
        code = self.run_bench()
        self.assertIsInstance(code, SystemExit)
        self.assertEqual(code.code, 128 + signal.SIGINT)
        self.assertTrue(self.w.teardown_done)
        self.assertEqual(len(self.sweeps_run()), 2)
        self.assertIn(("sender0", f"{z.REMOTE}/{TCP}.jsonl"), self.w.fetched, "the interrupted sweep")
        self.assertEqual(len([a for a in self.w.local_calls if a[1].endswith("report.py")]), 1)

    def test_timed_out_receiver_stop_and_fetches_while_unwinding_still_reach_teardown(self):
        self.w.interrupt_sweep = UDP
        self.w.hang = True
        self.assertIsInstance(self.run_bench(), subprocess.TimeoutExpired)
        self.assertEqual(len(self.w.fetched), 2 + z.SENDERS, "every file of the interrupted sweep tried")
        self.assertTrue(self.w.teardown_done)

    def test_dirty_tree_refused_before_anything_is_created(self):
        def local(argv, **kw):
            self.w.local_calls.append(argv)
            return Proc(0, " M bench/src/main.rs\n")

        env = z.Env(profile_region=lambda: "ap-southeast-2", aws=lambda r: FakeAws(self.w), local=local, now=lambda: NOW)
        out = io.StringIO()
        with contextlib.redirect_stdout(out), self.assertRaisesRegex(SystemExit, "preflight failed"):
            z.run(argparse.Namespace(region="r", ttl="2h", out=self.out), env)
        self.assertIn("preflight source: FAIL the hosts build `git archive HEAD`", out.getvalue())
        self.assertEqual(self.w.calls, [])
        self.assertEqual(os.listdir(self.out), [])


class BinanceTest(unittest.TestCase):
    """`binance` end to end: fake AWS and host, the real runner flow and Created file."""

    def setUp(self):
        self.w = World()
        self.out = tempfile.mkdtemp()

    def run_binance(self, seconds=30):
        w = self.w

        def local(argv, **kw):
            w.local_calls.append(argv)
            assert argv[0] == "git", argv
            return Proc(0, b"TAR" if "archive" in argv else "")

        def clean_run(ec2, run_id, recorded, log=print, **kw):
            self.assertEqual(kw["journal_path"], os.path.join(self.out, run_id, "created.json"))
            w.clean_calls.append((run_id, {k: list(v) for k, v in recorded.items()}))
            w.teardown_done = True
            return list(w.left_after_teardown)

        env = z.Env(profile_region=lambda: "ap-southeast-2", aws=lambda region: FakeAws(w), host=lambda info, key, rd: FakeHost(w, info, rd),
                    local=local, my_ip=lambda: "203.0.113.9", now=lambda: NOW, sleep=lambda s: None)
        handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
        self.log = io.StringIO()
        with mock.patch.object(z.janitor, "clean_run", clean_run), contextlib.redirect_stdout(self.log):
            try:
                code = z.main(["binance", "--seconds", str(seconds), "--out", self.out], env)
            except BaseException as e:
                code = e
            finally:
                for s, h in handlers.items():
                    signal.signal(s, h)
        runs = os.listdir(self.out)
        self.run_dir = os.path.join(self.out, runs[0]) if runs else None
        return code

    def feed_runs(self):
        return [c for r, c in self.w.calls if "run-binance.sh" in c]

    def test_one_host_end_to_end(self):
        self.assertEqual(self.run_binance(seconds=30), 0)
        w = self.w
        self.assertEqual(w.calls[:4], [
            ("aws", "create_network 203.0.113.9 m7g.xlarge precision-time"), ("aws", "launch_hosts m7g.xlarge binance"),
            ("aws", f"attach_eip eni-0 {RECEIVER_DATA_IP}"), ("binance", "install b'TAR'")])
        self.assertFalse(any("setup" in c or "bootstrap" in c for r, c in w.calls), "no zhida-bench build")
        self.assertEqual(self.feed_runs(), [f"root mkdir -p {z.REMOTE} && {z.SRC_DIR}/deploy/run-binance.sh"
                                            f" --seconds 30 > {z.REMOTE}/feed.out 2>&1"])
        self.assertEqual(w.feed_timeouts, [30 + z.FEED_MARGIN_S])
        self.assertEqual(w.fetched, [("binance", "/var/log/zhida/binance.log"), ("binance", z.FEED_OUT)])
        for name in ("binance.log", "feed.out", "verify.txt", "hosts.json"):
            self.assertTrue(os.path.exists(os.path.join(self.run_dir, name)), name)
        with open(os.path.join(self.run_dir, "verify.txt")) as f:
            self.assertEqual(f.read(), "[binance] verify: ok\n")
        recorded = {"vpc": ["vpc-1"], "subnet": ["subnet-1"], "instance": ["i-0"], "address": ["eipalloc-1"]}
        self.assertEqual(janitor.read_created(os.path.join(self.run_dir, "created.json")), recorded)
        self.assertEqual(w.clean_calls, [(os.path.basename(self.run_dir), recorded)])
        log = self.log.getvalue()
        self.assertIn("messages: 8", log)
        self.assertIn("p50 8192..16384 ns", log)
        self.assertLess(log.index("tearing down"), log.index("messages: 8"), "summary last")

    def test_a_failed_feed_fetches_reports_and_fails(self):
        self.w.feed_code, self.w.feed_text = 3, PORT_OPEN_FAILED
        self.assertEqual(self.run_binance(), 1)
        self.assertEqual(len(self.w.fetched), 2)
        log = self.log.getvalue()
        for line in ("run-binance.sh exit 3", "no feed_latency line", "port_open_error: PMD lacks"):
            self.assertIn(line, log)
        self.assertTrue(self.w.teardown_done)

    def test_a_failed_fetch_fails_the_run(self):
        self.w.fetch_fail.add(("binance", "/var/log/zhida/binance.log"))
        self.assertEqual(self.run_binance(), 1)
        self.assertIn("fetch failed: binance:/var/log/zhida/binance.log: scp exited 1", self.log.getvalue())
        self.assertIn("messages: 8", self.log.getvalue())

    def test_a_hung_feed_is_still_fetched_and_torn_down(self):
        self.w.feed_hang = True
        self.assertIsInstance(self.run_binance(), subprocess.TimeoutExpired)
        self.assertEqual(len(self.w.fetched), 2)
        self.assertIn("run-binance.sh did not finish", self.log.getvalue())
        self.assertTrue(self.w.teardown_done)

    def test_failed_verify_tears_down_without_running_the_feed(self):
        self.w.verify_fail.add("binance")
        code = self.run_binance()
        self.assertIsInstance(code, RuntimeError)
        self.assertIn("host setup failed", str(code))
        self.assertEqual(self.feed_runs(), [])
        self.assertEqual(len(self.w.clean_calls), 1)
        with open(os.path.join(self.run_dir, "verify.txt")) as f:
            self.assertIn("verify: FAIL", f.read())

    def test_resources_left_after_teardown_fail_the_run(self):
        self.w.left_after_teardown = ["address eipalloc-1"]
        self.assertEqual(self.run_binance(), 1)
        self.assertIn("NOT VERIFIED GONE", self.log.getvalue())

    def test_type_not_offered_fails_before_anything_is_created(self):
        self.w.offered = False
        code = self.run_binance()
        self.assertIsInstance(code, SystemExit)
        self.assertEqual(str(code), "preflight failed; nothing was created")
        self.assertIn("preflight offering: FAIL no zone in ap-northeast-1 offers m7g.xlarge", self.log.getvalue())
        self.assertEqual(self.w.calls, [])
        self.assertIsNone(self.run_dir)
        self.assertEqual(self.w.clean_calls, [])


class BootstrapTest(unittest.TestCase):
    def test_shell_syntax(self):
        subprocess.run(["sh", "-n", os.path.join(os.path.dirname(os.path.abspath(__file__)), "bootstrap.sh")],
                       check=True)


if __name__ == "__main__":
    unittest.main()
