#!/usr/bin/env python3
"""zhida-aws-bench: the latency/load bench on EC2 in one command, 1 receiver + SENDERS sender hosts.

  zhida_aws_bench.py plan [--shape arm|intel]              print hosts, sweeps and needs; no AWS calls
  zhida_aws_bench.py preflight [--shape arm|intel] [--region R]   check tree, credentials, offerings, quota
  zhida_aws_bench.py run [--shape arm|intel] [--region R] [--ttl 2h] [--out DIR]
  zhida_aws_bench.py kernel [--shape arm|intel] [--region R] [--ttl 2h] [--out DIR]   Linux receiver
  zhida_aws_bench.py cleanup --run-id ID [--region R] [--out DIR]   delete a leftover run's resources
  zhida_aws_bench.py binance [--region R] [--seconds 600] [--out DIR] [--ttl 2h]   the Binance listener

Needs python3 with boto3, ssh, scp, git, a clean committed tree (the hosts build `git archive HEAD`)
and AWS credentials; --region defaults to the AWS profile's region. Design: plans/bench/design.md.
`preflight` (also the first step of `run`) checks the tree, credentials, a common zone offering
every instance type and on-demand Standard vCPU quota (L-1216C47A) before anything is created.

Permissions the caller needs: ec2 (VPC, subnet, internet gateway, route table, security group,
placement group, key pair, run/terminate/describe instances, create/attach/delete/modify network
interfaces, delete volumes, create tags, describe instance type offerings; for `binance` also
allocate/associate/disassociate/release/describe addresses), ssm:GetParameter (the AL2023 AMI),
sts:GetCallerIdentity, servicequotas:GetServiceQuota.

`run` creates a VPC, subnet, internet gateway, route table, security group (SSH from the caller's
address), cluster placement group and key pair for the run; provisions on-demand instances in
the placement group (one request for ARM, separate receiver and sender requests for Intel), one data ENI each; applies
`deploy/hft-host.sh` (builds DPDK and the toolchain, reboots, verify), using Linux ENA on the
receiver for `kernel` and DPDK on all other hosts, then builds zhida-bench
natively (bootstrap.sh); runs the fixed sweep matrix with the coordinator on sender0 driving one
worker per sender host over SSH; fetches every result; tears down; runs bench/report.py. Every
resource carries the run id and expiry tags and is recorded in RUN_DIR/created.json as it is
created; teardown deletes all of them and exits non-zero if a final describe still finds any. Each
instance powers itself off (terminate) at the TTL. Teardown runs on exit, errors, Ctrl-C and SIGTERM.

`binance` does the same with one m7g.xlarge (BINANCE_TYPE) in a precision-time placement group (ENA
hardware RX timestamps and the PHC), its data ENI with a run Elastic IP (the feed's route out): source,
hft-host.sh apply, verify, then `deploy/run-binance.sh --seconds N`; fetches /var/log/zhida/binance.log
and the feed's output into RUN_DIR (with verify.txt), prints the message count and latency, tears down.
Exit 0 only when the feed exited 0, both files were fetched and teardown verified nothing is left.
"""
import argparse
import datetime
import hashlib
import ipaddress
import json
import math
import os
import random
import shlex
import signal
import string
import subprocess
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import janitor  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
AMI_PARAM = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
INTEL_AMI_PARAM = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
VPC_CIDR, SUBNET_CIDR = "10.80.0.0/16", "10.80.1.0/24"
NAME = janitor.NAME
TTL = "2h"

# The fixed shape: 1 receiver + SENDERS sender hosts, all HOST_TYPE. 1 + 3 x c7gn.xlarge = 16 vCPUs
# (the account's quota); 1 + 5 x c7gn.2xlarge (48 vCPUs) is SENDERS = 5, HOST_TYPE = "c7gn.2xlarge".
# Each host: hft-host.sh isolates its highest CPU for the app, the rest (at least 3) housekeep.
SENDERS, HOST_TYPE = 3, "c7gn.xlarge"
HOST_VCPUS = {"c7gn.xlarge": 4, "c7gn.2xlarge": 8}[HOST_TYPE]
SHAPES = {
    "arm": (HOST_TYPE, HOST_TYPE, AMI_PARAM, (1 + SENDERS) * HOST_VCPUS),
    "intel": ("c6in.8xlarge", "c6in.2xlarge", INTEL_AMI_PARAM, 32 + SENDERS * 8),
}
ROOT_GB = 40
ROLES = ["receiver"] + [f"sender{i}" for i in range(SENDERS)]
CONNECTIONS = 4 * SENDERS
SIZE = 64
RATES = (10_000, 100_000, 400_000, 800_000, 900_000, 1_000_000,
         1_100_000, 1_200_000, 1_400_000, 1_800_000, 2_500_000)
STEP_MS, WARMUP_MS, DRAIN_MS = 10_000, 250, 200
SWEEPS = [("udp", None), ("tcp", 50), ("tcp", None), ("ws", 50), ("ws", None)]
PORT = 7000
MIMALLOC_ENV = "MIMALLOC_ALLOW_LARGE_OS_PAGES=0"
XSTATS_ENV = "ZHIDA_BENCH_XSTATS=1"
SRC_DIR = "/opt/zhida-src"
HFT = f"{SRC_DIR}/deploy/hft-host.sh"
REMOTE = "/var/tmp/zhida"
RECEIVER_UNIT = "zhida-bench-receiver"
# The 11 scheduled steps take 115.5 s; this 15-minute backstop also covers normal startup,
# clock probes and shutdown. It does not cover every sequential per-worker timeout.
SWEEP_WORST_S = 15 * 60
BUILD_S = 3600
SETUP_REBOOT_S = 900
# The bound of every ssh/scp without its own: the receiver stop (systemd SIGKILLs after 90 s), the
# fetches while unwinding to teardown.
SSH_S = 120
QUOTA_CODE = "L-1216C47A"
# The Binance listener host: the smallest 4-vCPU arm64 type of a family that gives ENA hardware RX
# timestamps and the PHC in a precision-time placement group (deploy/README.md; not c7gn).
BINANCE_TYPE, BINANCE_VCPUS = "m7g.xlarge", 4
BINANCE_LOG = "/var/log/zhida/binance.log"
FEED_OUT = f"{REMOTE}/feed.out"
# run-binance.sh beyond the feed's --seconds: verify, DNS, port open, the connect (20 s limit), the close.
FEED_MARGIN_S = 300


def log(msg):
    print(f"zhida-aws-bench: {msg}", flush=True)


# ---------------------------------------------------------------- pure parts


def parse_duration(text):
    """'90m', '2h', '45s' or plain seconds -> seconds."""
    units = {"s": 1, "m": 60, "h": 3600}
    if text and text[-1] in units:
        return int(float(text[:-1]) * units[text[-1]])
    return int(text)


def new_run_id(now):
    return now.strftime("%Y%m%d-%H%M%S-") + "".join(random.choices(string.ascii_lowercase + string.digits, k=4))


def run_tags(run_id, expires):
    return [{"Key": janitor.RUN_TAG, "Value": run_id}, {"Key": janitor.EXPIRES_TAG, "Value": expires.isoformat()},
            {"Key": "Name", "Value": f"{NAME}-{run_id}"}]


def tag(proto, budget):
    return f"{proto}-n{CONNECTIONS}-s{SIZE}-{'uncapped' if budget is None else f'b{budget}'}"


def sweep_tags():
    return [tag(p, b) for p, b in SWEEPS]


def gen_template(proto, receiver_ip, t):
    """--gen-cmd: the coordinator runs it with `sh -c` on sender0 for {i} = 0..4; it starts worker
    {i} on sender{i} through the run's ssh_config, pinned to that host's APP_CPU."""
    inner = (f". /etc/zhida/host.env; exec env {MIMALLOC_ENV} {XSTATS_ENV} taskset -c $APP_CPU zhida-bench gen"
             f" --config /etc/zhida/dut.json --proto {proto} --target {receiver_ip}:{PORT} --senders {{senders}}"
             f" --size {SIZE} --local-port 40000 2>>{REMOTE}/gen-{t}.log")
    return f"ssh -F {REMOTE}/ssh_config sender{{i}} {shlex.quote('sudo sh -c ' + shlex.quote(inner))}"


def sweep_command(proto, budget, receiver_ip, housekeeping):
    """The coordinator, run as ec2-user on sender0's housekeeping CPUs."""
    t = tag(proto, budget)
    budget_arg = "" if budget is None else f" --budget-us {budget}"
    command = (f"taskset -c {shlex.quote(housekeeping)} zhida-bench sweep --proto {proto} --gens {SENDERS}"
               f" --senders {CONNECTIONS} --rates {','.join(map(str, RATES))}{budget_arg}"
               f" --step-ms {STEP_MS} --warmup-ms {WARMUP_MS} --drain-ms {DRAIN_MS}"
               f" --gen-cmd {shlex.quote(gen_template(proto, receiver_ip, t))}"
               f" > {REMOTE}/{t}.jsonl 2> >(tee {REMOTE}/{t}.coord.log >&2)")
    return f"bash -c {shlex.quote(command)}"


def serve_command(proto, app_cpu, kernel_ip=None):
    """The receiver as a transient systemd unit; generators never change."""
    command = (f"serve --config /etc/zhida/dut.json" if kernel_ip is None
               else f"kernel-serve --bind {shlex.quote(kernel_ip)}")
    return (f"systemd-run --unit {RECEIVER_UNIT} --setenv={MIMALLOC_ENV} --setenv={XSTATS_ENV} taskset -c {shlex.quote(app_cpu)}"
            f" zhida-bench {command} --proto {proto} --port {PORT}")


def ssh_config(sender_ips):
    """sender0's ssh_config: the senders by their primary private IPs, the run's key."""
    lines = []
    for i, ip in enumerate(sender_ips):
        lines += [f"Host sender{i}", f"    HostName {ip}"]
    lines += ["Host *", "    User ec2-user", f"    IdentityFile {REMOTE}/id_run", "    IdentitiesOnly yes",
              "    BatchMode yes",
              f"    UserKnownHostsFile {REMOTE}/known_hosts", "    StrictHostKeyChecking accept-new",
              "    ConnectTimeout 10", "    ServerAliveInterval 15"]
    return "\n".join(lines) + "\n"


HOST_ENV_KEYS = ("MGMT_IF", "DATA_IF", "DATA_PCI", "DATA_MAC", "NUMA_NODE", "APP_CPU", "ISOLATED", "HOUSEKEEPING")


def parse_host_env(text):
    """/etc/zhida/host.env (shell assignments) -> dict; every key hft-host.sh writes must be there."""
    out = {}
    for line in text.splitlines():
        words = shlex.split(line, comments=True)
        for w in words:
            if "=" in w:
                k, v = w.split("=", 1)
                out[k] = v
    missing = [k for k in HOST_ENV_KEYS if not out.get(k)]
    if missing:
        raise ValueError(f"host.env lacks {', '.join(missing)}")
    return out


def data_ip(dut_json):
    """The data ENI's address from /etc/zhida/dut.json ("ipv4": "10.80.1.23/24")."""
    return json.loads(dut_json)["ipv4"].split("/")[0]


def user_data(deadline):
    """Instance user data: a persistent systemd timer powers the instance off at `deadline` (which
    terminates it: InstanceInitiatedShutdownBehavior=terminate); it survives the setup reboot."""
    stamp = deadline.strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"""#!/bin/sh
cat >/etc/systemd/system/zhida-deadman.timer <<EOF
[Unit]
Description=zhida-bench dead-man: power off (terminate) at the run's TTL
[Timer]
OnCalendar={stamp}
Persistent=true
[Install]
WantedBy=timers.target
EOF
cat >/etc/systemd/system/zhida-deadman.service <<EOF
[Service]
Type=oneshot
ExecStart=/usr/bin/systemctl poweroff
EOF
systemctl daemon-reload
systemctl enable --now zhida-deadman.timer
"""


def latency_text(value):
    """zhida-nic's feed_latency value (conduit/crates/binance-feed/src/lib.rs, Display for Latencies:
    `no_stamp=N not_ready=N impossible=N max_mapping_bound_ns=N` then `<=0:N`, `lo..hi:N`, `1073741824..:N`
    per nonempty log2 ns bucket, ascending) -> those counts and the buckets holding p50/p99/p99.9/max
    of the sampled messages. Anything else (`latency invalid: NIC clock jumped`) is the reason as is."""
    words = value.split()
    if not words or not words[0].startswith("no_stamp="):
        return value
    counts = [w for w in words if ":" not in w]
    buckets = [(label, int(n)) for label, n in (w.rsplit(":", 1) for w in words if ":" in w)]
    samples = sum(n for _, n in buckets)
    head = f"{samples} samples, {' '.join(counts)}"
    if not samples:
        return head

    def bucket(permille):  # nearest rank, in integers: no float rounding past a bucket edge
        rank, seen = max(1, -(-permille * samples // 1000)), 0
        for label, n in buckets:
            seen += n
            if seen >= rank:
                return label

    return head + "".join(f", p{pm / 10:g} {bucket(pm)} ns" for pm in (500, 990, 999)) + f", max {buckets[-1][0]} ns"


def feed_summary(text, code):
    """What the Binance run did, from run-binance.sh's exit code (None: it did not finish) and output
    (None: not fetched): zhida-nic's `feed_count` and `feed_latency` lines, or why there are none."""
    out = ["run-binance.sh " + ("did not finish" if code is None else f"exit {code}")]
    if text is None:
        return out + ["no feed output fetched"]
    fields = dict(line[len("zhida-nic: "):].split(": ", 1) for line in text.splitlines()
                  if line.startswith("zhida-nic: ") and ": " in line[len("zhida-nic: "):])
    if "feed_count" in fields:
        out.append(f"messages: {fields['feed_count']}")
    out.append(f"latency: {latency_text(fields['feed_latency'])}" if "feed_latency" in fields
               else "no feed_latency line")
    if code != 0:
        out += ["feed output ends:"] + text.splitlines()[-5:]
    return out


def plan_text(region, ttl, run_id, expires, kernel=False, shape="arm"):
    receiver_type, sender_type, _, vcpus = SHAPES[shape]
    hosts = (f"{len(ROLES)} x {receiver_type}" if receiver_type == sender_type
             else f"1 x {receiver_type} receiver, {SENDERS} x {sender_type} senders")
    lines = [
        f"run {run_id}, region {region}, TTL {ttl} (expires {expires.isoformat()})",
        f"hosts: {hosts} on-demand, in a cluster placement group, one subnet/AZ;"
        f" each: primary ENI (SSH, public IPv4) + 1 data ENI"
        f" ({'receiver on Linux ena, senders on vfio-pci' if kernel else 'vfio-pci'}, DeleteOnTermination);"
        " CPUs from hft-host.sh: one isolated APP_CPU, the rest HOUSEKEEPING",
        f"  {'receiver':9} zhida-bench {'kernel-serve' if kernel else 'serve'} on APP_CPU"
        f" (systemd unit {RECEIVER_UNIT})",
        f"  {'sender0':9} zhida-bench sweep (coordinator) on HOUSEKEEPING; worker 0 (zhida-bench gen) on APP_CPU",
    ]
    lines += [f"  {f'sender{i}':9} worker {i} (zhida-bench gen, over SSH from sender0) on APP_CPU"
              for i in range(1, SENDERS)]
    lines += [
        f"sweeps: {CONNECTIONS} senders ({CONNECTIONS // SENDERS} per sender host), {SIZE} B messages,"
        f" rates {','.join(map(str, RATES))} msg/s; coordinator killed after {SWEEP_WORST_S} s",
        f"timing per rate: {STEP_MS} ms measurement + {WARMUP_MS} ms warmup + {DRAIN_MS} ms drain"
        " + 50 ms lead; host setup/builds, worker startup and clock probes are additional",
    ]
    lines += [f"  {t}" + ("" if b is None else f"  (--budget-us {b})") for t, (p, b) in zip(sweep_tags(), SWEEPS)]
    lines += [
        f"needs: on-demand Standard vCPU quota L-1216C47A with {vcpus} vCPUs free in {region};"
        " permissions: see `zhida_aws_bench.py -h`",
        "created for the run and deleted at teardown, tagged with the run id: VPC, subnet, internet gateway,"
        " route table, security group, placement group, key pair, the instances and data ENIs",
        f"billable while the run lives: {len(ROLES)} instances, {len(ROLES)} x {ROOT_GB} GB gp3 root volumes,"
        f" {len(ROLES)} public IPv4 addresses",
        "expiry: each instance powers itself off (terminates) at the TTL; the runner tears down on exit, errors,"
        " Ctrl-C and SIGTERM",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------- AWS


class Aws:
    def __init__(self, region, clients=None):
        if clients is None:
            try:
                import boto3
            except ImportError:
                raise SystemExit("boto3 is not importable: pip3 install boto3") from None

            session = boto3.session.Session(region_name=region)
            clients = {n: session.client(n) for n in ("ec2", "ssm", "sts", "service-quotas")}
        self.region = region
        self.ec2, self.ssm = clients["ec2"], clients["ssm"]
        self.sts, self.quotas = clients["sts"], clients["service-quotas"]

    def create_network(self, run_id, expires, my_ip, record, key_path, zone, strategy):
        """The run's VPC, subnet in `zone`, internet gateway and route,
        security group, placement group (`strategy`) and key pair (private key to `key_path`), each
        passed to `record` as its create succeeds."""
        ec2 = self.ec2
        name = f"{NAME}-{run_id}"

        def spec(kind):
            return [{"ResourceType": kind, "Tags": run_tags(run_id, expires)}]

        vpc = ec2.create_vpc(CidrBlock=VPC_CIDR, TagSpecifications=spec("vpc"))["Vpc"]["VpcId"]
        record("vpc", vpc)
        ec2.get_waiter("vpc_available").wait(VpcIds=[vpc])
        ec2.modify_vpc_attribute(VpcId=vpc, EnableDnsHostnames={"Value": True})
        subnet = ec2.create_subnet(VpcId=vpc, CidrBlock=SUBNET_CIDR, AvailabilityZone=zone,
                                   TagSpecifications=spec("subnet"))["Subnet"]["SubnetId"]
        record("subnet", subnet)
        igw = ec2.create_internet_gateway(TagSpecifications=spec("internet-gateway"))["InternetGateway"]
        record("internet-gateway", igw["InternetGatewayId"])
        ec2.attach_internet_gateway(InternetGatewayId=igw["InternetGatewayId"], VpcId=vpc)
        rt = ec2.create_route_table(VpcId=vpc, TagSpecifications=spec("route-table"))["RouteTable"]["RouteTableId"]
        record("route-table", rt)
        ec2.create_route(RouteTableId=rt, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw["InternetGatewayId"])
        ec2.associate_route_table(RouteTableId=rt, SubnetId=subnet)
        sg = ec2.create_security_group(GroupName=name, Description="zhida-bench hosts", VpcId=vpc,
                                       TagSpecifications=spec("security-group"))["GroupId"]
        record("security-group", sg)
        ec2.authorize_security_group_ingress(GroupId=sg, IpPermissions=[
            {"IpProtocol": "-1", "UserIdGroupPairs": [{"GroupId": sg}]},
            {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": f"{my_ip}/32"}]}])
        ec2.create_placement_group(GroupName=name, Strategy=strategy, TagSpecifications=spec("placement-group"))
        record("placement-group", name)
        material = ec2.create_key_pair(KeyName=name, KeyType="ed25519",
                                       TagSpecifications=spec("key-pair"))["KeyMaterial"]
        record("key-pair", name)
        with open(os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f:
            f.write(material)
        return {"name": name, "subnet": subnet, "zone": zone, "sg": sg}

    def zone_offering(self, instance_types):
        common = None
        for instance_type in dict.fromkeys(instance_types):
            zones = set()
            token = None
            while True:
                kwargs = {"LocationType": "availability-zone",
                          "Filters": [{"Name": "instance-type", "Values": [instance_type]}]}
                if token:
                    kwargs["NextToken"] = token
                response = self.ec2.describe_instance_type_offerings(**kwargs)
                zones.update(o["Location"] for o in response["InstanceTypeOfferings"])
                token = response.get("NextToken")
                if not token:
                    break
            common = zones if common is None else common & zones
        if not common:
            raise RuntimeError(f"no zone in {self.region} offers {', '.join(dict.fromkeys(instance_types))}")
        return sorted(common)[0]

    def launch_hosts(self, net, run_id, expires, record, instance_type, roles, ami_param):
        """One instance per role in one run_instances into the placement group; then one data ENI
        each (create, attach, DeleteOnTermination); every ID passed to `record` as its create
        succeeds. Returns host records in `roles` order."""
        ec2 = self.ec2
        ami = self.ssm.get_parameter(Name=ami_param)["Parameter"]["Value"]
        tags = run_tags(run_id, expires)
        r = ec2.run_instances(
            ImageId=ami, InstanceType=instance_type, MinCount=len(roles), MaxCount=len(roles), KeyName=net["name"],
            InstanceInitiatedShutdownBehavior="terminate",
            Placement={"GroupName": net["name"], "AvailabilityZone": net["zone"]},
            NetworkInterfaces=[{"DeviceIndex": 0, "SubnetId": net["subnet"], "Groups": [net["sg"]],
                                "AssociatePublicIpAddress": True, "DeleteOnTermination": True}],
            BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
                "VolumeSize": ROOT_GB, "VolumeType": "gp3", "DeleteOnTermination": True}}],
            UserData=user_data(expires),
            TagSpecifications=[{"ResourceType": k, "Tags": tags} for k in ("instance", "volume", "network-interface")])
        ids = [i["InstanceId"] for i in sorted(r["Instances"], key=lambda i: i["AmiLaunchIndex"])]
        for iid in ids:
            record("instance", iid)
        ec2.get_waiter("instance_running").wait(InstanceIds=ids)
        enis = {}
        for iid in ids:
            eni = ec2.create_network_interface(
                SubnetId=net["subnet"], Groups=[net["sg"]],
                TagSpecifications=[{"ResourceType": "network-interface", "Tags": tags}])["NetworkInterface"]
            record("network-interface", eni["NetworkInterfaceId"])
            att = ec2.attach_network_interface(NetworkInterfaceId=eni["NetworkInterfaceId"], InstanceId=iid,
                                               DeviceIndex=1)["AttachmentId"]
            ec2.modify_network_interface_attribute(NetworkInterfaceId=eni["NetworkInterfaceId"],
                                                   Attachment={"AttachmentId": att, "DeleteOnTermination": True})
            enis[iid] = {"id": eni["NetworkInterfaceId"], "mac": eni["MacAddress"], "ip": eni["PrivateIpAddress"]}
        described = {i["InstanceId"]: i
                     for page in ec2.get_paginator("describe_instances").paginate(InstanceIds=ids)
                     for res in page["Reservations"] for i in res["Instances"]}
        return [{"role": role, "id": iid, "public_ip": described[iid]["PublicIpAddress"],
                 "private_ip": described[iid]["PrivateIpAddress"], "data_eni": enis[iid]}
                for iid, role in zip(ids, roles)]

    def attach_eip(self, eni, run_id, expires, record):
        """A run's Elastic IP on the data ENI's private address (`eni` as launch_hosts gives it): an
        instance with two ENIs gets no public IPv4, and the feed on the data ENI needs one."""
        alloc = self.ec2.allocate_address(Domain="vpc", TagSpecifications=[
            {"ResourceType": "elastic-ip", "Tags": run_tags(run_id, expires)}])["AllocationId"]
        record("address", alloc)
        self.ec2.associate_address(AllocationId=alloc, NetworkInterfaceId=eni["id"], PrivateIpAddress=eni["ip"])


# ---------------------------------------------------------------- hosts


class Host:
    def __init__(self, info, key_path, run_dir, run=subprocess.run, sleep=time.sleep, clock=time.monotonic):
        self.info, self.key, self.run_dir = info, key_path, run_dir
        self.role = info["role"]
        self._run, self._sleep, self._clock = run, sleep, clock

    def _opts(self):
        return ["-i", self.key, "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes",
                "-o", f"UserKnownHostsFile={self.run_dir}/known_hosts", "-o", "ConnectTimeout=10",
                "-o", "ServerAliveInterval=15"]

    def ssh(self, command, check=True, timeout=None, input=None, stream=False):
        argv = ["ssh", *self._opts(), f"ec2-user@{self.info['public_ip']}", command]
        r = self._run(argv, capture_output=not stream, input=input, text=not isinstance(input, bytes),
                      timeout=timeout or SSH_S)
        if check and r.returncode != 0:
            err = r.stderr.decode(errors="replace") if isinstance(r.stderr, bytes) else (r.stderr or "")
            raise RuntimeError(f"{self.role}: `{command[:120]}` exited {r.returncode}: {err[-2000:]}")
        return r

    def root(self, command, **kw):
        return self.ssh(f"sudo sh -c {shlex.quote(command)}", **kw)

    def wait_ssh(self, limit=600):
        deadline = self._clock() + limit
        last = "no SSH attempt completed"
        while self._clock() < deadline:
            r = self.ssh("true", check=False)
            if r.returncode == 0:
                return
            last = f"SSH exited {r.returncode}: {(r.stderr or '').strip()[-2000:]}"
            self._sleep(5)
        raise RuntimeError(f"{self.role}: no SSH within {limit} s; last attempt: {last}")

    def put_text(self, remote, text, mode):
        self.ssh(f"umask 077 && cat > {remote} && chmod {mode} {remote}", input=text)

    def fetch(self, remote, local):
        """scp remote -> local; None on success, else what failed."""
        argv = ["scp", "-q", *self._opts(), f"ec2-user@{self.info['public_ip']}:{remote}", local]
        try:
            r = self._run(argv, capture_output=True, text=True, timeout=SSH_S)
        except subprocess.TimeoutExpired:
            return f"{self.role}:{remote}: scp timed out after {SSH_S} s"
        if r.returncode == 0:
            return None
        return f"{self.role}:{remote}: scp exited {r.returncode}: {(r.stderr or '').strip()[-500:]}"

    def setup_diagnostics(self):
        command = ("echo 'systemctl status zhida-host.service:'; "
                   "systemctl status zhida-host.service --no-pager; "
                   "echo 'journalctl -u zhida-host.service -b --no-pager:'; "
                   "journalctl -u zhida-host.service -b --no-pager; "
                   "echo 'loaded vfio_pci_core srcversion:'; "
                   "cat /sys/module/vfio_pci_core/srcversion; "
                   "echo 'on-disk vfio_pci_core srcversion:'; modinfo -F srcversion vfio_pci_core")
        try:
            r = self.root(command, check=False)
        except subprocess.TimeoutExpired:
            return f"setup diagnostics: SSH timed out after {SSH_S} s"
        return f"setup diagnostics (exit {r.returncode}):\n{r.stdout.rstrip()}\n{r.stderr.rstrip()}"

    def install(self, archive, kernel_receiver=False):
        """Source, hft-host.sh apply (builds, reboots), verify. Returns (ok, report text)."""
        self.ssh(f"sudo rm -rf {SRC_DIR} && sudo mkdir -p {SRC_DIR} && sudo tar -x -C {SRC_DIR}", input=archive,
                 timeout=BUILD_S)
        apply = self.root(f"sh {HFT} apply{' --kernel-receiver' if kernel_receiver else ''}", check=False, timeout=BUILD_S)
        text = [f"[{self.role} {self.info['id']}]", "apply:", apply.stdout.rstrip()]
        if apply.returncode not in (0, 100, 255):  # 100: rebooting; 255: SSH dropped by the reboot
            return False, "\n".join(text + [f"apply exited {apply.returncode}: {apply.stderr.strip()[-2000:]}",
                                            self.setup_diagnostics()])
        # Before reboot verify and SSH can fail normally; the new boot unit failing is terminal.
        deadline = self._clock() + SETUP_REBOOT_S
        while True:
            try:
                v = self.root(f"sh {HFT} verify", check=False)
            except subprocess.TimeoutExpired:
                v = subprocess.CompletedProcess([], 255, "", f"verify SSH timed out after {SSH_S} s")
            if v.returncode == 0:
                break
            if v.returncode != 255:
                try:
                    state = self.root("systemctl is-failed zhida-host.service", check=False)
                except subprocess.TimeoutExpired:
                    state = None
                if state is not None and state.returncode == 0 and state.stdout.strip() == "failed":
                    text += ["zhida-host.service failed"]
                    break
            if self._clock() >= deadline:
                text += [f"verify did not pass within {SETUP_REBOOT_S} s"]
                break
            self._sleep(10)
        text += ["verify:", v.stdout.rstrip(), v.stderr.rstrip()]
        if v.returncode != 0:
            text += [self.setup_diagnostics()]
        return v.returncode == 0, "\n".join(text)

    def setup(self, archive, kernel_receiver=False):
        """install, then zhida-bench (bootstrap.sh)."""
        ok, text = self.install(archive, kernel_receiver)
        if ok:
            self.root(f"SRC={SRC_DIR} sh {SRC_DIR}/bench/aws/bootstrap.sh", timeout=BUILD_S)
        return ok, text


# ---------------------------------------------------------------- run


def default_region():
    """The AWS profile's region (AWS_REGION, AWS_DEFAULT_REGION or the profile's config), as boto3
    resolves it; None if none is set."""
    try:
        import boto3
    except ImportError:
        raise SystemExit("boto3 is not importable: pip3 install boto3") from None
    return boto3.session.Session().region_name


class Env:
    """What run() touches outside this process; tests substitute fakes."""

    def __init__(self, aws=Aws, host=Host, local=subprocess.run, my_ip=None, now=None, sleep=time.sleep,
                 profile_region=None):
        self.aws, self.host, self.local, self.sleep = aws, host, local, sleep
        self.profile_region = profile_region or default_region
        self.my_ip = my_ip or (lambda: urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10)
                               .read().decode().strip())
        self.now = now or (lambda: datetime.datetime.now(datetime.timezone.utc))


def tree_problems(local):
    """The pushed source is `git archive HEAD`: no uncommitted tracked changes, hft-host.sh in it."""
    out = []
    dirty = local(["git", "-C", REPO, "status", "--porcelain", "--untracked-files=no"],
                  capture_output=True, text=True, check=True).stdout.strip()
    if dirty:
        out.append(f"the hosts build `git archive HEAD`; commit these tracked changes first:\n{dirty}")
    if local(["git", "-C", REPO, "cat-file", "-e", "HEAD:deploy/hft-host.sh"], capture_output=True).returncode:
        out.append("HEAD has no deploy/hft-host.sh: the hosts need it")
    return out


def quota_check(aws, need):
    """(ok, detail): L-1216C47A minus the running on-demand vCPUs leaves `need`. Counts every family
    (the quota covers only Standard ones), so it can only err towards refusing."""
    quota = int(aws.quotas.get_service_quota(ServiceCode="ec2", QuotaCode=QUOTA_CODE)["Quota"]["Value"])
    used = 0
    for page in aws.ec2.get_paginator("describe_instances").paginate(
            Filters=[{"Name": "instance-state-name", "Values": ["pending", "running"]}]):
        for res in page["Reservations"]:
            for i in res["Instances"]:
                if i.get("InstanceLifecycle") != "spot":
                    used += i["CpuOptions"]["CoreCount"] * i["CpuOptions"]["ThreadsPerCore"]
    free = quota - used
    detail = f"{QUOTA_CODE} {quota} vCPUs, {used} in use, {free} free, need {need}"
    if free < need:
        detail += f": request an increase in Service Quotas ({aws.region}, EC2, Running On-Demand Standard)"
    return free >= need, detail


def preflight(env, region, instance_types, vcpus):
    """Every check before creation: ([(name, ok, detail)], Aws, common zone)."""
    problems = tree_problems(env.local)
    out = [("source", not problems, "; ".join(problems) or "clean, HEAD has deploy/hft-host.sh")]
    aws = env.aws(region)
    zone = None
    try:
        who = aws.sts.get_caller_identity()
        out.append(("credentials", True, f"account {who['Account']} as {who['Arn']}"))
    except Exception as e:
        return out + [("credentials", False, str(e))] + [
            (name, False, "not checked without credentials") for name in ("offering", "quota")], aws, zone
    try:
        zone = aws.zone_offering(instance_types)
        out.append(("offering", True, f"{', '.join(dict.fromkeys(instance_types))} in {zone}"))
    except Exception as e:
        out.append(("offering", False, str(e)))
    try:
        out.append(("quota", *quota_check(aws, vcpus)))
    except Exception as e:
        out.append(("quota", False, str(e)))
    return out, aws, zone


def checked_preflight(env, region, instance_types, vcpus):
    results, aws, zone = preflight(env, region, instance_types, vcpus)
    for name, ok, detail in results:
        log(f"preflight {name}: {'ok' if ok else 'FAIL'} {detail}")
    if not all(ok for _, ok, _ in results):
        raise SystemExit("preflight failed; nothing was created")
    return aws, zone


def source_archive(local, revision="HEAD"):
    return local(["git", "-C", REPO, "archive", "--format=tar", revision], capture_output=True, check=True).stdout


def setup_hosts(hosts, archive, kernel=False):
    """All hosts in parallel; raises listing every host that failed."""
    results, threads = {}, []

    def one(h):
        try:
            results[h.role] = h.setup(archive, kernel_receiver=kernel and h.role == "receiver")
        except Exception as e:  # reported below with the others
            results[h.role] = (False, f"[{h.role}] setup error: {e}")

    for h in hosts:
        t = threading.Thread(target=one, args=(h,), daemon=True)  # Ctrl-C tears down without waiting
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    failed = [r for r in ROLES if not results[r][0]]
    text = [results[r][1] for r in ROLES]
    if failed:
        raise RuntimeError(f"host setup failed on {', '.join(failed)}:\n" + "\n".join(text))
    return text


def read_facts(hosts, receiver_eni_ip):
    """host.env of every host; the receiver's data IP from its dut.json, which must be its data ENI's."""
    facts = {h.role: parse_host_env(h.root("cat /etc/zhida/host.env").stdout) for h in hosts}
    ip = data_ip(hosts[0].root("cat /etc/zhida/dut.json").stdout)
    if ip != receiver_eni_ip:
        raise RuntimeError(f"receiver dut.json ipv4 {ip} is not its data ENI's address {receiver_eni_ip}")
    return facts, ip


KERNEL_TABLE, KERNEL_RULE = 7000, 10000
KERNEL_ROUTE_SETTLE_INTERVAL = 0.5
KERNEL_ROUTE_SETTLE_STABLE_SAMPLES = 6
KERNEL_ROUTE_SETTLE_MAX_SAMPLES = 20


def check_kernel_route(text, destination, source, interface):
    routes = json.loads(text)
    if (len(routes) != 1 or routes[0].get("dst") != destination
            or routes[0].get("from") != source or routes[0].get("dev") != interface
            or str(routes[0].get("table")) != str(KERNEL_TABLE)):
        raise RuntimeError(f"kernel receiver route mismatch for {destination} from {source}: {text}")


def configure_kernel_receiver(receiver, facts, receiver_ip, sender_ips):
    """Configure the already-ENA data device without changing management routes."""
    config = json.loads(receiver.root("cat /etc/zhida/dut.json").stdout)
    data = ipaddress.IPv4Interface(config["ipv4"])
    mtu = config["mtu"]
    if str(data.ip) != receiver_ip or str(data.network) != SUBNET_CIDR:
        raise RuntimeError(f"receiver data address/subnet mismatch: {data}, expected {receiver_ip} in {SUBNET_CIDR}")
    if facts["DATA_IF"] == facts["MGMT_IF"]:
        raise RuntimeError("receiver DATA_IF is the management interface")
    interface = shlex.quote(facts["DATA_IF"])
    pci = shlex.quote(facts["DATA_PCI"])
    mac = shlex.quote(facts["DATA_MAC"])
    # Refuse occupied policy slots rather than deleting somebody else's policy.
    rules = json.loads(receiver.root("ip -j -4 rule show").stdout)
    if any(r.get("priority") == KERNEL_RULE or str(r.get("table")) == str(KERNEL_TABLE) for r in rules):
        raise RuntimeError("kernel receiver policy rule/table already in use")
    routes = json.loads(receiver.root("ip -j -4 route show table all").stdout)
    if any(str(r.get("table")) == str(KERNEL_TABLE) for r in routes):
        raise RuntimeError("kernel receiver policy table already has routes")
    receiver.root(
        f"set -eu; test \"$(basename \"$(readlink /sys/bus/pci/devices/{pci}/driver)\")\" = ena; "
        f"test \"$(basename \"$(readlink /sys/class/net/{interface}/device)\")\" = {pci}; "
        f"test \"$(cat /sys/class/net/{interface}/address)\" = {mac}; "
        f"ip link set dev {interface} mtu {shlex.quote(str(mtu))} up; "
        f"ip -4 addr replace {data} dev {interface} noprefixroute")
    actual_mtu = int(receiver.root(f"cat /sys/class/net/{interface}/mtu").stdout.strip())
    if actual_mtu != mtu:
        raise RuntimeError(f"kernel receiver MTU mismatch on {facts['DATA_IF']}: "
                           f"configured {mtu}, actual {actual_mtu}")
    before = receiver.root("ip -j -4 route show table main").stdout
    stable = 0
    for _ in range(KERNEL_ROUTE_SETTLE_MAX_SAMPLES):
        receiver._sleep(KERNEL_ROUTE_SETTLE_INTERVAL)
        current = receiver.root("ip -j -4 route show table main").stdout
        stable = stable + 1 if json.loads(current) == json.loads(before) else 0
        before = current
        if stable == KERNEL_ROUTE_SETTLE_STABLE_SAMPLES:
            break
    else:
        raise RuntimeError("kernel receiver main routes did not settle after "
                           f"{KERNEL_ROUTE_SETTLE_MAX_SAMPLES} checks "
                           f"({KERNEL_ROUTE_SETTLE_INTERVAL:g} second delay between checks)")
    receiver.root(
        f"set -eu; sysctl -w net.ipv4.conf.{interface}.rp_filter=2; "
        f"ip -4 route add table {KERNEL_TABLE} {data.network} dev {interface} src {data.ip}; "
        f"ip -4 route add table {KERNEL_TABLE} default via {data.network.network_address + 1} dev {interface}; "
        f"ip -4 rule add priority {KERNEL_RULE} from {data.ip}/32 table {KERNEL_TABLE}")
    after = receiver.root("ip -j -4 route show table main").stdout
    if json.loads(before) != json.loads(after):
        raise RuntimeError(f"kernel receiver changed the main routing table; refusing to measure\n"
                           f"before: {before}\nafter: {after}")
    checked_routes = []
    for sender_ip in sender_ips:
        text = receiver.root(f"ip -j -4 route get {shlex.quote(sender_ip)} from {data.ip}").stdout
        check_kernel_route(text, sender_ip, receiver_ip, facts["DATA_IF"])
        checked_routes += json.loads(text)
    return {"address": str(data), "interface": facts["DATA_IF"], "routes": checked_routes,
            "main_routes_before": json.loads(before), "main_routes_after": json.loads(after),
            "placement": kernel_snapshot(receiver, facts, configure=True)}


def kernel_snapshot(receiver, facts, configure=False):
    """Data IRQ placement plus counters; NET_RX is host-wide, including management SSH."""
    script = r'''
import json, pathlib, sys, time
pci, interface, cpu, configure = sys.argv[1:]
configure = configure == "True"
irqs = sorted(int(p.name) for p in pathlib.Path("/sys/bus/pci/devices", pci, "msi_irqs").iterdir())
if not irqs:
    raise RuntimeError("data ENI has no MSI IRQs")
lines = pathlib.Path("/proc/interrupts").read_text().splitlines()
cpus = [int(c.removeprefix("CPU")) for c in lines[0].split()]
names = {}
for line in lines[1:]:
    name, _, values = line.partition(":")
    if name.strip().isdigit() and int(name) in irqs:
        names[name.strip()] = " ".join(values.split()[len(cpus):])
affinity, details = {}, {}
for irq in irqs:
    directory = pathlib.Path("/proc/irq", str(irq))
    requested = directory / "smp_affinity_list"
    effective = directory / "effective_affinity_list"
    before = {"requested": requested.read_text().strip(), "effective": effective.read_text().strip()}
    error = None
    if configure:
        try:
            requested.write_text(cpu)
        except OSError as exc:
            error = str(exc)
    key = str(irq)
    affinity[key] = effective.read_text().strip()
    details[key] = {"name": names.get(key), "before": before,
                    "requested": requested.read_text().strip(), "effective": affinity[key],
                    "write_error": error}
if configure and not any(info["write_error"] for info in details.values()):
    deadline = time.monotonic() + 5
    while any(value != cpu for value in affinity.values()) and time.monotonic() < deadline:
        time.sleep(0.05)
        for irq in irqs:
            key = str(irq)
            affinity[key] = pathlib.Path("/proc/irq", key, "effective_affinity_list").read_text().strip()
            details[key]["effective"] = affinity[key]
if any(info["write_error"] or info["effective"] != cpu for info in details.values()):
    for key, info in details.items():
        debug = pathlib.Path("/sys/kernel/debug/irq/irqs", key)
        if debug.exists():
            try:
                info["kernel_irq_state"] = debug.read_text()
            except OSError as exc:
                info["kernel_irq_state_error"] = str(exc)
    print(json.dumps({"affinity_error": {"target_cpu": cpu, "irqs": details}}))
    sys.exit(0)
queues = list(pathlib.Path("/sys/class/net", interface, "queues").glob("rx-*/rps_cpus"))
if not queues:
    raise RuntimeError("data ENI has no RX queues")
rps = {}
for path in queues:
    if configure:
        path.write_text("0")
    rps[str(path)] = path.read_text().strip()
    if int(rps[str(path)].replace(",", ""), 16) != 0:
        raise RuntimeError(f"RPS enabled: {path}")
lines = pathlib.Path("/proc/interrupts").read_text().splitlines()
counts = {}
for line in lines[1:]:
    name, _, values = line.partition(":")
    if name.strip().isdigit() and int(name) in irqs:
        counts[name.strip()] = list(map(int, values.split()[:len(cpus)]))
if len(counts) != len(irqs):
    raise RuntimeError("missing data IRQ counters")
soft = pathlib.Path("/proc/softirqs").read_text().splitlines()
soft_cpus = [int(c.removeprefix("CPU")) for c in soft[0].split()]
net_rx = next(list(map(int, line.split(":")[1].split())) for line in soft[1:] if line.strip().startswith("NET_RX:"))
print(json.dumps({"cpus": cpus, "irqs": counts, "softirq_cpus": soft_cpus, "net_rx": net_rx,
                  "effective_affinity": affinity, "rps_cpus": rps}))
'''
    command = ["python3", "-c", script, facts["DATA_PCI"], facts["DATA_IF"], facts["APP_CPU"], str(configure)]
    snapshot = json.loads(receiver.root(shlex.join(command)).stdout)
    if "affinity_error" in snapshot:
        path = os.path.join(receiver.run_dir, "kernel-irq-affinity-error.json")
        with open(path, "w") as f:
            json.dump(snapshot["affinity_error"], f, indent=2)
        raise RuntimeError(f"receiver data IRQ affinity differs from CPU {facts['APP_CPU']}; details: {path}")
    return snapshot


def kernel_counter_delta(before, after, app_cpu):
    if before["cpus"] != after["cpus"] or before["irqs"].keys() != after["irqs"].keys():
        raise RuntimeError("receiver data IRQs/CPUs changed during sweep")
    irqs = {irq: [b - a for a, b in zip(values, after["irqs"][irq])]
            for irq, values in before["irqs"].items()}
    if any(value < 0 or (cpu != int(app_cpu) and value != 0)
           for values in irqs.values() for cpu, value in zip(before["cpus"], values)):
        raise RuntimeError("receiver data IRQ counters reset or ran outside APP_CPU")
    if before["softirq_cpus"] != after["softirq_cpus"]:
        raise RuntimeError("receiver softirq CPUs changed during sweep")
    return {"cpus": before["cpus"], "data_irqs": irqs, "softirq_cpus": before["softirq_cpus"],
            "net_rx_host_wide": [b - a for a, b in zip(before["net_rx"], after["net_rx"])]}

ENA_COUNTERS = ("pps_allowance_exceeded", "bw_in_allowance_exceeded", "bw_out_allowance_exceeded")


def parse_ena_stats(text):
    counters = {}
    for line in text.splitlines():
        name, sep, value = line.strip().partition(":")
        if sep and name in ENA_COUNTERS:
            if name in counters or not value.strip().isdigit():
                raise ValueError(f"invalid ethtool counter {name}: {value.strip()}")
            counters[name] = int(value.strip())
    return {"counters": counters, "missing": [name for name in ENA_COUNTERS if name not in counters]}


def kernel_ena_snapshot(receiver, interface):
    unix_ns = time.time_ns()
    try:
        result = receiver.root(f"ethtool -S {shlex.quote(interface)}", check=False)
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        return {"status": "error", "error": str(error), "unix_ns": unix_ns}
    if result.returncode:
        return {"status": "error", "error": f"ethtool exited {result.returncode}: {result.stderr or ''}", "unix_ns": unix_ns}
    try:
        parsed = parse_ena_stats(result.stdout)
        return {"status": "unsupported" if parsed["missing"] else "ok", "unix_ns": unix_ns, **parsed}
    except ValueError as error:
        return {"status": "error", "error": str(error), "unix_ns": unix_ns}


def kernel_ena_result(before, after):
    invalid = []
    if before["status"] != "ok" or after["status"] != "ok":
        invalid.append("snapshot unavailable or counters missing")
    if before["status"] != "error" and after["status"] != "error":
        for name, value in after["counters"].items():
            if name in before["counters"] and value < before["counters"][name]:
                invalid.append(f"counter decreased: {name}")
    return {"before": before, "after": after, "invalid": invalid,
            "reset_detection": "counter decreases only; a reset followed by counter growth may not be detectable"}


def save_receiver_journal(receiver, invocation, path):
    if not invocation or len(invocation) != 32 or any(c not in string.hexdigits for c in invocation):
        text = f"receiver invocation ID unavailable or invalid: {invocation!r}\n"
    else:
        try:
            result = receiver.root(f"journalctl --no-pager -o cat _SYSTEMD_INVOCATION_ID={invocation}", check=False)
            text = result.stdout if result.returncode == 0 else f"journalctl exited {result.returncode}: {result.stderr or ''}\n"
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            text = f"journalctl failed: {error}\n"
    with open(path, "w") as f:
        f.write(text)


def connect_senders(hosts, key_path):
    """sender0 reaches sender0..4 as ec2-user through REMOTE/ssh_config, with the run's key pair."""
    senders = hosts[1:]
    with open(key_path) as f:
        senders[0].put_text(f"{REMOTE}/id_run", f.read(), "600")
    senders[0].put_text(f"{REMOTE}/ssh_config", ssh_config([h.info["private_ip"] for h in senders]), "644")


def fetch_sweep(t, hosts, run_dir):
    """Every file of sweep `t` into run_dir; returns the failures."""
    senders = hosts[1:]
    wanted = [(senders[0], f"{REMOTE}/{t}.jsonl", run_dir), (senders[0], f"{REMOTE}/{t}.coord.log", run_dir)]
    for h in senders:
        d = os.path.join(run_dir, h.role)
        os.makedirs(d, exist_ok=True)
        wanted.append((h, f"{REMOTE}/gen-{t}.log", d))
    return [e for e in (h.fetch(remote, local) for h, remote, local in wanted) if e]


def run_sweep(proto, budget, hosts, facts, receiver_ip, env, kernel=False, run_dir=None):
    """One sweep: receiver unit up, coordinator on sender0, receiver stopped. Returns the coordinator's
    exit code, or None when it did not run or timed out."""
    receiver, sender0 = hosts[0], hosts[1]
    t = tag(proto, budget)
    before = kernel_snapshot(receiver, facts["receiver"]) if kernel else None
    ena_before = kernel_ena_snapshot(receiver, facts["receiver"]["DATA_IF"]) if kernel else None
    invocation = None
    try:
        launched = receiver.root(
            f"systemctl stop {RECEIVER_UNIT} 2>/dev/null; systemctl reset-failed {RECEIVER_UNIT} 2>/dev/null;"
            f" {serve_command(proto, facts['receiver']['APP_CPU'], receiver_ip if kernel else None)}",
            check=False)
        invocation_result = receiver.root(f"systemctl show {RECEIVER_UNIT} --property=InvocationID --value", check=False)
        if invocation_result.returncode == 0:
            invocation = invocation_result.stdout.strip()
        if launched.returncode:
            log(f"{t}: receiver launch failed: {launched.stderr or ''}")
            return None
        env.sleep(3)
        if receiver.root(f"systemctl is-active {RECEIVER_UNIT}", check=False).stdout.strip() != "active":
            log(f"{t}: receiver did not start")
            return None
        try:
            return sender0.ssh(sweep_command(proto, budget, receiver_ip, facts["sender0"]["HOUSEKEEPING"]),
                               check=False, timeout=SWEEP_WORST_S, stream=True).returncode
        except subprocess.TimeoutExpired:
            log(f"{t}: coordinator still running after {SWEEP_WORST_S} s; killed")
            sender0.ssh("pkill -f 'zhida-bench sweep'", check=False, timeout=60)
            return None
    finally:
        try:
            receiver.root(f"systemctl stop {RECEIVER_UNIT}", check=False)
        finally:
            save_receiver_journal(receiver, invocation, os.path.join(run_dir, f"{t}.receiver.log"))
            if kernel:
                ena_after = kernel_ena_snapshot(receiver, facts["receiver"]["DATA_IF"])
                with open(os.path.join(run_dir, f"{t}.kernel-ena.json"), "w") as f:
                    json.dump(kernel_ena_result(ena_before, ena_after), f, indent=2)
        if kernel:
            artifact = {"before": before,
                        "net_rx_note": "Host-wide NET_RX includes management; not data-ENI attribution."}
            try:
                after = kernel_snapshot(receiver, facts["receiver"])
                artifact["after"] = after
                artifact["delta"] = kernel_counter_delta(before, after, facts["receiver"]["APP_CPU"])
            except (RuntimeError, subprocess.CalledProcessError) as error:
                artifact["error"] = str(error)
                raise
            finally:
                with open(os.path.join(run_dir, f"{t}.kernel-cpu.json"), "w") as f:
                    json.dump(artifact, f, indent=2)


def measure(hosts, facts, receiver_ip, run_dir, env, kernel=False):
    """The fixed matrix. A failed sweep is logged and the next one runs; every sweep started is
    fetched. Returns (fetch failures, sweeps whose coordinator did not exit 0)."""
    with open(os.path.join(run_dir, "expected.json"), "w") as f:
        json.dump({"sweeps": sweep_tags()}, f)
    fetch_errors, failed = [], []
    for proto, budget in SWEEPS:
        t = tag(proto, budget)
        try:
            code = run_sweep(proto, budget, hosts, facts, receiver_ip, env, kernel, run_dir)
            log(f"{t}: coordinator exit {code}")
            if code != 0:
                failed.append(t)
        finally:
            fetch_errors += fetch_sweep(t, hosts, run_dir)
    return fetch_errors, failed


def exit_on_signal(signum, _frame):
    raise SystemExit(128 + signum)


def on_signals(handler):
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def run(args, env=None, kernel=False):
    env = env or Env()
    now = env.now()
    expires = now + datetime.timedelta(seconds=parse_duration(args.ttl))
    run_id = new_run_id(now)
    receiver_type, sender_type, ami_param, vcpus = SHAPES[args.shape]
    print(plan_text(args.region, args.ttl, run_id, expires, kernel, args.shape))
    if kernel:
        print("receiver: Linux kernel sockets on the same data ENI; generators: unchanged Zhida/DPDK")
    aws, zone = checked_preflight(env, args.region, (receiver_type, sender_type), vcpus)
    revision = env.local(["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    archive = source_archive(env.local, revision)
    run_dir = os.path.join(args.out, run_id)
    os.makedirs(run_dir, exist_ok=True)
    with open(os.path.join(run_dir, "source.json"), "w") as f:
        json.dump({"commit": revision, "archive_sha256": hashlib.sha256(archive).hexdigest()}, f, indent=2)
    created = janitor.Created(os.path.join(run_dir, "created.json"))
    key_path = os.path.join(run_dir, "key.pem")
    my_ip = env.my_ip()

    on_signals(exit_on_signal)
    fetch_errors, failed = [], ["(not measured)"]
    # From here on every exit path deletes the run's resources.
    try:
        log(f"run {run_id} start: provisioning hosts")
        net = aws.create_network(run_id, expires, my_ip, created.add, key_path, zone, "cluster")
        if receiver_type == sender_type:
            infos = aws.launch_hosts(net, run_id, expires, created.add, receiver_type, ROLES, ami_param)
        else:
            infos = aws.launch_hosts(net, run_id, expires, created.add, receiver_type, ROLES[:1], ami_param)
            infos += aws.launch_hosts(net, run_id, expires, created.add, sender_type, ROLES[1:], ami_param)
        with open(os.path.join(run_dir, "hosts.json"), "w") as f:
            json.dump(infos, f, indent=1)
        hosts = [env.host(info, key_path, run_dir) for info in infos]
        for h in hosts:
            h.wait_ssh()
        log(f"hosts up: all {len(hosts)} hosts reachable over SSH; setting up and building")
        setup = setup_hosts(hosts, archive, kernel)
        log(f"code built: all {len(hosts)} hosts passed setup verification and built zhida-bench")
        facts, receiver_ip = read_facts(hosts, infos[0]["data_eni"]["ip"])
        if kernel:
            evidence = configure_kernel_receiver(hosts[0], facts["receiver"], receiver_ip,
                                               [info["data_eni"]["ip"] for info in infos[1:]])
            with open(os.path.join(run_dir, "kernel-receiver.json"), "w") as f:
                json.dump(evidence, f, indent=2)
        for h in hosts:
            h.root(f"install -d -m 1777 {REMOTE}")
        setup += [f"[{r} host.env] " + " ".join(f"{k}={v}" for k, v in sorted(facts[r].items())) for r in ROLES]
        with open(os.path.join(run_dir, "host-setup.txt"), "w") as f:
            f.write("\n".join(setup) + "\n")
        connect_senders(hosts, key_path)
        fetch_errors, failed = measure(hosts, facts, receiver_ip, run_dir, env, kernel)
    finally:
        # Nothing interrupts teardown; it goes first because the report is local.
        on_signals(signal.SIG_IGN)
        log(f"tearing down run {run_id}")
        torn = teardown(aws.ec2, run_id, created.ids, created.path)
        report_code = None
        if os.path.exists(os.path.join(run_dir, "expected.json")):
            if fetch_errors:
                with open(os.path.join(run_dir, "fetch-errors.txt"), "w") as f:
                    f.write("\n".join(fetch_errors) + "\n")
                log(f"{len(fetch_errors)} fetch failures: {os.path.join(run_dir, 'fetch-errors.txt')}")
            report_code = env.local([sys.executable, os.path.join(REPO, "bench", "report.py"), run_dir, "--title",
                                     f"zhida-bench {run_id} ({'Linux kernel' if kernel else 'Zhida'} receiver,"
                                     f" {SENDERS} Zhida senders, {receiver_type} receiver, {sender_type} senders)"]
                                    ).returncode
            log(f"report: {os.path.join(run_dir, 'report.md')} (report.py exit {report_code})")
        if failed:
            log(f"sweeps that failed or were generator-bound: {', '.join(failed)}")
        log(f"run {run_id} end: {'success' if report_code == 0 and not fetch_errors and not failed and torn else 'failed'}")
    return 0 if report_code == 0 and not fetch_errors and not failed and torn else 1


def teardown(ec2, run_id, recorded, journal_path):
    """Deletes the run's resources; True only when nothing was found left and nothing errored."""
    problems = janitor.clean_run(ec2, run_id, recorded, log=log, journal_path=journal_path)
    if problems:
        log(f"run {run_id}: NOT VERIFIED GONE; rerun `cleanup --run-id {run_id}`: {'; '.join(problems)}")
        return False
    log(f"run {run_id}: nothing left")
    return True


def binance(args, env=None):
    """The Binance listener on one BINANCE_TYPE host for --seconds; fetches its output, tears down."""
    env = env or Env()
    now = env.now()
    expires = now + datetime.timedelta(seconds=parse_duration(args.ttl))
    run_id = new_run_id(now)
    log(f"binance run {run_id}: 1 x {BINANCE_TYPE} in a precision-time placement group, {args.region},"
        f" feed {args.seconds} s, everything expires {expires.isoformat()}")
    aws, zone = checked_preflight(env, args.region, (BINANCE_TYPE,), BINANCE_VCPUS)
    archive = source_archive(env.local)
    run_dir = os.path.join(args.out, run_id)
    os.makedirs(run_dir, exist_ok=True)
    created = janitor.Created(os.path.join(run_dir, "created.json"))
    key_path = os.path.join(run_dir, "key.pem")
    my_ip = env.my_ip()
    on_signals(exit_on_signal)
    code, fetch_errors, summary = None, ["(not run)"], None
    # From here on every exit path deletes the run's resources.
    try:
        net = aws.create_network(run_id, expires, my_ip, created.add, key_path, zone, "precision-time")
        [info] = aws.launch_hosts(net, run_id, expires, created.add, BINANCE_TYPE, ["binance"], AMI_PARAM)
        aws.attach_eip(info["data_eni"], run_id, expires, created.add)
        with open(os.path.join(run_dir, "hosts.json"), "w") as f:
            json.dump([info], f, indent=1)
        host = env.host(info, key_path, run_dir)
        host.wait_ssh()
        log("Binance host ready (SSH available); setup started")
        ok, text = host.install(archive)
        with open(os.path.join(run_dir, "verify.txt"), "w") as f:
            f.write(text + "\n")
        if not ok:
            raise RuntimeError(f"host setup failed:\n{text}")
        log("Binance host setup finished (verify passed)")
        try:
            log("Binance feed started (requesting remote run)")
            code = host.root(f"mkdir -p {REMOTE} && {SRC_DIR}/deploy/run-binance.sh --seconds {args.seconds}"
                             f" > {FEED_OUT} 2>&1", check=False, timeout=args.seconds + FEED_MARGIN_S).returncode
            log(f"Binance feed finished (remote exit {code})")
        finally:
            fetch_errors = [e for e in (host.fetch(BINANCE_LOG, run_dir), host.fetch(FEED_OUT, run_dir)) if e]
            for e in fetch_errors:
                log(f"fetch failed: {e}")
            feed_path = os.path.join(run_dir, os.path.basename(FEED_OUT))
            if os.path.exists(feed_path):
                with open(feed_path, errors="replace") as f:
                    summary = feed_summary(f.read(), code)
            else:
                summary = feed_summary(None, code)
    finally:
        on_signals(signal.SIG_IGN)
        log(f"tearing down run {run_id}")
        torn = teardown(aws.ec2, run_id, created.ids, created.path)
        if summary:
            log(f"results in {run_dir}:\n  " + "\n  ".join(summary))
    return 0 if code == 0 and not fetch_errors and torn else 1


def cleanup(args, env=None):
    """A leftover run: the IDs its run_dir/created.json recorded, plus a tag lookup."""
    aws = (env or Env()).aws(args.region)
    path = os.path.join(args.out, args.run_id, "created.json")
    recorded = janitor.read_created(path) if os.path.exists(path) else {}
    log(f"run {args.run_id}: {sum(map(len, recorded.values()))} recorded IDs from {path}" if recorded
        else f"run {args.run_id}: no {path}; tag lookup only")
    on_signals(signal.SIG_IGN)
    return 0 if teardown(aws.ec2, args.run_id, recorded, path) else 1


def parser():
    p = argparse.ArgumentParser(prog="zhida-aws-bench", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "kernel", "plan", "preflight", "cleanup"):
        r = sub.add_parser(name)
        r.add_argument("--region", help="default: the AWS profile's region")
        r.add_argument("--out", default=os.path.join(REPO, "bench", "out", "aws"))
        if name == "cleanup":
            r.add_argument("--run-id", required=True)
        else:
            r.add_argument("--ttl", default=TTL, help="hard expiry of everything billable (e.g. 90m, 2h)")
            r.add_argument("--shape", choices=SHAPES, default="arm", help="benchmark host shape (default: arm)")
    b = sub.add_parser("binance", help="run the Binance listener (deploy/run-binance.sh) on one EC2 host")
    b.add_argument("--region", help="default: the AWS profile's region")
    b.add_argument("--seconds", type=int, default=600, help="the feed's --seconds (from the WebSocket open)")
    b.add_argument("--out", default=os.path.join(REPO, "bench", "out", "aws"))
    b.add_argument("--ttl", default=TTL, help="hard expiry of everything billable (e.g. 90m, 2h)")
    return p


def main(argv, env=None):
    args = parser().parse_args(argv)
    if args.cmd != "plan" and not args.region:
        args.region = (env or Env()).profile_region()
        if not args.region:
            raise SystemExit("no region: pass --region, or set one in the AWS profile (aws configure) or AWS_REGION")
    if args.cmd == "preflight":
        receiver_type, sender_type, _, vcpus = SHAPES[args.shape]
        checked_preflight(env or Env(), args.region, (receiver_type, sender_type), vcpus)
        return 0
    if args.cmd == "plan":
        now = datetime.datetime.now(datetime.timezone.utc)
        print(plan_text(args.region or "(the AWS profile's)", args.ttl, new_run_id(now),
                        now + datetime.timedelta(seconds=parse_duration(args.ttl)), shape=args.shape))
        return 0
    if args.cmd in ("run", "kernel"):
        return run(args, env, kernel=args.cmd == "kernel")
    if args.cmd == "binance":
        return binance(args, env)
    return cleanup(args, env)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
