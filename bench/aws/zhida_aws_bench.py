#!/usr/bin/env python3
"""zhida-aws-bench: the latency/load bench on EC2 in one command, 1 receiver + 5 sender hosts.

  zhida_aws_bench.py plan                                 print hosts, sweeps and needs; no AWS calls
  zhida_aws_bench.py preflight [--region R]               check tree, credentials, vCPU quota
  zhida_aws_bench.py run [--region R] [--ttl 2h] [--out DIR]
  zhida_aws_bench.py cleanup --run-id ID | --shared       delete a run's resources | the shared ones

Needs python3 with boto3, ssh, scp, ssh-keygen, git, a clean committed tree (the hosts build
`git archive HEAD`) and AWS credentials for the region. Design: plans/bench/design.md.
`preflight` (also the first step of `run`) checks the tree, the credentials and the on-demand
Standard vCPU quota (L-1216C47A: 48 free) before anything is created.

Permissions the caller needs: ec2 (VPC, subnet, internet gateway, route table, security group,
placement group, key pair, run/terminate/describe instances, create/attach/delete/modify network
interfaces, delete volumes, create tags, describe instance type offerings), ssm:GetParameter (the
AL2023 AMI), sts:GetCallerIdentity, servicequotas:GetServiceQuota.

`run` provisions six c7gn.2xlarge on-demand instances (one run_instances) in one cluster placement
group, one data ENI each; applies `deploy/hft-host.sh` (builds DPDK and the toolchain, reboots,
verify), then builds zhida-bench natively (bootstrap.sh); runs the fixed sweep matrix with the
coordinator on sender0 driving one worker per sender host over SSH; fetches every result; runs
bench/report.py; tears down. Billable per-run resources carry the run id and expiry tags; each
instance powers itself off (terminate) at the TTL; the runner tears down on exit, errors, Ctrl-C and
SIGTERM, and exits non-zero if a final describe still finds any of the run's resources.
"""
import argparse
import datetime
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
SHARED = {"Key": janitor.SHARED_TAG, "Value": "shared"}
AMI_PARAM = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
VPC_CIDR, SUBNET_CIDR = "10.80.0.0/16", "10.80.1.0/24"
NAME = janitor.NAME
KEY_DIR = os.path.expanduser("~/.zhida-bench")
REGION = "ap-northeast-1"
TTL = "2h"

INSTANCE_TYPE, HOST_VCPUS, ROOT_GB = "c7gn.2xlarge", 8, 40
ROLES = ["receiver"] + [f"sender{i}" for i in range(5)]
SENDER_HOSTS = len(ROLES) - 1
SENDERS = 4 * SENDER_HOSTS
SIZE = 64
RATE_MIN, RATE_MAX = 10_000, 5_000_000
BUDGET_US = 50
SWEEPS = [("udp", None), ("tcp", BUDGET_US), ("ws", BUDGET_US)]
PORT = 7000
MIMALLOC_ENV = "MIMALLOC_ALLOW_LARGE_OS_PAGES=1"
SRC_DIR = "/opt/zhida-src"
HFT = f"{SRC_DIR}/deploy/hft-host.sh"
REMOTE = "/var/tmp/zhida"
RECEIVER_UNIT = "zhida-bench-receiver"
# The coordinator's worst case and timeout: one step per rate RATE_MIN x sqrt(2)^k <= RATE_MAX, each
# <= 50 ms lead + 0.5 s warmup + 2 s window + 0.2 s drain + 10 s reply slack, plus 60 s start-up.
SWEEP_STEPS = sum(1 for k in range(64) if RATE_MIN * 2 ** (k / 2) <= RATE_MAX)
SWEEP_WORST_S = math.ceil(SWEEP_STEPS * (0.05 + 0.5 + 2 + 0.2 + 10) + 60)
BUILD_S = 3600
SETUP_REBOOT_S = 900
QUOTA_CODE = "L-1216C47A"


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


def shared_key_path(region):
    return os.path.join(KEY_DIR, f"{NAME}-{region}.pem")


def run_tags(run_id, expires):
    return [{"Key": janitor.RUN_TAG, "Value": run_id}, {"Key": janitor.EXPIRES_TAG, "Value": expires.isoformat()},
            {"Key": "Name", "Value": f"{NAME}-{run_id}"}]


def tag(proto, budget):
    return f"{proto}-n{SENDERS}-s{SIZE}-{'uncapped' if budget is None else f'b{budget}'}"


def sweep_tags():
    return [tag(p, b) for p, b in SWEEPS]


def gen_template(proto, receiver_ip, t):
    """--gen-cmd: the coordinator runs it with `sh -c` on sender0 for {i} = 0..4; it starts worker
    {i} on sender{i} through the run's ssh_config, pinned to that host's APP_CPU."""
    inner = (f". /etc/zhida/host.env; exec env {MIMALLOC_ENV} taskset -c $APP_CPU zhida-bench gen"
             f" --config /etc/zhida/dut.json --proto {proto} --target {receiver_ip}:{PORT} --senders {{senders}}"
             f" --size {SIZE} --local-port 40000 2>>{REMOTE}/gen-{t}.log")
    return f"ssh -F {REMOTE}/ssh_config sender{{i}} {shlex.quote('sudo sh -c ' + shlex.quote(inner))}"


def sweep_command(proto, budget, receiver_ip, housekeeping):
    """The coordinator, run as ec2-user on sender0's housekeeping CPUs."""
    t = tag(proto, budget)
    budget_arg = "" if budget is None else f" --budget-us {budget}"
    return (f"taskset -c {shlex.quote(housekeeping)} zhida-bench sweep --proto {proto} --gens {SENDER_HOSTS}"
            f" --senders {SENDERS} --size {SIZE} --rate-min {RATE_MIN} --rate-max {RATE_MAX}{budget_arg}"
            f" --gen-cmd {shlex.quote(gen_template(proto, receiver_ip, t))}"
            f" > {REMOTE}/{t}.jsonl 2> {REMOTE}/{t}.coord.log")


def serve_command(proto, app_cpu):
    """The receiver as a transient systemd unit."""
    return (f"systemd-run --unit {RECEIVER_UNIT} --setenv={MIMALLOC_ENV} taskset -c {shlex.quote(app_cpu)}"
            f" zhida-bench serve --config /etc/zhida/dut.json --proto {proto} --port {PORT}")


def ssh_config(sender_ips):
    """sender0's ssh_config: the five senders by their primary private IPs, the run's key."""
    lines = []
    for i, ip in enumerate(sender_ips):
        lines += [f"Host sender{i}", f"    HostName {ip}"]
    lines += ["Host *", "    User ec2-user", f"    IdentityFile {REMOTE}/id_run", "    BatchMode yes",
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


def plan_text(region, ttl, run_id, expires):
    lines = [
        f"run {run_id}, region {region}, TTL {ttl} (expires {expires.isoformat()})",
        f"hosts: {len(ROLES)} x {INSTANCE_TYPE} on-demand ({HOST_VCPUS} vCPU each), one run_instances into"
        f" cluster placement group {NAME}, one subnet/AZ; each: primary ENI (SSH, public IPv4) + 1 DPDK data ENI"
        " (vfio-pci, DeleteOnTermination); CPUs from hft-host.sh: one isolated APP_CPU, the rest HOUSEKEEPING",
        f"  {'receiver':9} zhida-bench serve on APP_CPU (systemd unit {RECEIVER_UNIT})",
        f"  {'sender0':9} zhida-bench sweep (coordinator) on HOUSEKEEPING; worker 0 (zhida-bench gen) on APP_CPU",
    ]
    lines += [f"  {f'sender{i}':9} worker {i} (zhida-bench gen, over SSH from sender0) on APP_CPU"
              for i in range(1, SENDER_HOSTS)]
    lines += [
        f"sweeps: {SENDERS} senders ({SENDERS // SENDER_HOSTS} per sender host), {SIZE} B messages,"
        f" {RATE_MIN}..{RATE_MAX} msg/s in {SWEEP_STEPS} steps of x sqrt(2); coordinator timeout {SWEEP_WORST_S} s",
    ]
    lines += [f"  {t}" + ("" if b is None else f"  (--budget-us {b})") for t, (p, b) in zip(sweep_tags(), SWEEPS)]
    lines += [
        f"needs: on-demand Standard vCPU quota L-1216C47A with {len(ROLES) * HOST_VCPUS} vCPUs free in {region};"
        " permissions: see `zhida_aws_bench.py -h`",
        f"billable while the run lives: {len(ROLES)} instances, {len(ROLES)} x {ROOT_GB} GB gp3 root volumes,"
        f" {len(ROLES)} public IPv4 addresses",
        "expiry: each instance powers itself off (terminates) at the TTL; the runner tears down on exit,"
        " Ctrl-C and SIGTERM",
    ]
    return "\n".join(lines)


def error_code(e):
    return getattr(e, "response", {}).get("Error", {}).get("Code")


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

    def shared_one(self, describe, key, filters):
        items = describe(Filters=[{"Name": f"tag:{janitor.SHARED_TAG}", "Values": ["shared"]}] + filters)[key]
        return items[0] if items else None

    def ensure_shared(self, my_ip):
        """Resources reused across runs; idempotent."""
        ec2 = self.ec2

        def tagspec(kind):
            return [{"ResourceType": kind, "Tags": [SHARED, {"Key": "Name", "Value": NAME}]}]

        vpc = self.shared_one(ec2.describe_vpcs, "Vpcs", [])
        if vpc is None:
            vpc = ec2.create_vpc(CidrBlock=VPC_CIDR, TagSpecifications=tagspec("vpc"))["Vpc"]
            ec2.get_waiter("vpc_available").wait(VpcIds=[vpc["VpcId"]])
            ec2.modify_vpc_attribute(VpcId=vpc["VpcId"], EnableDnsHostnames={"Value": True})
        vpc_id = vpc["VpcId"]
        in_vpc = [{"Name": "vpc-id", "Values": [vpc_id]}]
        subnet = self.shared_one(ec2.describe_subnets, "Subnets", in_vpc)
        if subnet is None:
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock=SUBNET_CIDR, AvailabilityZone=self.zone_offering(),
                                       TagSpecifications=tagspec("subnet"))["Subnet"]
        igw = self.shared_one(ec2.describe_internet_gateways, "InternetGateways", [])
        if igw is None:
            igw = ec2.create_internet_gateway(TagSpecifications=tagspec("internet-gateway"))["InternetGateway"]
            ec2.attach_internet_gateway(InternetGatewayId=igw["InternetGatewayId"], VpcId=vpc_id)
        rt = self.shared_one(ec2.describe_route_tables, "RouteTables", in_vpc)
        if rt is None:
            rt = ec2.create_route_table(VpcId=vpc_id, TagSpecifications=tagspec("route-table"))["RouteTable"]
            ec2.create_route(RouteTableId=rt["RouteTableId"], DestinationCidrBlock="0.0.0.0/0",
                             GatewayId=igw["InternetGatewayId"])
            ec2.associate_route_table(RouteTableId=rt["RouteTableId"], SubnetId=subnet["SubnetId"])
        sg = self.shared_one(ec2.describe_security_groups, "SecurityGroups", in_vpc)
        if sg is None:
            sg_id = ec2.create_security_group(GroupName=NAME, Description="zhida-bench hosts", VpcId=vpc_id,
                                              TagSpecifications=tagspec("security-group"))["GroupId"]
            ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=[
                {"IpProtocol": "-1", "UserIdGroupPairs": [{"GroupId": sg_id}]}])
        else:
            sg_id = sg["GroupId"]
        try:
            ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=[
                {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": f"{my_ip}/32"}]}])
        except Exception as e:
            if error_code(e) != "InvalidPermission.Duplicate":
                raise
        if not ec2.describe_placement_groups(Filters=[{"Name": "group-name", "Values": [NAME]}])["PlacementGroups"]:
            ec2.create_placement_group(GroupName=NAME, Strategy="cluster", TagSpecifications=tagspec("placement-group"))
        key_path = shared_key_path(self.region)
        keys = ec2.describe_key_pairs(Filters=[{"Name": "key-name", "Values": [NAME]}])["KeyPairs"]
        if keys and not os.path.exists(key_path):
            raise RuntimeError(f"key pair {NAME} exists but {key_path} is missing: restore the file, or run"
                               " `cleanup --shared` and start over")
        if not keys:
            os.makedirs(KEY_DIR, mode=0o700, exist_ok=True)
            material = ec2.create_key_pair(KeyName=NAME, TagSpecifications=tagspec("key-pair"))["KeyMaterial"]
            with open(os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
                f.write(material)
        return {"vpc": vpc_id, "subnet": subnet["SubnetId"], "zone": subnet["AvailabilityZone"], "sg": sg_id,
                "key_path": key_path}

    def zone_offering(self):
        offered = self.ec2.describe_instance_type_offerings(
            LocationType="availability-zone",
            Filters=[{"Name": "instance-type", "Values": [INSTANCE_TYPE]}])["InstanceTypeOfferings"]
        zones = sorted(o["Location"] for o in offered)
        if not zones:
            raise RuntimeError(f"no zone in {self.region} offers {INSTANCE_TYPE}")
        return zones[0]

    def launch_hosts(self, shared, run_id, expires):
        """Six instances in one run_instances into the placement group; then one data ENI each
        (create, attach, DeleteOnTermination). Returns host records in ROLES order."""
        ec2 = self.ec2
        ami = self.ssm.get_parameter(Name=AMI_PARAM)["Parameter"]["Value"]
        tags = run_tags(run_id, expires)
        r = ec2.run_instances(
            ImageId=ami, InstanceType=INSTANCE_TYPE, MinCount=len(ROLES), MaxCount=len(ROLES), KeyName=NAME,
            InstanceInitiatedShutdownBehavior="terminate",
            Placement={"GroupName": NAME, "AvailabilityZone": shared["zone"]},
            NetworkInterfaces=[{"DeviceIndex": 0, "SubnetId": shared["subnet"], "Groups": [shared["sg"]],
                                "AssociatePublicIpAddress": True, "DeleteOnTermination": True}],
            BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
                "VolumeSize": ROOT_GB, "VolumeType": "gp3", "DeleteOnTermination": True}}],
            UserData=user_data(expires),
            TagSpecifications=[{"ResourceType": k, "Tags": tags} for k in ("instance", "volume", "network-interface")])
        ids = [i["InstanceId"] for i in sorted(r["Instances"], key=lambda i: i["AmiLaunchIndex"])]
        ec2.get_waiter("instance_running").wait(InstanceIds=ids)
        enis = {}
        for iid in ids:
            eni = ec2.create_network_interface(
                SubnetId=shared["subnet"], Groups=[shared["sg"]],
                TagSpecifications=[{"ResourceType": "network-interface", "Tags": tags}])["NetworkInterface"]
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
                for iid, role in zip(ids, ROLES)]


# ---------------------------------------------------------------- hosts


class Host:
    def __init__(self, info, key_path, run_dir, run=subprocess.run, sleep=time.sleep, clock=time.monotonic):
        self.info, self.key, self.run_dir = info, key_path, run_dir
        self.role = info["role"]
        self._run, self._sleep, self._clock = run, sleep, clock

    def _opts(self):
        return ["-i", self.key, "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes",
                "-o", f"UserKnownHostsFile={self.run_dir}/known_hosts", "-o", "ConnectTimeout=10"]

    def ssh(self, command, check=True, timeout=None, input=None):
        argv = ["ssh", *self._opts(), "-o", "ServerAliveInterval=15", f"ec2-user@{self.info['public_ip']}", command]
        r = self._run(argv, capture_output=True, input=input, text=not isinstance(input, bytes), timeout=timeout)
        if check and r.returncode != 0:
            err = r.stderr.decode(errors="replace") if isinstance(r.stderr, bytes) else (r.stderr or "")
            raise RuntimeError(f"{self.role}: `{command[:120]}` exited {r.returncode}: {err[-2000:]}")
        return r

    def root(self, command, **kw):
        return self.ssh(f"sudo sh -c {shlex.quote(command)}", **kw)

    def wait_ssh(self, limit=600):
        deadline = self._clock() + limit
        while self._clock() < deadline:
            if self.ssh("true", check=False).returncode == 0:
                return
            self._sleep(5)
        raise RuntimeError(f"{self.role}: no SSH within {limit} s")

    def put_text(self, remote, text, mode):
        self.ssh(f"umask 077 && cat > {remote} && chmod {mode} {remote}", input=text)

    def fetch(self, remote, local):
        """scp remote -> local; None on success, else what failed."""
        argv = ["scp", "-q", *self._opts(), f"ec2-user@{self.info['public_ip']}:{remote}", local]
        r = self._run(argv, capture_output=True, text=True)
        if r.returncode == 0:
            return None
        return f"{self.role}:{remote}: scp exited {r.returncode}: {(r.stderr or '').strip()[-500:]}"

    def setup(self, archive):
        """Source, hft-host.sh apply (builds, reboots), verify, then zhida-bench (bootstrap.sh).
        Returns (ok, report text)."""
        self.ssh(f"sudo rm -rf {SRC_DIR} && sudo mkdir -p {SRC_DIR} && sudo tar -x -C {SRC_DIR}", input=archive)
        apply = self.root(f"sh {HFT} apply", check=False, timeout=BUILD_S)
        text = [f"[{self.role} {self.info['id']} {INSTANCE_TYPE}]", "apply:", apply.stdout.rstrip()]
        if apply.returncode not in (0, 100, 255):  # 100: rebooting; 255: SSH dropped by the reboot
            return False, "\n".join(text + [f"apply exited {apply.returncode}: {apply.stderr.strip()[-2000:]}"])
        # Before the reboot verify fails (cmdline), during it SSH fails; zhida-host.service runs `boot`
        # after it. So: retry verify until it passes.
        deadline = self._clock() + SETUP_REBOOT_S
        while True:
            v = self.root(f"sh {HFT} verify", check=False)
            if v.returncode == 0 or self._clock() >= deadline:
                break
            self._sleep(10)
        text += ["verify:", v.stdout.rstrip()]
        if v.returncode != 0:
            return False, "\n".join(text)
        self.root(f"SRC={SRC_DIR} sh {SRC_DIR}/bench/aws/bootstrap.sh", timeout=BUILD_S)
        return True, "\n".join(text)


# ---------------------------------------------------------------- run


class Env:
    """What run() touches outside this process; tests substitute fakes."""

    def __init__(self, aws=Aws, host=Host, local=subprocess.run, my_ip=None, now=None, sleep=time.sleep):
        self.aws, self.host, self.local, self.sleep = aws, host, local, sleep
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


def quota_check(aws):
    """(ok, detail): L-1216C47A minus the running on-demand vCPUs leaves room for six hosts. Counts
    every family (the quota covers only Standard ones), so it can only err towards refusing."""
    need = len(ROLES) * HOST_VCPUS
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


def preflight(env, region):
    """Every check, before anything is created: ([(name, ok, detail)], Aws)."""
    problems = tree_problems(env.local)
    out = [("source", not problems, "; ".join(problems) or "clean, HEAD has deploy/hft-host.sh")]
    aws = env.aws(region)
    try:
        who = aws.sts.get_caller_identity()
        out.append(("credentials", True, f"account {who['Account']} as {who['Arn']}"))
    except Exception as e:
        return out + [("credentials", False, str(e)), ("quota", False, "not checked without credentials")], aws
    try:
        out.append(("quota", *quota_check(aws)))
    except Exception as e:
        out.append(("quota", False, str(e)))
    return out, aws


def checked_preflight(env, region):
    results, aws = preflight(env, region)
    for name, ok, detail in results:
        log(f"preflight {name}: {'ok' if ok else 'FAIL'} {detail}")
    if not all(ok for _, ok, _ in results):
        raise SystemExit("preflight failed; nothing was created")
    return aws


def source_archive(local):
    return local(["git", "-C", REPO, "archive", "--format=tar", "HEAD"], capture_output=True, check=True).stdout


def setup_hosts(hosts, archive):
    """All six in parallel; raises listing every host that failed."""
    results, threads = {}, []

    def one(h):
        try:
            results[h.role] = h.setup(archive)
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


def connect_senders(hosts, run_dir, run_id, local):
    """A run-scoped key: sender0 reaches sender0..4 as ec2-user through REMOTE/ssh_config."""
    key = os.path.join(run_dir, "run_key")
    local(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", f"{NAME}-{run_id}", "-f", key], check=True)
    with open(key) as f:
        private = f.read()
    with open(key + ".pub") as f:
        public = f.read().strip()
    senders = hosts[1:]
    for h in senders:
        h.ssh(f"grep -qxF {shlex.quote(public)} ~/.ssh/authorized_keys || echo {shlex.quote(public)}"
              " >> ~/.ssh/authorized_keys")
    sender0 = senders[0]
    sender0.put_text(f"{REMOTE}/id_run", private, "600")
    sender0.put_text(f"{REMOTE}/ssh_config", ssh_config([h.info["private_ip"] for h in senders]), "644")
    sender0.ssh(f"rm -f {REMOTE}/known_hosts")


def fetch_sweep(t, hosts, run_dir):
    """Every file of sweep `t` into run_dir; returns the failures."""
    senders = hosts[1:]
    wanted = [(senders[0], f"{REMOTE}/{t}.jsonl", run_dir), (senders[0], f"{REMOTE}/{t}.coord.log", run_dir)]
    for h in senders:
        d = os.path.join(run_dir, h.role)
        os.makedirs(d, exist_ok=True)
        wanted.append((h, f"{REMOTE}/gen-{t}.log", d))
    return [e for e in (h.fetch(remote, local) for h, remote, local in wanted) if e]


def run_sweep(proto, budget, hosts, facts, receiver_ip, env):
    """One sweep: receiver unit up, coordinator on sender0, receiver stopped. Returns the coordinator's
    exit code, or None when it did not run or timed out."""
    receiver, sender0 = hosts[0], hosts[1]
    t = tag(proto, budget)
    receiver.root(f"systemctl stop {RECEIVER_UNIT} 2>/dev/null; systemctl reset-failed {RECEIVER_UNIT} 2>/dev/null;"
                  f" {serve_command(proto, facts['receiver']['APP_CPU'])}")
    try:
        env.sleep(3)
        if receiver.root(f"systemctl is-active {RECEIVER_UNIT}", check=False).stdout.strip() != "active":
            journal = receiver.root(f"journalctl -u {RECEIVER_UNIT} --no-pager | tail -20", check=False).stdout
            log(f"{t}: receiver did not start:\n{journal}")
            return None
        try:
            return sender0.ssh(sweep_command(proto, budget, receiver_ip, facts["sender0"]["HOUSEKEEPING"]),
                               check=False, timeout=SWEEP_WORST_S).returncode
        except subprocess.TimeoutExpired:
            log(f"{t}: coordinator still running after {SWEEP_WORST_S} s; killed")
            sender0.ssh("pkill -f 'zhida-bench sweep'", check=False, timeout=60)
            return None
    finally:
        receiver.root(f"systemctl stop {RECEIVER_UNIT}", check=False)


def measure(hosts, facts, receiver_ip, run_dir, env):
    """The fixed matrix. A failed sweep is logged and the next one runs; every sweep started is
    fetched. Returns the fetch failures."""
    with open(os.path.join(run_dir, "expected.json"), "w") as f:
        json.dump({"sweeps": sweep_tags()}, f)
    fetch_errors = []
    for proto, budget in SWEEPS:
        t = tag(proto, budget)
        try:
            log(f"{t}: coordinator exit {run_sweep(proto, budget, hosts, facts, receiver_ip, env)}")
        finally:
            fetch_errors += fetch_sweep(t, hosts, run_dir)
    return fetch_errors


def run(args, env=None):
    env = env or Env()
    now = env.now()
    expires = now + datetime.timedelta(seconds=parse_duration(args.ttl))
    run_id = new_run_id(now)
    print(plan_text(args.region, args.ttl, run_id, expires))
    aws = checked_preflight(env, args.region)
    archive = source_archive(env.local)
    run_dir = os.path.join(args.out, run_id)
    os.makedirs(run_dir, exist_ok=True)
    shared = aws.ensure_shared(env.my_ip())

    def on_signal(signum, _frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    fetch_errors = []
    # From here on every exit path deletes the run's billable resources.
    try:
        infos = aws.launch_hosts(shared, run_id, expires)
        with open(os.path.join(run_dir, "hosts.json"), "w") as f:
            json.dump(infos, f, indent=1)
        hosts = [env.host(info, shared["key_path"], run_dir) for info in infos]
        for h in hosts:
            h.wait_ssh()
        setup = setup_hosts(hosts, archive)
        facts, receiver_ip = read_facts(hosts, infos[0]["data_eni"]["ip"])
        for h in hosts:
            h.root(f"install -d -m 1777 {REMOTE}")
        setup += [f"[{r} host.env] " + " ".join(f"{k}={v}" for k, v in sorted(facts[r].items())) for r in ROLES]
        with open(os.path.join(run_dir, "host-setup.txt"), "w") as f:
            f.write("\n".join(setup) + "\n")
        connect_senders(hosts, run_dir, run_id, env.local)
        fetch_errors = measure(hosts, facts, receiver_ip, run_dir, env)
    finally:
        # teardown first: the report is local, and a second Ctrl-C during it must not skip teardown
        log(f"tearing down run {run_id}")
        torn = teardown(aws.ec2, run_id)
        report_code = None
        if os.path.exists(os.path.join(run_dir, "expected.json")):
            if fetch_errors:
                with open(os.path.join(run_dir, "fetch-errors.txt"), "w") as f:
                    f.write("\n".join(fetch_errors) + "\n")
                log(f"{len(fetch_errors)} fetch failures: {os.path.join(run_dir, 'fetch-errors.txt')}")
            report_code = env.local([sys.executable, os.path.join(REPO, "bench", "report.py"), run_dir, "--title",
                                     f"zhida-bench {run_id} (1 receiver + {SENDER_HOSTS} senders, {INSTANCE_TYPE})"]
                                    ).returncode
            log(f"report: {os.path.join(run_dir, 'report.md')} (report.py exit {report_code})")
    return 0 if report_code == 0 and not fetch_errors and torn else 1


def teardown(ec2, run_id):
    """Deletes the run's resources; True only when a describe found none of them left."""
    remaining = janitor.clean_run(ec2, run_id, log=log)
    if remaining:
        log(f"run {run_id}: STILL PRESENT after teardown (rerun `cleanup --run-id {run_id}`): {'; '.join(remaining)}")
        return False
    log(f"run {run_id}: nothing left")
    return True


def cleanup(args, env=None):
    aws = (env or Env()).aws(args.region)
    if args.run_id:
        return 0 if teardown(aws.ec2, args.run_id) else 1
    remaining = janitor.clean_shared(aws.ec2, shared_key_path(args.region), log=log)
    if remaining:
        log(f"cleanup --shared: STILL PRESENT: {'; '.join(remaining)}")
        return 1
    log("cleanup --shared: nothing left")
    return 0


def parser():
    p = argparse.ArgumentParser(prog="zhida-aws-bench", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "plan", "preflight"):
        r = sub.add_parser(name)
        r.add_argument("--region", default=REGION)
        r.add_argument("--ttl", default=TTL, help="hard expiry of everything billable (e.g. 90m, 2h)")
        r.add_argument("--out", default=os.path.join(REPO, "bench", "out", "aws"))
    c = sub.add_parser("cleanup")
    which = c.add_mutually_exclusive_group(required=True)
    which.add_argument("--run-id")
    which.add_argument("--shared", action="store_true")
    c.add_argument("--region", default=REGION)
    return p


def main(argv, env=None):
    args = parser().parse_args(argv)
    if args.cmd == "preflight":
        checked_preflight(env or Env(), args.region)
        return 0
    if args.cmd == "plan":
        now = datetime.datetime.now(datetime.timezone.utc)
        print(plan_text(args.region, args.ttl, new_run_id(now),
                        now + datetime.timedelta(seconds=parse_duration(args.ttl))))
        return 0
    if args.cmd == "run":
        return run(args, env)
    return cleanup(args, env)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
