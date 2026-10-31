#!/usr/bin/env python3
"""zhida-aws-bench: the latency/load bench on EC2 in one command, 1 receiver + 5 sender hosts.

  zhida_aws_bench.py plan                                 print hosts, sweeps and needs; no AWS calls
  zhida_aws_bench.py preflight [--region R]               check tree, credentials, vCPU quota
  zhida_aws_bench.py run [--region R] [--ttl 2h] [--out DIR]
  zhida_aws_bench.py cleanup --run-id ID [--region R] [--out DIR]   delete a leftover run's resources

Needs python3 with boto3, ssh, scp, git, a clean committed tree (the hosts build `git archive HEAD`)
and AWS credentials for the region. Design: plans/bench/design.md.
`preflight` (also the first step of `run`) checks the tree, the credentials and the on-demand
Standard vCPU quota (L-1216C47A: 48 free) before anything is created.

Permissions the caller needs: ec2 (VPC, subnet, internet gateway, route table, security group,
placement group, key pair, run/terminate/describe instances, create/attach/delete/modify network
interfaces, delete volumes, create tags, describe instance type offerings), ssm:GetParameter (the
AL2023 AMI), sts:GetCallerIdentity, servicequotas:GetServiceQuota.

`run` creates a VPC, subnet, internet gateway, route table, security group (SSH from the caller's
address), cluster placement group and key pair for the run; provisions six c7gn.2xlarge on-demand
instances (one run_instances) in the placement group, one data ENI each; applies
`deploy/hft-host.sh` (builds DPDK and the toolchain, reboots, verify), then builds zhida-bench
natively (bootstrap.sh); runs the fixed sweep matrix with the coordinator on sender0 driving one
worker per sender host over SSH; fetches every result; tears down; runs bench/report.py. Every
resource carries the run id and expiry tags and is recorded in RUN_DIR/created.json as it is
created; teardown deletes all of them and exits non-zero if a final describe still finds any. Each
instance powers itself off (terminate) at the TTL. Teardown runs on exit, errors, Ctrl-C and SIGTERM.
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
AMI_PARAM = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
VPC_CIDR, SUBNET_CIDR = "10.80.0.0/16", "10.80.1.0/24"
NAME = janitor.NAME
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
# a backstop only: the coordinator bounds its own steps and workers
SWEEP_WORST_S = 15 * 60
BUILD_S = 3600
SETUP_REBOOT_S = 900
# The bound of every ssh/scp without its own: the receiver stop (systemd SIGKILLs after 90 s), the
# fetches while unwinding to teardown.
SSH_S = 120
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
        f" its cluster placement group, one subnet/AZ; each: primary ENI (SSH, public IPv4) + 1 DPDK data ENI"
        " (vfio-pci, DeleteOnTermination); CPUs from hft-host.sh: one isolated APP_CPU, the rest HOUSEKEEPING",
        f"  {'receiver':9} zhida-bench serve on APP_CPU (systemd unit {RECEIVER_UNIT})",
        f"  {'sender0':9} zhida-bench sweep (coordinator) on HOUSEKEEPING; worker 0 (zhida-bench gen) on APP_CPU",
    ]
    lines += [f"  {f'sender{i}':9} worker {i} (zhida-bench gen, over SSH from sender0) on APP_CPU"
              for i in range(1, SENDER_HOSTS)]
    lines += [
        f"sweeps: {SENDERS} senders ({SENDERS // SENDER_HOSTS} per sender host), {SIZE} B messages,"
        f" {RATE_MIN}..{RATE_MAX} msg/s in steps of x sqrt(2); coordinator killed after {SWEEP_WORST_S} s",
    ]
    lines += [f"  {t}" + ("" if b is None else f"  (--budget-us {b})") for t, (p, b) in zip(sweep_tags(), SWEEPS)]
    lines += [
        f"needs: on-demand Standard vCPU quota L-1216C47A with {len(ROLES) * HOST_VCPUS} vCPUs free in {region};"
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

    def create_network(self, run_id, expires, my_ip, record, key_path, instance_type, strategy):
        """The run's VPC, subnet (in a zone offering `instance_type`), internet gateway and route,
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
        zone = self.zone_offering(instance_type)
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

    def zone_offering(self, instance_type):
        offered = self.ec2.describe_instance_type_offerings(
            LocationType="availability-zone",
            Filters=[{"Name": "instance-type", "Values": [instance_type]}])["InstanceTypeOfferings"]
        zones = sorted(o["Location"] for o in offered)
        if not zones:
            raise RuntimeError(f"no zone in {self.region} offers {instance_type}")
        return zones[0]

    def launch_hosts(self, net, run_id, expires, record, instance_type, roles):
        """One instance per role in one run_instances into the placement group; then one data ENI
        each (create, attach, DeleteOnTermination); every ID passed to `record` as its create
        succeeds. Returns host records in `roles` order."""
        ec2 = self.ec2
        ami = self.ssm.get_parameter(Name=AMI_PARAM)["Parameter"]["Value"]
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
        return ["-i", self.key, "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes",
                "-o", f"UserKnownHostsFile={self.run_dir}/known_hosts", "-o", "ConnectTimeout=10",
                "-o", "ServerAliveInterval=15"]

    def ssh(self, command, check=True, timeout=None, input=None):
        argv = ["ssh", *self._opts(), f"ec2-user@{self.info['public_ip']}", command]
        r = self._run(argv, capture_output=True, input=input, text=not isinstance(input, bytes),
                      timeout=timeout or SSH_S)
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
        try:
            r = self._run(argv, capture_output=True, text=True, timeout=SSH_S)
        except subprocess.TimeoutExpired:
            return f"{self.role}:{remote}: scp timed out after {SSH_S} s"
        if r.returncode == 0:
            return None
        return f"{self.role}:{remote}: scp exited {r.returncode}: {(r.stderr or '').strip()[-500:]}"

    def setup(self, archive):
        """Source, hft-host.sh apply (builds, reboots), verify, then zhida-bench (bootstrap.sh).
        Returns (ok, report text)."""
        self.ssh(f"sudo rm -rf {SRC_DIR} && sudo mkdir -p {SRC_DIR} && sudo tar -x -C {SRC_DIR}", input=archive,
                 timeout=BUILD_S)
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
    fetched. Returns (fetch failures, sweeps whose coordinator did not exit 0)."""
    with open(os.path.join(run_dir, "expected.json"), "w") as f:
        json.dump({"sweeps": sweep_tags()}, f)
    fetch_errors, failed = [], []
    for proto, budget in SWEEPS:
        t = tag(proto, budget)
        try:
            code = run_sweep(proto, budget, hosts, facts, receiver_ip, env)
            log(f"{t}: coordinator exit {code}")
            if code != 0:
                failed.append(t)
        finally:
            fetch_errors += fetch_sweep(t, hosts, run_dir)
    return fetch_errors, failed


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
    created = janitor.Created(os.path.join(run_dir, "created.json"))
    key_path = os.path.join(run_dir, "key.pem")
    my_ip = env.my_ip()

    def on_signal(signum, _frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    fetch_errors, failed = [], ["(not measured)"]
    # From here on every exit path deletes the run's resources.
    try:
        net = aws.create_network(run_id, expires, my_ip, created.add, key_path, INSTANCE_TYPE, "cluster")
        infos = aws.launch_hosts(net, run_id, expires, created.add, INSTANCE_TYPE, ROLES)
        with open(os.path.join(run_dir, "hosts.json"), "w") as f:
            json.dump(infos, f, indent=1)
        hosts = [env.host(info, key_path, run_dir) for info in infos]
        for h in hosts:
            h.wait_ssh()
        setup = setup_hosts(hosts, archive)
        facts, receiver_ip = read_facts(hosts, infos[0]["data_eni"]["ip"])
        for h in hosts:
            h.root(f"install -d -m 1777 {REMOTE}")
        setup += [f"[{r} host.env] " + " ".join(f"{k}={v}" for k, v in sorted(facts[r].items())) for r in ROLES]
        with open(os.path.join(run_dir, "host-setup.txt"), "w") as f:
            f.write("\n".join(setup) + "\n")
        connect_senders(hosts, key_path)
        fetch_errors, failed = measure(hosts, facts, receiver_ip, run_dir, env)
    finally:
        # Nothing interrupts teardown; it goes first because the report is local.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        log(f"tearing down run {run_id}")
        torn = teardown(aws.ec2, run_id, created.ids)
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
        if failed:
            log(f"sweeps that failed or were generator-bound: {', '.join(failed)}")
    return 0 if report_code == 0 and not fetch_errors and not failed and torn else 1


def teardown(ec2, run_id, recorded):
    """Deletes the run's resources; True only when nothing was found left and nothing errored."""
    problems = janitor.clean_run(ec2, run_id, recorded, log=log)
    if problems:
        log(f"run {run_id}: NOT VERIFIED GONE; rerun `cleanup --run-id {run_id}`: {'; '.join(problems)}")
        return False
    log(f"run {run_id}: nothing left")
    return True


def cleanup(args, env=None):
    """A leftover run: the IDs its run_dir/created.json recorded, plus a tag lookup."""
    aws = (env or Env()).aws(args.region)
    path = os.path.join(args.out, args.run_id, "created.json")
    recorded = janitor.read_created(path) if os.path.exists(path) else {}
    log(f"run {args.run_id}: {sum(map(len, recorded.values()))} recorded IDs from {path}" if recorded
        else f"run {args.run_id}: no {path}; tag lookup only")
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    return 0 if teardown(aws.ec2, args.run_id, recorded) else 1


def parser():
    p = argparse.ArgumentParser(prog="zhida-aws-bench", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "plan", "preflight", "cleanup"):
        r = sub.add_parser(name)
        r.add_argument("--region", default=REGION)
        r.add_argument("--out", default=os.path.join(REPO, "bench", "out", "aws"))
        if name == "cleanup":
            r.add_argument("--run-id", required=True)
        else:
            r.add_argument("--ttl", default=TTL, help="hard expiry of everything billable (e.g. 90m, 2h)")
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
