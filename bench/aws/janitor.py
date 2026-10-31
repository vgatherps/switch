"""Deletes a bench run's billable AWS resources, found by tag. Used by the runner's teardown, by
`zhida-aws-bench cleanup`, and (unchanged) as the AWS Lambda behind each run's one-shot
EventBridge Scheduler schedule, so a run expires at its TTL even when the laptop is gone.

Billable per-run resources carry RUN_TAG=<run id> and EXPIRES_TAG=<UTC ISO time>: the spot
instances (their root volumes and ENIs are DeleteOnTermination, so they go with them), ENIs or
volumes left unattached if a launch failed half way, and the Binance smoke's EIP. Instances are
terminated first and waited for, so the EIP and ENIs they held are free when deleted.

Every call is idempotent: a second clean of the same run finds nothing and deletes nothing.
"""
import datetime
import time

RUN_TAG = "zhida-bench-run"
EXPIRES_TAG = "zhida-bench-expires"
SHARED_TAG = "zhida-bench"
SCHEDULE_PREFIX = "zhida-bench-"


def tags(resource):
    return {t["Key"]: t["Value"] for t in resource.get("Tags", [])}


def run_filter(run_id):
    if run_id is None:
        return [{"Name": "tag-key", "Values": [RUN_TAG]}]
    return [{"Name": f"tag:{RUN_TAG}", "Values": [run_id]}]


def expired(resource, now):
    """Whether the resource's TTL tag is past `now` (an aware datetime). Untagged: not expired."""
    value = tags(resource).get(EXPIRES_TAG)
    if value is None:
        return False
    return datetime.datetime.fromisoformat(value) <= now


def selected(resource, run_id, now):
    """A resource of run `run_id`, or (run_id None) of any run whose TTL has passed."""
    t = tags(resource)
    if RUN_TAG not in t:
        return False
    return t[RUN_TAG] == run_id if run_id is not None else expired(resource, now)


def clean(ec2, scheduler=None, run_id=None, now=None, wait_s=600, poll_s=5, log=print):
    """Deletes run `run_id`'s resources, or with run_id None those of every expired run.
    Returns what it deleted, by kind."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    done = {"instances": [], "addresses": [], "network_interfaces": [], "volumes": [], "schedules": []}
    instances = [
        i
        for r in ec2.describe_instances(Filters=run_filter(run_id))["Reservations"]
        for i in r["Instances"]
        if i["State"]["Name"] not in ("terminated",) and selected(i, run_id, now)
    ]
    runs = {tags(i)[RUN_TAG] for i in instances}
    live = [i["InstanceId"] for i in instances if i["State"]["Name"] != "shutting-down"]
    if live:
        ec2.terminate_instances(InstanceIds=live)
        log(f"janitor: terminating {', '.join(live)}")
    ids = [i["InstanceId"] for i in instances]
    done["instances"] = ids
    deadline = time.monotonic() + wait_s
    while ids:
        states = {
            i["InstanceId"]: i["State"]["Name"]
            for r in ec2.describe_instances(InstanceIds=ids)["Reservations"]
            for i in r["Instances"]
        }
        ids = [i for i in ids if states.get(i, "terminated") != "terminated"]
        if not ids:
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"instances not terminated within {wait_s} s: {ids}")
        time.sleep(poll_s)
    for a in ec2.describe_addresses(Filters=run_filter(run_id))["Addresses"]:
        if not selected(a, run_id, now):
            continue
        runs.add(tags(a)[RUN_TAG])
        if "AssociationId" in a:
            ec2.disassociate_address(AssociationId=a["AssociationId"])
        ec2.release_address(AllocationId=a["AllocationId"])
        done["addresses"].append(a["AllocationId"])
        log(f"janitor: released {a['AllocationId']} ({a.get('PublicIp')})")
    for n in ec2.describe_network_interfaces(Filters=run_filter(run_id))["NetworkInterfaces"]:
        if n["Status"] == "available" and selected(n, run_id, now):
            runs.add(tags(n)[RUN_TAG])
            ec2.delete_network_interface(NetworkInterfaceId=n["NetworkInterfaceId"])
            done["network_interfaces"].append(n["NetworkInterfaceId"])
            log(f"janitor: deleted {n['NetworkInterfaceId']}")
    for v in ec2.describe_volumes(Filters=run_filter(run_id))["Volumes"]:
        if v["State"] == "available" and selected(v, run_id, now):
            runs.add(tags(v)[RUN_TAG])
            ec2.delete_volume(VolumeId=v["VolumeId"])
            done["volumes"].append(v["VolumeId"])
            log(f"janitor: deleted {v['VolumeId']}")
    if scheduler is not None:
        for run in sorted(runs | ({run_id} if run_id else set())):
            name = SCHEDULE_PREFIX + run
            try:
                scheduler.delete_schedule(Name=name)
                done["schedules"].append(name)
            except scheduler.exceptions.ResourceNotFoundException:
                pass
    return done


def lambda_handler(event, context):
    """The Lambda behind a run's schedule: event {"run_id": ...}."""
    import boto3

    return clean(boto3.client("ec2"), boto3.client("scheduler"), run_id=event["run_id"], wait_s=600)
