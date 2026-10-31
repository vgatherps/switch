"""Deletes what a bench run created in AWS; used by the runner's teardown and by
`zhida-aws-bench cleanup --run-id`.

A run creates a VPC, subnet, internet gateway, route table, security group, placement group, key
pair, instances (with their root volumes and primary ENIs), one data ENI each and, for `binance`, an
Elastic IP on the data ENI, all tagged RUN_TAG=<run id> through TagSpecifications. The runner records each ID as its create succeeds
(Created: in memory and run_dir/created.json). clean_run uses both sources: a resource is present
while a describe finds it by tag or by recorded ID, or while it is recorded and no delete of it has
yet succeeded or answered not-found. Each round deletes what is present, dependents first; a delete
that fails (instance still shutting down, ENI in use, a dependency) is retried next round. It
returns [] only when a round finds nothing and no describe failed and no recorded ID answered
not-found (a fresh ID can be invisible for a while); otherwise the caller says to rerun
`cleanup --run-id`.
"""
import json
import time

NAME = "zhida-bench"
RUN_TAG = "zhida-bench-run"
EXPIRES_TAG = "zhida-bench-expires"

# kind: (describe operation, response key, ID field = delete parameter, describe filter on the ID,
# delete operation), in deletion order
KINDS = {
    "instance": ("describe_instances", "Reservations", "InstanceId", "instance-id", "terminate_instances"),
    "address": ("describe_addresses", "Addresses", "AllocationId", "allocation-id", "release_address"),
    "network-interface": ("describe_network_interfaces", "NetworkInterfaces", "NetworkInterfaceId",
                          "network-interface-id", "delete_network_interface"),
    "volume": ("describe_volumes", "Volumes", "VolumeId", "volume-id", "delete_volume"),
    "key-pair": ("describe_key_pairs", "KeyPairs", "KeyName", "key-name", "delete_key_pair"),
    "placement-group": ("describe_placement_groups", "PlacementGroups", "GroupName", "group-name",
                        "delete_placement_group"),
    "security-group": ("describe_security_groups", "SecurityGroups", "GroupId", "group-id", "delete_security_group"),
    "subnet": ("describe_subnets", "Subnets", "SubnetId", "subnet-id", "delete_subnet"),
    "route-table": ("describe_route_tables", "RouteTables", "RouteTableId", "route-table-id", "delete_route_table"),
    "internet-gateway": ("describe_internet_gateways", "InternetGateways", "InternetGatewayId",
                         "internet-gateway-id", "delete_internet_gateway"),
    "vpc": ("describe_vpcs", "Vpcs", "VpcId", "vpc-id", "delete_vpc"),
}
UNPAGED = {"describe_addresses", "describe_key_pairs", "describe_placement_groups"}


class Created:
    """The run's resource IDs by kind, recorded as each create succeeds; each also appended to
    `path` as one JSON line ({"kind": ..., "id": ...}), which `cleanup --run-id` reads back."""

    def __init__(self, path):
        self.path, self.ids = path, {}

    def add(self, kind, rid):
        self.ids.setdefault(kind, []).append(rid)
        with open(self.path, "a") as f:
            f.write(json.dumps({"kind": kind, "id": rid}) + "\n")


def read_created(path):
    ids = {}
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            ids.setdefault(r["kind"], []).append(r["id"])
    return ids


def not_found(e):
    code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
    return code.endswith(".NotFound") or code == "InvalidPlacementGroup.Unknown"


def describe(ec2, kind, filters):
    """Every resource of `kind` matching `filters` (all pages); instances not yet terminated."""
    op, key = KINDS[kind][:2]
    if op in UNPAGED:
        items = getattr(ec2, op)(Filters=filters)[key]
    else:
        items = [x for page in ec2.get_paginator(op).paginate(Filters=filters) for x in page[key]]
    if kind == "instance":
        items = [i for r in items for i in r["Instances"] if i["State"]["Name"] != "terminated"]
    return items


def present(ec2, run_id, recorded, unconfirmed, problems, log):
    """{kind: {id: resource, or None when only recorded}} in deletion order. A failed describe is
    added to `problems` and that kind's unconfirmed recorded IDs still count as present."""
    out = {}
    for kind, (_, _, field, id_filter, _) in KINDS.items():
        searches = [[{"Name": f"tag:{RUN_TAG}", "Values": [run_id]}]]
        if recorded.get(kind):
            searches.append([{"Name": id_filter, "Values": recorded[kind]}])
        found = {}
        for filters in searches:
            try:
                found.update((r[field], r) for r in describe(ec2, kind, filters))
            except Exception as e:
                log(f"janitor: describe {kind}: {e}")
                problems.append(f"describe {kind} failed: {e}")
        found.update((rid, None) for k, rid in unconfirmed if k == kind and rid not in found)
        if found:
            out[kind] = found
    return out


def busy(kind, r):
    """Waiting on something else: an instance shutting down, an ENI or volume still attached."""
    if r is None:
        return False
    if kind == "instance":
        return r["State"]["Name"] == "shutting-down"
    return r.get("Status", r.get("State")) == "in-use"


def delete(ec2, kind, rid, r):
    if kind == "instance":
        ec2.terminate_instances(InstanceIds=[rid])
        return
    if kind == "address" and (r or {}).get("AssociationId"):
        ec2.disassociate_address(AssociationId=r["AssociationId"])
    if kind == "route-table":
        for a in (r or {}).get("Associations", []):
            if not a.get("Main"):
                ec2.disassociate_route_table(AssociationId=a["RouteTableAssociationId"])
    if kind == "internet-gateway":
        for a in (r or {}).get("Attachments", []):
            ec2.detach_internet_gateway(InternetGatewayId=rid, VpcId=a["VpcId"])
    _, _, field, _, op = KINDS[kind]
    getattr(ec2, op)(**{field: rid})


def clean_run(ec2, run_id, recorded=None, deadline_s=900, poll_s=10, log=print, sleep=time.sleep,
              clock=time.monotonic):
    """Deletes run `run_id`'s resources (found by tag, plus `recorded`: {kind: [id]}). Returns the
    problems: what the last look still found, every failed describe, and every recorded ID that
    answered not-found without any describe having shown it (possibly not visible yet, so not
    proof of deletion; one seen and then gone was deleted, e.g. a data ENI with its instance). [] means verified absent;
    anything else means rerun `cleanup --run-id`."""
    recorded = recorded or {}
    unconfirmed = {(kind, rid) for kind, ids in recorded.items() for rid in ids}
    problems, seen = [], set()
    end = clock() + deadline_s
    while True:
        found = present(ec2, run_id, recorded, unconfirmed, problems, log)
        seen |= {(kind, rid) for kind, items in found.items() for rid, r in items.items() if r is not None}
        if not found or clock() >= end:
            return [f"{kind} {rid}" for kind, items in found.items() for rid in items] + problems
        for kind, items in found.items():
            for rid, r in items.items():
                if busy(kind, r):
                    continue
                try:
                    delete(ec2, kind, rid, r)
                    log(f"janitor: {KINDS[kind][4]} {rid}")
                except Exception as e:  # still present next round; retried until the deadline
                    if not not_found(e):
                        log(f"janitor: {kind} {rid}: {e}")
                        continue
                    if (kind, rid) not in seen:
                        problems.append(f"{kind} {rid}: recorded but never found")
                unconfirmed.discard((kind, rid))
        sleep(poll_s)
