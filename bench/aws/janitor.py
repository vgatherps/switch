"""Deletes what the bench created in AWS, found by tag; used by the runner's teardown and by
`zhida-aws-bench cleanup`.

Run resources carry RUN_TAG=<run id> (and EXPIRES_TAG): the instances, their root volumes and
primary ENIs, and each data ENI. Volumes and ENIs are DeleteOnTermination, so they normally go with
their instance; a data ENI a half-failed launch left unattached is deleted here. clean_run
terminates, deletes whatever is deletable, and describes again (paginated) until nothing of the run
is left or the deadline passes; it returns what remains, so empty means verified absent.

Shared resources carry SHARED_TAG=shared or the fixed names below; clean_shared deletes them,
children before parents, and refuses while any run resource exists.
"""
import os
import time

NAME = "zhida-bench"
RUN_TAG = "zhida-bench-run"
EXPIRES_TAG = "zhida-bench-expires"
SHARED_TAG = "zhida-bench"
NOT_FOUND = {"InvalidPlacementGroup.Unknown"}


def tags(resource):
    return {t["Key"]: t["Value"] for t in resource.get("Tags", [])}


def gone(e):
    return getattr(e, "response", {}).get("Error", {}).get("Code") in NOT_FOUND


def pages(client, op, key, **kw):
    for page in client.get_paginator(op).paginate(**kw):
        yield from page[key]


def run_resources(ec2, filters):
    """Every run resource still present: instances not yet terminated, ENIs, volumes."""
    return {
        "instance": [i for r in pages(ec2, "describe_instances", "Reservations", Filters=filters)
                     for i in r["Instances"] if i["State"]["Name"] != "terminated"],
        "network-interface": list(pages(ec2, "describe_network_interfaces", "NetworkInterfaces", Filters=filters)),
        "volume": list(pages(ec2, "describe_volumes", "Volumes", Filters=filters)),
    }


def listing(found):
    ids = {"instance": ("InstanceId", lambda r: r["State"]["Name"]),
           "network-interface": ("NetworkInterfaceId", lambda r: r["Status"]),
           "volume": ("VolumeId", lambda r: r["State"])}
    return [f"{kind} {r[ids[kind][0]]} ({ids[kind][1](r)}, run {tags(r).get(RUN_TAG)})"
            for kind, items in found.items() for r in items]


def clean_run(ec2, run_id, deadline_s=900, poll_s=10, log=print, sleep=time.sleep, clock=time.monotonic):
    """Deletes run `run_id`'s resources; returns what a final describe still found."""
    filters = [{"Name": f"tag:{RUN_TAG}", "Values": [run_id]}]
    end = clock() + deadline_s
    while True:
        found = run_resources(ec2, filters)
        remaining = listing(found)
        if not remaining or clock() >= end:
            return remaining
        live = [i["InstanceId"] for i in found["instance"] if i["State"]["Name"] != "shutting-down"]
        if live:
            log(f"janitor: terminating {', '.join(live)}")
            ec2.terminate_instances(InstanceIds=live)
        deletable = [(n["NetworkInterfaceId"], ec2.delete_network_interface, {"NetworkInterfaceId": n["NetworkInterfaceId"]})
                     for n in found["network-interface"] if n["Status"] == "available"]
        deletable += [(v["VolumeId"], ec2.delete_volume, {"VolumeId": v["VolumeId"]})
                      for v in found["volume"] if v["State"] == "available"]
        for what, call, kw in deletable:
            log(f"janitor: deleting {what}")
            try:
                call(**kw)
            except Exception as e:  # still listed next round; retried until the deadline
                log(f"janitor: {what}: {e}")
        sleep(poll_s)


def clean_shared(ec2, key_path, log=print):
    """Deletes the shared resources, children before parents; returns what a final check still
    found. Refuses (returns the run resources) while any run resource exists."""
    runs = listing(run_resources(ec2, [{"Name": "tag-key", "Values": [RUN_TAG]}]))
    if runs:
        log("cleanup --shared: run resources still exist; `cleanup --run-id ID` them first")
        return runs

    def step(what, call, **kw):
        log(f"cleanup --shared: {what}")
        try:
            call(**kw)
        except Exception as e:
            if not gone(e):
                raise

    step(f"delete key pair {NAME}", ec2.delete_key_pair, KeyName=NAME)
    if os.path.exists(key_path):
        os.remove(key_path)

    step(f"delete placement group {NAME}", ec2.delete_placement_group, GroupName=NAME)

    shared = [{"Name": f"tag:{SHARED_TAG}", "Values": ["shared"]}]
    vpcs = [v["VpcId"] for v in pages(ec2, "describe_vpcs", "Vpcs", Filters=shared)]
    for vpc in vpcs:
        in_vpc = [{"Name": "vpc-id", "Values": [vpc]}]
        for sg in list(pages(ec2, "describe_security_groups", "SecurityGroups", Filters=in_vpc)):
            if sg["GroupName"] != "default":
                step(f"delete security group {sg['GroupId']}", ec2.delete_security_group, GroupId=sg["GroupId"])
        for rt in list(pages(ec2, "describe_route_tables", "RouteTables", Filters=in_vpc)):
            if any(a.get("Main") for a in rt.get("Associations", [])):
                continue  # goes with the VPC
            for a in rt.get("Associations", []):
                step(f"disassociate {a['RouteTableAssociationId']}", ec2.disassociate_route_table,
                     AssociationId=a["RouteTableAssociationId"])
            step(f"delete route table {rt['RouteTableId']}", ec2.delete_route_table, RouteTableId=rt["RouteTableId"])
        for igw in list(pages(ec2, "describe_internet_gateways", "InternetGateways",
                              Filters=[{"Name": "attachment.vpc-id", "Values": [vpc]}])):
            step(f"detach {igw['InternetGatewayId']}", ec2.detach_internet_gateway,
                 InternetGatewayId=igw["InternetGatewayId"], VpcId=vpc)
        for sn in list(pages(ec2, "describe_subnets", "Subnets", Filters=in_vpc)):
            step(f"delete subnet {sn['SubnetId']}", ec2.delete_subnet, SubnetId=sn["SubnetId"])
    for igw in list(pages(ec2, "describe_internet_gateways", "InternetGateways", Filters=shared)):
        step(f"delete internet gateway {igw['InternetGatewayId']}", ec2.delete_internet_gateway,
             InternetGatewayId=igw["InternetGatewayId"])
    for vpc in vpcs:
        step(f"delete vpc {vpc}", ec2.delete_vpc, VpcId=vpc)
    return shared_remaining(ec2, key_path)


def shared_remaining(ec2, key_path):
    """What of the shared resources a fresh look still finds."""
    shared = [{"Name": f"tag:{SHARED_TAG}", "Values": ["shared"]}]
    out = [f"vpc {v['VpcId']}" for v in pages(ec2, "describe_vpcs", "Vpcs", Filters=shared)]
    out += [f"internet gateway {g['InternetGatewayId']}"
            for g in pages(ec2, "describe_internet_gateways", "InternetGateways", Filters=shared)]
    out += [f"key pair {k['KeyName']}" for k in
            ec2.describe_key_pairs(Filters=[{"Name": "key-name", "Values": [NAME]}])["KeyPairs"]]
    out += [f"placement group {g['GroupName']}" for g in
            ec2.describe_placement_groups(Filters=[{"Name": "group-name", "Values": [NAME]}])["PlacementGroups"]]
    if os.path.exists(key_path):
        out.append(f"file {key_path}")
    return out
