#!/usr/bin/env python3
"""Tests of janitor.py and `cleanup` against fake EC2/S3/IAM (one item per describe page, so a
describe that skips pagination misses resources): python3 -m unittest discover -s bench/aws"""
import contextlib
import functools
import io
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import janitor  # noqa: E402
import zhida_aws_bench as z  # noqa: E402


class ClientError(Exception):
    def __init__(self, code, message=""):
        super().__init__(f"An error occurred ({code}): {message}")
        self.response = {"Error": {"Code": code, "Message": message}}


def run_tag(run):
    return [{"Key": janitor.RUN_TAG, "Value": run}, {"Key": janitor.EXPIRES_TAG, "Value": "2026-11-01T12:00:00+00:00"}]


SHARED = [{"Key": janitor.SHARED_TAG, "Value": "shared"}]
PAGED = {"describe_instances": "Reservations", "describe_network_interfaces": "NetworkInterfaces",
         "describe_volumes": "Volumes", "describe_vpcs": "Vpcs", "describe_subnets": "Subnets",
         "describe_security_groups": "SecurityGroups", "describe_route_tables": "RouteTables",
         "describe_internet_gateways": "InternetGateways"}


def matches(r, filters):
    t = janitor.tags(r)
    for f in filters:
        n, vals = f["Name"], f["Values"]
        if n == "tag-key" and not any(k in t for k in vals):
            return False
        if n.startswith("tag:") and t.get(n[4:]) not in vals:
            return False
        if n == "vpc-id" and r.get("VpcId") not in vals:
            return False
        if n == "attachment.vpc-id" and not any(a["VpcId"] in vals for a in r.get("Attachments", [])):
            return False
    return True


class FakeEc2:
    """Instances: running -> (terminate) shutting-down -> terminated one describe later, taking
    their DeleteOnTermination ENIs and volumes with them. An ENI or volume with _busy=N stays in-use
    for N more describes. Deletes enforce the dependencies EC2 enforces."""

    def __init__(self):
        self.instances, self.enis, self.volumes = {}, {}, {}
        self.vpcs, self.subnets, self.sgs, self.rts, self.igws = {}, {}, {}, {}, {}
        self.keys, self.pgs = {"zhida-bench"}, {"zhida-bench"}
        self.calls, self.fail = [], {}   # fail: call name -> ClientError raised every time

    # ---- describes: paginated only, one item per page
    def get_paginator(self, op):
        ec2 = self

        class P:
            def paginate(self, Filters=()):
                for item in ec2.items(op, Filters):
                    if op == "describe_instances":
                        yield {"Reservations": [{"Instances": [item]}]}
                    else:
                        yield {PAGED[op]: [item]}
        return P()

    def items(self, op, filters):
        if op == "describe_instances":
            for i in self.instances.values():
                if i["State"]["Name"] == "shutting-down" and i.pop("_seen", False):
                    i["State"]["Name"] = "terminated"
                    for store in (self.enis, self.volumes):
                        for k in [k for k, v in store.items() if v.get("_owner") == i["InstanceId"]]:
                            del store[k]
                elif i["State"]["Name"] == "shutting-down":
                    i["_seen"] = True
            store = self.instances
        else:
            store = {"describe_network_interfaces": self.enis, "describe_volumes": self.volumes,
                     "describe_vpcs": self.vpcs, "describe_subnets": self.subnets,
                     "describe_security_groups": self.sgs, "describe_route_tables": self.rts,
                     "describe_internet_gateways": self.igws}[op]
        for r in store.values():
            if r.get("_busy"):
                r["_busy"] -= 1
                if not r["_busy"]:
                    r["Status" if "Status" in r else "State"] = "available"
        return [{k: v for k, v in r.items() if not k.startswith("_")} for r in list(store.values())
                if matches(r, filters)]

    def _call(self, name, **kw):
        self.calls.append((name, kw))
        if name in self.fail:
            raise self.fail[name]

    # ---- run resources
    def terminate_instances(self, InstanceIds):
        self._call("terminate_instances", InstanceIds=InstanceIds)
        for i in InstanceIds:
            self.instances[i]["State"]["Name"] = "shutting-down"

    def delete_network_interface(self, NetworkInterfaceId):
        self._call("delete_network_interface", NetworkInterfaceId=NetworkInterfaceId)
        if self.enis[NetworkInterfaceId]["Status"] != "available":
            raise ClientError("InvalidNetworkInterface.InUse")
        del self.enis[NetworkInterfaceId]

    def delete_volume(self, VolumeId):
        self._call("delete_volume", VolumeId=VolumeId)
        if self.volumes[VolumeId]["State"] != "available":
            raise ClientError("VolumeInUse")
        del self.volumes[VolumeId]

    # ---- shared
    def describe_key_pairs(self, Filters):
        return {"KeyPairs": [{"KeyName": k} for k in self.keys]}

    def describe_placement_groups(self, Filters):
        return {"PlacementGroups": [{"GroupName": g} for g in self.pgs]}

    def delete_key_pair(self, KeyName):
        self._call("delete_key_pair", KeyName=KeyName)
        self.keys.discard(KeyName)  # EC2 succeeds on a missing key pair

    def delete_placement_group(self, GroupName):
        self._call("delete_placement_group", GroupName=GroupName)
        if GroupName not in self.pgs:
            raise ClientError("InvalidPlacementGroup.Unknown")
        self.pgs.remove(GroupName)

    def delete_security_group(self, GroupId):
        self._call("delete_security_group", GroupId=GroupId)
        assert not self.enis, "SG deleted while ENIs exist"
        del self.sgs[GroupId]

    def disassociate_route_table(self, AssociationId):
        self._call("disassociate_route_table", AssociationId=AssociationId)
        for rt in self.rts.values():
            rt["Associations"] = [a for a in rt["Associations"] if a["RouteTableAssociationId"] != AssociationId]

    def delete_route_table(self, RouteTableId):
        self._call("delete_route_table", RouteTableId=RouteTableId)
        assert not self.rts[RouteTableId]["Associations"], "route table deleted while associated"
        del self.rts[RouteTableId]

    def detach_internet_gateway(self, InternetGatewayId, VpcId):
        self._call("detach_internet_gateway", InternetGatewayId=InternetGatewayId, VpcId=VpcId)
        self.igws[InternetGatewayId]["Attachments"] = []

    def delete_internet_gateway(self, InternetGatewayId):
        self._call("delete_internet_gateway", InternetGatewayId=InternetGatewayId)
        assert not self.igws[InternetGatewayId]["Attachments"], "IGW deleted while attached"
        del self.igws[InternetGatewayId]

    def delete_subnet(self, SubnetId):
        self._call("delete_subnet", SubnetId=SubnetId)
        del self.subnets[SubnetId]

    def delete_vpc(self, VpcId):
        self._call("delete_vpc", VpcId=VpcId)
        assert not any(s["VpcId"] == VpcId for s in self.subnets.values()), "VPC deleted with subnets"
        assert not any(g["VpcId"] == VpcId and g["GroupName"] != "default" for g in self.sgs.values()), "with SGs"
        main = lambda r: any(a.get("Main") for a in r["Associations"])  # noqa: E731
        assert not any(r["VpcId"] == VpcId and not main(r) for r in self.rts.values()), "with route tables"
        assert not any(a["VpcId"] == VpcId for g in self.igws.values() for a in g["Attachments"]), "with IGW"
        del self.vpcs[VpcId]
        self.sgs = {k: g for k, g in self.sgs.items() if g["VpcId"] != VpcId}
        self.rts = {k: r for k, r in self.rts.items() if r["VpcId"] != VpcId}


class FakeS3:
    def __init__(self, versions=0):
        self.objects = {(f"k{i}", f"v{i}") for i in range(versions)}
        self.bucket, self.calls = True, []

    def get_paginator(self, op):
        s3 = self

        class P:
            def paginate(self, Bucket):
                if not s3.bucket:
                    raise ClientError("NoSuchBucket")
                objs = sorted(s3.objects)
                half = len(objs) // 2
                for chunk in (objs[:half], objs[half:]):
                    yield {"Versions": [{"Key": k, "VersionId": v} for k, v in chunk if k != "marker"],
                           "DeleteMarkers": [{"Key": k, "VersionId": v} for k, v in chunk if k == "marker"]}
        return P()

    def delete_objects(self, Bucket, Delete):
        self.calls.append(("delete_objects", len(Delete["Objects"])))
        for o in Delete["Objects"]:
            self.objects.discard((o["Key"], o["VersionId"]))

    def delete_bucket(self, Bucket):
        self.calls.append(("delete_bucket", Bucket))
        if not self.bucket:
            raise ClientError("NoSuchBucket")
        assert not self.objects, "bucket deleted while not empty"
        self.bucket = False

    def head_bucket(self, Bucket):
        if not self.bucket:
            raise ClientError("404")


class FakeIam:
    """One role in one instance profile, one inline policy; deletes of missing things: NoSuchEntity."""

    def __init__(self):
        self.profile, self.role, self.policies, self.calls = ["zhida-bench-host"], True, ["zhida-bench-host"], []

    def _need(self, ok):
        if not ok:
            raise ClientError("NoSuchEntity")

    def get_instance_profile(self, InstanceProfileName):
        self._need(self.profile is not None)

    def remove_role_from_instance_profile(self, InstanceProfileName, RoleName):
        self.calls.append("remove_role_from_instance_profile")
        self._need(self.profile and RoleName in self.profile)
        self.profile.remove(RoleName)

    def delete_instance_profile(self, InstanceProfileName):
        self.calls.append("delete_instance_profile")
        self._need(self.profile is not None)
        assert not self.profile, "profile deleted with a role in it"
        self.profile = None

    def delete_role_policy(self, RoleName, PolicyName):
        self.calls.append("delete_role_policy")
        self._need(PolicyName in self.policies)
        self.policies.remove(PolicyName)

    def delete_role(self, RoleName):
        self.calls.append("delete_role")
        self._need(self.role)
        assert not self.policies and not self.profile, "role deleted with policies or in a profile"
        self.role = False

    def get_role(self, RoleName):
        self._need(self.role)


def run_world():
    ec2 = FakeEc2()
    for iid, run in [("i-a", "r1"), ("i-b", "r1"), ("i-c", "r2")]:
        ec2.instances[iid] = {"InstanceId": iid, "State": {"Name": "running"}, "Tags": run_tag(run)}
        ec2.enis[f"eni-{iid}"] = {"NetworkInterfaceId": f"eni-{iid}", "Status": "in-use", "Tags": run_tag(run),
                                  "_owner": iid}
        ec2.volumes[f"vol-{iid}"] = {"VolumeId": f"vol-{iid}", "State": "in-use", "Tags": run_tag(run), "_owner": iid}
    ec2.instances["i-other"] = {"InstanceId": "i-other", "State": {"Name": "running"}, "Tags": []}
    ec2.enis["eni-loose"] = {"NetworkInterfaceId": "eni-loose", "Status": "available", "Tags": run_tag("r1")}
    return ec2


def clean(ec2, run="r1", deadline_s=600):
    clock = iter(range(0, 100000, 10))
    return janitor.clean_run(ec2, run, deadline_s=deadline_s, poll_s=10, log=lambda m: None, sleep=lambda s: None,
                             clock=lambda: next(clock))


class CleanRunTest(unittest.TestCase):
    def test_run_cleanup_deletes_everything_of_that_run_and_verifies(self):
        ec2 = run_world()
        self.assertEqual(clean(ec2), [])
        self.assertEqual([c for c in ec2.calls if c[0] == "terminate_instances"],
                         [("terminate_instances", {"InstanceIds": ["i-a", "i-b"]})])
        self.assertEqual(sorted(ec2.enis), ["eni-i-c"])
        self.assertEqual(sorted(ec2.volumes), ["vol-i-c"])
        self.assertEqual(ec2.instances["i-c"]["State"]["Name"], "running", "another run touched")
        self.assertEqual(ec2.instances["i-other"]["State"]["Name"], "running", "untagged instance touched")

    def test_resources_on_later_pages_are_deleted(self):
        ec2 = FakeEc2()
        for k in range(5):
            ec2.enis[f"eni-{k}"] = {"NetworkInterfaceId": f"eni-{k}", "Status": "available", "Tags": run_tag("r1")}
            ec2.volumes[f"vol-{k}"] = {"VolumeId": f"vol-{k}", "State": "available", "Tags": run_tag("r1")}
        self.assertEqual(clean(ec2), [])
        self.assertEqual((ec2.enis, ec2.volumes), ({}, {}))

    def test_in_use_eni_is_retried_until_it_frees(self):
        ec2 = run_world()
        ec2.enis["eni-detaching"] = {"NetworkInterfaceId": "eni-detaching", "Status": "in-use", "Tags": run_tag("r1"),
                                     "_busy": 4}
        ec2.volumes["vol-detaching"] = {"VolumeId": "vol-detaching", "State": "in-use", "Tags": run_tag("r1"),
                                        "_busy": 3}
        self.assertEqual(clean(ec2), [])
        self.assertNotIn("eni-detaching", ec2.enis)
        self.assertNotIn("vol-detaching", ec2.volumes)

    def test_in_use_eni_that_never_frees_is_reported_not_declared_done(self):
        ec2 = run_world()
        ec2.enis["eni-stuck"] = {"NetworkInterfaceId": "eni-stuck", "Status": "in-use", "Tags": run_tag("r1")}
        self.assertEqual(clean(ec2, deadline_s=60), ["network-interface eni-stuck (in-use, run r1)"])
        self.assertEqual(ec2.instances["i-a"]["State"]["Name"], "terminated")

    def test_a_failing_delete_does_not_stop_the_rest(self):
        ec2 = run_world()
        ec2.volumes["vol-loose"] = {"VolumeId": "vol-loose", "State": "available", "Tags": run_tag("r1")}
        ec2.fail["delete_network_interface"] = ClientError("UnauthorizedOperation", "not allowed")
        self.assertEqual(clean(ec2, deadline_s=60), ["network-interface eni-loose (available, run r1)"])
        self.assertNotIn("vol-loose", ec2.volumes)
        self.assertEqual(ec2.instances["i-a"]["State"]["Name"], "terminated")

    def test_second_cleanup_is_a_no_op(self):
        ec2 = run_world()
        clean(ec2)
        ec2.calls.clear()
        self.assertEqual(clean(ec2), [])
        self.assertEqual(ec2.calls, [])


def shared_world(tmp):
    ec2 = FakeEc2()
    ec2.vpcs["vpc-1"] = {"VpcId": "vpc-1", "Tags": SHARED}
    ec2.subnets["subnet-1"] = {"SubnetId": "subnet-1", "VpcId": "vpc-1", "Tags": SHARED}
    ec2.sgs["sg-default"] = {"GroupId": "sg-default", "GroupName": "default", "VpcId": "vpc-1"}
    ec2.sgs["sg-1"] = {"GroupId": "sg-1", "GroupName": "zhida-bench", "VpcId": "vpc-1", "Tags": SHARED}
    ec2.rts["rtb-main"] = {"RouteTableId": "rtb-main", "VpcId": "vpc-1",
                           "Associations": [{"RouteTableAssociationId": "rtbassoc-main", "Main": True}]}
    ec2.rts["rtb-1"] = {"RouteTableId": "rtb-1", "VpcId": "vpc-1", "Tags": SHARED,
                        "Associations": [{"RouteTableAssociationId": "rtbassoc-1", "SubnetId": "subnet-1",
                                          "Main": False}]}
    ec2.igws["igw-1"] = {"InternetGatewayId": "igw-1", "Attachments": [{"VpcId": "vpc-1"}], "Tags": SHARED}
    pem = os.path.join(tmp, "zhida-bench-ap-northeast-1.pem")
    with open(pem, "w") as f:
        f.write("key")
    return ec2, FakeS3(versions=5), FakeIam(), pem


class CleanSharedTest(unittest.TestCase):
    def test_deletes_everything_in_order_and_is_idempotent(self):
        ec2, s3, iam, pem = shared_world(tempfile.mkdtemp())
        s3.objects.add(("marker", "dm1"))
        self.assertEqual(janitor.clean_shared(ec2, s3, iam, "bucket", pem, log=lambda m: None), [])
        names = [c[0] for c in ec2.calls]
        self.assertLess(names.index("disassociate_route_table"), names.index("delete_route_table"))
        self.assertLess(names.index("detach_internet_gateway"), names.index("delete_internet_gateway"))
        for child in ("delete_security_group", "delete_route_table", "delete_subnet", "delete_internet_gateway"):
            self.assertLess(names.index(child), names.index("delete_vpc"), child)
        self.assertNotIn({"GroupId": "sg-default"}, [kw for n, kw in ec2.calls if n == "delete_security_group"])
        self.assertEqual(s3.calls, [("delete_objects", 3), ("delete_objects", 3), ("delete_bucket", "bucket")])
        self.assertEqual(iam.calls, ["remove_role_from_instance_profile", "delete_instance_profile",
                                     "delete_role_policy", "delete_role"])
        self.assertFalse(os.path.exists(pem))
        self.assertEqual((ec2.vpcs, ec2.igws, ec2.keys, ec2.pgs), ({}, {}, set(), set()))
        # again: everything already gone, nothing raised, nothing left
        self.assertEqual(janitor.clean_shared(ec2, s3, iam, "bucket", pem, log=lambda m: None), [])

    def test_refuses_while_run_resources_exist(self):
        ec2, s3, iam, pem = shared_world(tempfile.mkdtemp())
        ec2.enis["eni-r9"] = {"NetworkInterfaceId": "eni-r9", "Status": "available", "Tags": run_tag("r9")}
        self.assertEqual(janitor.clean_shared(ec2, s3, iam, "bucket", pem, log=lambda m: None),
                         ["network-interface eni-r9 (available, run r9)"])
        self.assertEqual((ec2.calls, iam.calls, s3.calls), ([], [], []))
        self.assertTrue(os.path.exists(pem))


class FakeSts:
    def get_caller_identity(self):
        return {"Account": "123456789012"}


class CleanupCommandTest(unittest.TestCase):
    def main(self, argv, ec2, s3=None, iam=None):
        aws = z.Aws("ap-northeast-1", {"ec2": ec2, "s3": s3, "iam": iam, "ssm": None, "sts": FakeSts(),
                                       "service-quotas": None})
        fast = functools.partial(janitor.clean_run, deadline_s=30, poll_s=10, sleep=lambda s: None,
                                 clock=iter(range(0, 100000, 10)).__next__)
        out = io.StringIO()
        with mock.patch.object(z.janitor, "clean_run", fast), contextlib.redirect_stdout(out):
            code = z.main(argv, z.Env(aws=lambda region: aws))
        return code, out.getvalue()

    def test_run_id_exit_status_follows_what_remains(self):
        code, out = self.main(["cleanup", "--run-id", "r1"], run_world())
        self.assertEqual(code, 0, out)
        self.assertIn("run r1: nothing left", out)
        ec2 = run_world()
        ec2.enis["eni-stuck"] = {"NetworkInterfaceId": "eni-stuck", "Status": "in-use", "Tags": run_tag("r1")}
        code, out = self.main(["cleanup", "--run-id", "r1"], ec2)
        self.assertEqual(code, 1)
        self.assertIn("STILL PRESENT after teardown", out)
        self.assertIn("eni-stuck", out)

    def test_shared_uses_the_runner_names(self):
        tmp = tempfile.mkdtemp()
        ec2, s3, iam, _ = shared_world(tmp)
        pem = os.path.join(tmp, "zhida-bench-ap-northeast-1.pem")
        with mock.patch.object(z, "KEY_DIR", tmp), mock.patch.object(s3, "delete_bucket",
                                                                      wraps=s3.delete_bucket) as delete_bucket:
            code, out = self.main(["cleanup", "--shared"], ec2, s3, iam)
        self.assertEqual(code, 0, out)
        delete_bucket.assert_called_once_with(Bucket="zhida-bench-cache-123456789012-ap-northeast-1")
        self.assertFalse(os.path.exists(pem))
        self.assertIn("cleanup --shared: nothing left", out)

    def test_shared_refusal_exits_non_zero(self):
        ec2, s3, iam, _ = shared_world(tempfile.mkdtemp())
        ec2.instances["i-r9"] = {"InstanceId": "i-r9", "State": {"Name": "running"}, "Tags": run_tag("r9")}
        code, out = self.main(["cleanup", "--shared"], ec2, s3, iam)
        self.assertEqual(code, 1)
        self.assertIn("STILL PRESENT: instance i-r9 (running, run r9)", out)


if __name__ == "__main__":
    unittest.main()
