#!/usr/bin/env python3
"""Tests of janitor.py and `cleanup` against a fake EC2 (one item per describe page, so a describe
that skips pagination misses resources): python3 -m unittest discover -s bench/aws"""
import contextlib
import functools
import io
import os
import signal
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


KIND_OF_OP = {v[0]: kind for kind, v in janitor.KINDS.items()}


class FakeEc2:
    """Per-kind stores. Instances: running -> (terminate) shutting-down -> terminated one describe
    later, taking the ENIs and volumes they own (DeleteOnTermination) with them. An ENI or volume
    with _busy=N stays in-use for N more describes. IDs in `hidden` are invisible to describes.
    Deletes enforce the dependencies EC2 enforces; `deleted` lists successful deletes in order."""

    def __init__(self):
        self.store = {kind: {} for kind in janitor.KINDS}
        self.hidden, self.calls, self.deleted, self.fail = set(), [], [], {}

    def add(self, kind, rid, run="r1", **attrs):
        self.store[kind][rid] = {janitor.KINDS[kind][2]: rid, **attrs, **({"Tags": run_tag(run)} if run else {})}

    # ---- describes
    def _items(self, op, filters):
        kind = KIND_OF_OP[op]
        store = self.store[kind]
        if kind == "instance":
            for i in store.values():
                if i["State"]["Name"] == "shutting-down" and i.pop("_seen", False):
                    i["State"]["Name"] = "terminated"
                    for owned in ("network-interface", "volume"):
                        self.store[owned] = {k: v for k, v in self.store[owned].items()
                                             if v.get("_owner") != i["InstanceId"]}
                elif i["State"]["Name"] == "shutting-down":
                    i["_seen"] = True
        for r in store.values():
            if r.get("_busy"):
                r["_busy"] -= 1
                if not r["_busy"]:
                    r["Status" if "Status" in r else "State"] = "available"
        field, id_filter = janitor.KINDS[kind][2:4]
        out = []
        for rid, r in store.items():
            tags = {t["Key"]: t["Value"] for t in r.get("Tags", [])}
            ok = all((tags.get(f["Name"][4:]) in f["Values"]) if f["Name"].startswith("tag:")
                     else (f["Name"] == id_filter and r[field] in f["Values"]) for f in filters)
            if ok and rid not in self.hidden:
                out.append({k: v for k, v in r.items() if not k.startswith("_")})
        return out

    def get_paginator(self, op):
        ec2 = self

        class P:
            def paginate(self, Filters):
                for item in ec2._items(op, Filters):
                    yield {"Reservations": [{"Instances": [item]}]} if op == "describe_instances" else \
                        {janitor.KINDS[KIND_OF_OP[op]][1]: [item]}
        return P()

    def describe_key_pairs(self, Filters):
        return {"KeyPairs": self._items("describe_key_pairs", Filters)}

    def describe_addresses(self, Filters):
        return {"Addresses": self._items("describe_addresses", Filters)}

    def describe_placement_groups(self, Filters):
        return {"PlacementGroups": self._items("describe_placement_groups", Filters)}

    # ---- deletes
    def _need(self, name, kind, rid, code="InvalidID.NotFound"):
        self.calls.append((name, rid))
        if name in self.fail:
            raise self.fail[name]
        if rid not in self.store[kind]:
            raise ClientError(code)
        return self.store[kind][rid]

    def _delete(self, kind, rid, busy=None):
        if busy:
            raise ClientError("DependencyViolation", busy)
        del self.store[kind][rid]
        self.deleted.append((kind, rid))

    def live_instances(self):
        return [i for i in self.store["instance"].values() if i["State"]["Name"] != "terminated"]

    def terminate_instances(self, InstanceIds):
        for rid in InstanceIds:
            i = self._need("terminate_instances", "instance", rid)
            if i["State"]["Name"] != "terminated":
                i["State"]["Name"] = "shutting-down"

    def delete_network_interface(self, NetworkInterfaceId):
        eni = self._need("delete_network_interface", "network-interface", NetworkInterfaceId)
        if eni["Status"] != "available":
            raise ClientError("InvalidNetworkInterface.InUse")
        self._delete("network-interface", NetworkInterfaceId)

    def delete_volume(self, VolumeId):
        vol = self._need("delete_volume", "volume", VolumeId)
        if vol["State"] != "available":
            raise ClientError("VolumeInUse")
        self._delete("volume", VolumeId)

    def delete_key_pair(self, KeyName):
        self.calls.append(("delete_key_pair", KeyName))
        if self.store["key-pair"].pop(KeyName, None):  # EC2 succeeds on a missing key pair
            self.deleted.append(("key-pair", KeyName))

    def disassociate_address(self, AssociationId):
        self.calls.append(("disassociate_address", AssociationId))
        for a in self.store["address"].values():
            if a.get("AssociationId") == AssociationId:
                del a["AssociationId"]

    def release_address(self, AllocationId):
        a = self._need("release_address", "address", AllocationId, "InvalidAllocationID.NotFound")
        if a.get("AssociationId"):
            raise ClientError("InvalidIPAddress.InUse")
        self._delete("address", AllocationId)

    def delete_placement_group(self, GroupName):
        self._need("delete_placement_group", "placement-group", GroupName, "InvalidPlacementGroup.Unknown")
        if [i for i in self.live_instances() if i["_pg"] == GroupName]:
            raise ClientError("InvalidPlacementGroup.InUse")
        self._delete("placement-group", GroupName)

    def enis_in(self, vpc):
        return [e for e in self.store["network-interface"].values() if e.get("_vpc") == vpc]

    def delete_security_group(self, GroupId):
        sg = self._need("delete_security_group", "security-group", GroupId)
        self._delete("security-group", GroupId, self.enis_in(sg["VpcId"]) and "ENIs use it")

    def delete_subnet(self, SubnetId):
        sn = self._need("delete_subnet", "subnet", SubnetId)
        self._delete("subnet", SubnetId, self.enis_in(sn["VpcId"]) and "ENIs in it")

    def disassociate_route_table(self, AssociationId):
        self.calls.append(("disassociate_route_table", AssociationId))
        for rt in self.store["route-table"].values():
            rt["Associations"] = [a for a in rt["Associations"] if a["RouteTableAssociationId"] != AssociationId]

    def delete_route_table(self, RouteTableId):
        rt = self._need("delete_route_table", "route-table", RouteTableId)
        self._delete("route-table", RouteTableId, rt["Associations"] and "associated")

    def detach_internet_gateway(self, InternetGatewayId, VpcId):
        if any(a.get("AssociationId") for a in self.store["address"].values()):
            raise ClientError("DependencyViolation", "the network has mapped public addresses")
        self._need("detach_internet_gateway", "internet-gateway", InternetGatewayId)["Attachments"] = []

    def delete_internet_gateway(self, InternetGatewayId):
        igw = self._need("delete_internet_gateway", "internet-gateway", InternetGatewayId)
        self._delete("internet-gateway", InternetGatewayId, igw["Attachments"] and "attached")

    def delete_vpc(self, VpcId):
        self._need("delete_vpc", "vpc", VpcId)
        inside = [r for kind in ("subnet", "security-group", "route-table") for r in self.store[kind].values()
                  if r["VpcId"] == VpcId]
        inside += [g for g in self.store["internet-gateway"].values()
                   if any(a["VpcId"] == VpcId for a in g["Attachments"])]
        self._delete("vpc", VpcId, inside and "not empty")


def run_world(ec2=None, run="r1", n=""):
    """What a run creates (ids suffixed `n`), and the IDs the runner recorded for it."""
    ec2 = ec2 or FakeEc2()
    vpc = f"vpc{n}"
    ec2.add("vpc", vpc, run)
    ec2.add("subnet", f"subnet{n}", run, VpcId=vpc)
    ec2.add("internet-gateway", f"igw{n}", run, Attachments=[{"VpcId": vpc}])
    ec2.add("route-table", f"rtb{n}", run, VpcId=vpc,
            Associations=[{"RouteTableAssociationId": f"rtbassoc{n}", "SubnetId": f"subnet{n}", "Main": False}])
    ec2.add("security-group", f"sg{n}", run, VpcId=vpc)
    ec2.add("placement-group", f"zhida-bench-{run}", run)
    ec2.add("key-pair", f"zhida-bench-{run}", run)
    recorded = {"vpc": [vpc], "subnet": [f"subnet{n}"], "internet-gateway": [f"igw{n}"], "route-table": [f"rtb{n}"],
                "security-group": [f"sg{n}"], "placement-group": [f"zhida-bench-{run}"],
                "key-pair": [f"zhida-bench-{run}"], "instance": [], "network-interface": []}
    for k in range(2):
        iid = f"i-{k}{n}"
        ec2.add("instance", iid, run, State={"Name": "running"}, _pg=f"zhida-bench-{run}")
        ec2.add("volume", f"vol-{k}{n}", run, State="in-use", _owner=iid)
        ec2.add("network-interface", f"eni-primary-{k}{n}", run, Status="in-use", _owner=iid, _vpc=vpc)
        ec2.add("network-interface", f"eni-data-{k}{n}", run, Status="in-use", _owner=iid, _vpc=vpc)
        recorded["instance"].append(iid)
        recorded["network-interface"].append(f"eni-data-{k}{n}")
    return ec2, recorded


def clean(ec2, recorded=None, run="r1", deadline_s=600):
    clock = iter(range(0, 100000, 10))
    return janitor.clean_run(ec2, run, recorded, deadline_s=deadline_s, poll_s=10, log=lambda m: None,
                             sleep=lambda s: None, clock=lambda: next(clock))


def left(ec2, run="r1"):
    return sorted(rid for kind, store in ec2.store.items() for rid, r in store.items()
                  if {"Key": janitor.RUN_TAG, "Value": run} in r.get("Tags", []) and
                  not (kind == "instance" and r["State"]["Name"] == "terminated"))


class CleanRunTest(unittest.TestCase):
    def test_deletes_the_run_by_recorded_ids_in_dependency_order(self):
        ec2, recorded = run_world()
        run_world(ec2, run="r2", n="-other")
        # a data ENI a half-failed launch left unattached, whose tags the lookup does not see
        ec2.add("network-interface", "eni-untagged", None, Status="available")
        recorded["network-interface"].append("eni-untagged")
        self.assertEqual(clean(ec2, recorded), [])
        self.assertEqual(left(ec2), [])
        self.assertNotIn("eni-untagged", ec2.store["network-interface"])
        self.assertEqual(len(left(ec2, "r2")), 15, "another run touched")
        order = [kind for kind, _ in ec2.deleted]
        self.assertEqual(set(order), {"network-interface", "key-pair", "placement-group", "security-group", "subnet",
                                      "route-table", "internet-gateway", "vpc"})
        self.assertEqual(order[-1], "vpc")
        last_eni = max(i for i, k in enumerate(order) if k == "network-interface")
        self.assertLess(last_eni, order.index("security-group"))
        self.assertLess(last_eni, order.index("subnet"))
        names = [c[0] for c in ec2.calls]
        self.assertLess(names.index("disassociate_route_table"), names.index("delete_route_table"))
        self.assertLess(names.index("detach_internet_gateway"), names.index("delete_internet_gateway"))

    def test_a_recorded_id_no_describe_shows_yet_is_still_deleted(self):
        ec2, recorded = run_world()
        ec2.add("network-interface", "eni-fresh", Status="available")
        ec2.hidden.add("eni-fresh")
        recorded["network-interface"].append("eni-fresh")
        self.assertEqual(clean(ec2, recorded), [])
        self.assertIn(("network-interface", "eni-fresh"), ec2.deleted)

    def test_tag_lookup_alone_cleans_a_leftover_run(self):
        ec2, _ = run_world()
        self.assertEqual(clean(ec2, {}), [])
        self.assertEqual(left(ec2), [])
        self.assertEqual(ec2.store["vpc"], {})

    def test_resources_on_later_pages_are_deleted(self):
        ec2 = FakeEc2()
        for k in range(5):
            ec2.add("network-interface", f"eni-{k}", Status="available")
            ec2.add("volume", f"vol-{k}", State="available")
        self.assertEqual(clean(ec2), [])
        self.assertEqual((ec2.store["network-interface"], ec2.store["volume"]), ({}, {}))

    def test_in_use_eni_is_retried_until_it_frees(self):
        ec2, recorded = run_world()
        ec2.add("network-interface", "eni-detaching", Status="in-use", _busy=4)
        ec2.add("volume", "vol-detaching", State="in-use", _busy=3)
        self.assertEqual(clean(ec2, recorded), [])
        self.assertNotIn("eni-detaching", ec2.store["network-interface"])
        self.assertNotIn("vol-detaching", ec2.store["volume"])

    def test_in_use_eni_that_never_frees_is_listed_as_remaining(self):
        ec2, recorded = run_world()
        ec2.add("network-interface", "eni-stuck", Status="in-use", _vpc="vpc")
        remaining = clean(ec2, recorded, deadline_s=120)
        self.assertEqual(remaining, ["network-interface eni-stuck", "security-group sg", "subnet subnet", "vpc vpc"])
        self.assertEqual(ec2.store["instance"]["i-0"]["State"]["Name"], "terminated")

    def test_a_failed_describe_does_not_stop_deleting_and_is_reported(self):
        """Regression: one describe error used to abort teardown before any delete."""
        ec2, recorded = run_world()

        failures = {"n": 3}
        paginator = ec2.get_paginator

        def get_paginator(op):
            if op == "describe_instances" and failures["n"]:
                failures["n"] -= 1
                raise ClientError("RequestLimitExceeded", "slow down")
            return paginator(op)
        ec2.get_paginator = get_paginator
        problems = clean(ec2, recorded, deadline_s=600)
        self.assertEqual([i["State"]["Name"] for i in ec2.store["instance"].values()], ["terminated"] * 2)
        self.assertNotIn("vpc", [k for k, v in ec2.store.items() if v and k != "instance"])
        self.assertTrue(problems and all(p.startswith("describe instance failed") for p in problems), problems)

    def test_a_recorded_id_never_seen_that_answers_not_found_is_not_verified_gone(self):
        """Regression: a recorded VPC hidden from describes and answering not-found was taken as
        deleted, and the run reported nothing left while it still existed."""
        ec2, recorded = run_world()
        ec2.hidden.add("vpc")
        ec2.fail["delete_vpc"] = ClientError("InvalidVpcID.NotFound")
        problems = clean(ec2, recorded, deadline_s=600)
        self.assertEqual(problems, ["vpc vpc: recorded but never found"])

    def test_a_failing_delete_does_not_stop_the_rest(self):
        ec2, recorded = run_world()
        ec2.add("network-interface", "eni-loose", Status="available")
        ec2.add("volume", "vol-loose", State="available")
        ec2.fail["delete_network_interface"] = ClientError("UnauthorizedOperation", "not allowed")
        remaining = clean(ec2, recorded, deadline_s=120)
        self.assertIn("network-interface eni-loose", remaining)
        self.assertNotIn("vol-loose", ec2.store["volume"])
        self.assertIn(("key-pair", "zhida-bench-r1"), ec2.deleted)
        self.assertEqual(ec2.store["instance"]["i-0"]["State"]["Name"], "terminated")

    def test_an_elastic_ip_on_a_data_eni_is_disassociated_then_released(self):
        for recorded_address in (True, False):  # the runner's record, and the tag lookup alone
            ec2, recorded = run_world()
            ec2.add("address", "eipalloc-1", AssociationId="eipassoc-1", NetworkInterfaceId="eni-data-0")
            if recorded_address:
                recorded["address"] = ["eipalloc-1"]
            self.assertEqual(clean(ec2, recorded if recorded_address else {}), [])
            self.assertEqual(left(ec2), [])
            self.assertIn(("address", "eipalloc-1"), ec2.deleted)
            names = [c[0] for c in ec2.calls]
            self.assertLess(names.index("disassociate_address"), names.index("release_address"))
            self.assertLess(names.index("release_address"), names.index("detach_internet_gateway"))
            self.assertEqual(ec2.store["vpc"], {})

    def test_second_cleanup_is_a_no_op(self):
        ec2, recorded = run_world()
        clean(ec2, recorded)
        ec2.calls.clear()
        self.assertEqual(clean(ec2, {}), [])
        self.assertEqual(ec2.calls, [])


class CleanupCommandTest(unittest.TestCase):
    def main(self, argv, ec2):
        aws = z.Aws("ap-northeast-1", {"ec2": ec2, "ssm": None, "sts": None, "service-quotas": None})
        fast = functools.partial(janitor.clean_run, deadline_s=120, poll_s=10, sleep=lambda s: None,
                                 clock=iter(range(0, 100000, 10)).__next__)
        out = io.StringIO()
        with mock.patch.object(z.janitor, "clean_run", fast), contextlib.redirect_stdout(out):
            code = z.main(argv, z.Env(profile_region=lambda: "ap-southeast-2", aws=lambda region: aws))
        return code, out.getvalue()

    def recorded_run(self, recorded):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        os.makedirs(os.path.join(directory.name, "r1"))
        created = janitor.Created(os.path.join(directory.name, "r1", "created.json"))
        for kind, ids in recorded.items():
            for rid in ids:
                created.add(kind, rid)
        return directory.name, created

    def test_run_id_uses_the_recorded_ids_and_the_tags(self):
        out_dir = tempfile.mkdtemp()
        os.makedirs(os.path.join(out_dir, "r1"))
        created = janitor.Created(os.path.join(out_dir, "r1", "created.json"))
        ec2, recorded = run_world()
        ec2.add("network-interface", "eni-untagged", None, Status="available")
        for kind, ids in recorded.items():
            for rid in ids:
                created.add(kind, rid)
        created.add("network-interface", "eni-untagged")
        self.assertEqual(janitor.read_created(created.path), created.ids)
        code, out = self.main(["cleanup", "--run-id", "r1", "--out", out_dir], ec2)
        self.assertEqual(code, 0, out)
        self.assertIn("run r1: 12 recorded IDs from", out)
        self.assertIn("run r1: nothing left", out)
        self.assertNotIn("eni-untagged", ec2.store["network-interface"])

    def test_repeated_standalone_cleanup_keeps_the_same_recorded_ids(self):
        ec2, recorded = run_world()
        ec2.add("network-interface", "eni-hidden", None, Status="available")
        ec2.hidden.add("eni-hidden")
        recorded["network-interface"].append("eni-hidden")
        out_dir, created = self.recorded_run(recorded)
        argv = ["cleanup", "--run-id", "r1", "--out", out_dir]

        code, out = self.main(argv, ec2)
        self.assertEqual(code, 0, out)
        self.assertEqual(left(ec2), [])
        self.assertIn(("network-interface", "eni-hidden"), ec2.deleted)
        self.assertEqual(janitor.read_created(created.path), recorded)
        ec2.calls.clear()

        code, out = self.main(argv, ec2)
        self.assertEqual(code, 0, out)
        self.assertIn("run r1: 12 recorded IDs from", out)
        self.assertIn("run r1: nothing left", out)
        self.assertNotIn("recorded but never found", out)
        self.assertEqual(ec2.calls, [])
        self.assertEqual(janitor.read_created(created.path), recorded)

    def test_partial_cleanup_witnesses_survive_a_standalone_retry(self):
        ec2, recorded = run_world()
        ec2.add("network-interface", "eni-stuck", Status="in-use", _vpc="vpc")
        out_dir, created = self.recorded_run(recorded)
        argv = ["cleanup", "--run-id", "r1", "--out", out_dir]
        code, out = self.main(argv, ec2)
        self.assertEqual(code, 1, out)
        self.assertIn("network-interface eni-stuck", out)
        self.assertNotIn("zhida-bench-r1", ec2.store["placement-group"])

        ec2.store["network-interface"]["eni-stuck"]["Status"] = "available"
        code, out = self.main(argv, ec2)
        self.assertEqual(code, 0, out)
        self.assertIn("run r1: nothing left", out)
        self.assertEqual(left(ec2), [])
        self.assertEqual(janitor.read_created(created.path), recorded)

    def test_repeated_cleanup_does_not_confirm_a_fresh_hidden_not_found(self):
        ec2 = FakeEc2()
        ec2.add("vpc", "vpc-fresh")
        ec2.hidden.add("vpc-fresh")
        ec2.fail["delete_vpc"] = ClientError("InvalidVpcID.NotFound")
        out_dir, created = self.recorded_run({"vpc": ["vpc-fresh"]})
        argv = ["cleanup", "--run-id", "r1", "--out", out_dir]

        for _ in range(2):
            code, out = self.main(argv, ec2)
            self.assertEqual(code, 1, out)
            self.assertIn("vpc vpc-fresh: recorded but never found", out)
            self.assertNotIn("run r1: nothing left", out)
            self.assertIn("vpc-fresh", ec2.store["vpc"])
        self.assertEqual(janitor.read_journal(created.path)[1], set())

        ec2.hidden.clear()
        ec2.fail.clear()
        for _ in range(2):
            code, out = self.main(argv, ec2)
            self.assertEqual(code, 0, out)
            self.assertIn("run r1: nothing left", out)

    def test_accepted_async_delete_is_still_described_by_id_on_retry(self):
        ec2 = FakeEc2()
        ec2.add("instance", "i-pending", None, State={"Name": "running"})
        out_dir, _ = self.recorded_run({"instance": ["i-pending"]})
        argv = ["cleanup", "--run-id", "r1", "--out", out_dir]
        items = ec2._items

        def still_shutting_down(op, filters):
            ec2.store["instance"]["i-pending"].pop("_seen", None)
            return items(op, filters)

        with mock.patch.object(ec2, "_items", still_shutting_down):
            for _ in range(2):
                code, out = self.main(argv, ec2)
                self.assertEqual(code, 1, out)
                self.assertIn("instance i-pending", out)
                self.assertNotIn("run r1: nothing left", out)
        self.assertEqual(ec2.calls, [("terminate_instances", "i-pending")])

        ec2.store["instance"]["i-pending"]["State"]["Name"] = "terminated"
        code, out = self.main(argv, ec2)
        self.assertEqual(code, 0, out)
        self.assertIn("run r1: nothing left", out)

    def test_a_new_create_with_the_same_name_resets_its_cleanup_witness(self):
        ec2 = FakeEc2()
        ec2.add("placement-group", "pg")
        out_dir, created = self.recorded_run({"placement-group": ["pg"]})
        argv = ["cleanup", "--run-id", "r1", "--out", out_dir]
        code, out = self.main(argv, ec2)
        self.assertEqual(code, 0, out)

        ec2.add("placement-group", "pg")
        created.add("placement-group", "pg")
        ec2.hidden.add("pg")
        ec2.fail["delete_placement_group"] = ClientError("InvalidPlacementGroup.Unknown")
        code, out = self.main(argv, ec2)
        self.assertEqual(code, 1, out)
        self.assertIn("placement-group pg: recorded but never found", out)
        self.assertIn("pg", ec2.store["placement-group"])

    def test_cleanup_is_not_interrupted_by_ctrl_c(self):
        """Regression: standalone cleanup kept the default SIGINT handler."""
        ec2, _ = run_world()
        seen = []
        real = ec2.get_paginator

        def get_paginator(op):
            seen.append((signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)))
            return real(op)
        ec2.get_paginator = get_paginator
        saved = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
        try:
            self.main(["cleanup", "--run-id", "r1", "--out", tempfile.mkdtemp()], ec2)
        finally:
            for s, h in saved.items():
                signal.signal(s, h)
        self.assertTrue(seen and all(h == (signal.SIG_IGN, signal.SIG_IGN) for h in seen))

    def test_exit_status_follows_what_remains(self):
        ec2, _ = run_world()
        ec2.add("network-interface", "eni-stuck", Status="in-use")
        code, out = self.main(["cleanup", "--run-id", "r1", "--out", tempfile.mkdtemp()], ec2)
        self.assertEqual(code, 1)
        self.assertIn("tag lookup only", out)
        self.assertIn("NOT VERIFIED GONE; rerun `cleanup --run-id r1`", out)
        self.assertIn("network-interface eni-stuck", out)


if __name__ == "__main__":
    unittest.main()
