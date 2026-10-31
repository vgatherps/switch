#!/usr/bin/env python3
"""Tests of janitor.py against a fake EC2 and Scheduler: python3 bench/aws/test_janitor.py"""
import datetime
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import janitor  # noqa: E402

UTC = datetime.timezone.utc
NOW = datetime.datetime(2026, 11, 1, 12, 0, tzinfo=UTC)


def tagged(run, expires):
    return [{"Key": janitor.RUN_TAG, "Value": run}, {"Key": janitor.EXPIRES_TAG, "Value": expires.isoformat()}]


class FakeEc2:
    """Instances terminate after one describe; terminating frees their address association and
    their DeleteOnTermination ENIs."""

    def __init__(self):
        self.instances, self.addresses, self.enis, self.volumes = {}, {}, {}, {}
        self.calls = []

    def _match(self, resource, filters):
        t = janitor.tags(resource)
        for f in filters:
            if f["Name"] == "tag-key" and not any(k in t for k in f["Values"]):
                return False
            if f["Name"].startswith("tag:") and t.get(f["Name"][4:]) not in f["Values"]:
                return False
        return True

    def describe_instances(self, Filters=(), InstanceIds=None):
        out = []
        for iid, i in self.instances.items():
            if InstanceIds is not None and iid not in InstanceIds:
                continue
            if not self._match(i, Filters):
                continue
            if i["State"]["Name"] == "shutting-down" and i.pop("_described", False):
                i["State"]["Name"] = "terminated"
                for e in list(self.enis.values()):
                    if e.get("_owner") == iid:
                        del self.enis[e["NetworkInterfaceId"]]
                for a in self.addresses.values():
                    if a.get("_owner") == iid:
                        a.pop("AssociationId", None)
            elif i["State"]["Name"] == "shutting-down":
                i["_described"] = True
            out.append({k: v for k, v in i.items() if not k.startswith("_")})
        return {"Reservations": [{"Instances": out}]}

    def terminate_instances(self, InstanceIds):
        self.calls.append(("terminate", tuple(InstanceIds)))
        for iid in InstanceIds:
            self.instances[iid]["State"]["Name"] = "shutting-down"

    def describe_addresses(self, Filters=()):
        return {"Addresses": [dict(a) for a in self.addresses.values() if self._match(a, Filters)]}

    def disassociate_address(self, AssociationId):
        self.calls.append(("disassociate", AssociationId))

    def release_address(self, AllocationId):
        a = self.addresses[AllocationId]
        if a.get("_owner") and self.instances[a["_owner"]]["State"]["Name"] != "terminated":
            raise AssertionError("released an address its instance still holds")
        self.calls.append(("release", AllocationId))
        del self.addresses[AllocationId]

    def describe_network_interfaces(self, Filters=()):
        return {"NetworkInterfaces": [dict(e) for e in self.enis.values() if self._match(e, Filters)]}

    def delete_network_interface(self, NetworkInterfaceId):
        assert self.enis[NetworkInterfaceId]["Status"] == "available"
        self.calls.append(("delete-eni", NetworkInterfaceId))
        del self.enis[NetworkInterfaceId]

    def describe_volumes(self, Filters=()):
        return {"Volumes": [dict(v) for v in self.volumes.values() if self._match(v, Filters)]}

    def delete_volume(self, VolumeId):
        self.calls.append(("delete-volume", VolumeId))
        del self.volumes[VolumeId]


class NotFound(Exception):
    pass


class FakeScheduler:
    class exceptions:
        ResourceNotFoundException = NotFound

    def __init__(self, names):
        self.names = set(names)

    def delete_schedule(self, Name):
        if Name not in self.names:
            raise NotFound(Name)
        self.names.remove(Name)


def world():
    ec2 = FakeEc2()
    live, old = NOW + datetime.timedelta(minutes=30), NOW - datetime.timedelta(minutes=1)
    for iid, run, exp in [("i-a", "r1", old), ("i-b", "r1", old), ("i-c", "r2", live)]:
        ec2.instances[iid] = {"InstanceId": iid, "State": {"Name": "running"}, "Tags": tagged(run, exp)}
    ec2.instances["i-shared"] = {"InstanceId": "i-shared", "State": {"Name": "running"}, "Tags": []}
    ec2.addresses["eipalloc-1"] = {"AllocationId": "eipalloc-1", "AssociationId": "eipassoc-1",
                                   "Tags": tagged("r1", old), "_owner": "i-a"}
    ec2.enis["eni-attached"] = {"NetworkInterfaceId": "eni-attached", "Status": "in-use",
                                "Tags": tagged("r1", old), "_owner": "i-b"}
    ec2.enis["eni-loose"] = {"NetworkInterfaceId": "eni-loose", "Status": "available", "Tags": tagged("r1", old)}
    ec2.volumes["vol-loose"] = {"VolumeId": "vol-loose", "State": "available", "Tags": tagged("r1", old)}
    return ec2


class JanitorTest(unittest.TestCase):
    def test_run_cleanup_deletes_everything_of_that_run_in_order(self):
        ec2, sched = world(), FakeScheduler({"zhida-bench-r1", "zhida-bench-r2"})
        done = janitor.clean(ec2, sched, run_id="r1", now=NOW, poll_s=0, log=lambda m: None)
        self.assertEqual(sorted(done["instances"]), ["i-a", "i-b"])
        self.assertEqual(done["addresses"], ["eipalloc-1"])
        self.assertEqual(done["network_interfaces"], ["eni-loose"])
        self.assertEqual(done["volumes"], ["vol-loose"])
        self.assertEqual(done["schedules"], ["zhida-bench-r1"])
        kinds = [c[0] for c in ec2.calls]
        self.assertLess(kinds.index("terminate"), kinds.index("release"), "EIP released before the instance went")
        self.assertEqual(ec2.instances["i-c"]["State"]["Name"], "running", "another run touched")
        self.assertEqual(ec2.instances["i-shared"]["State"]["Name"], "running", "untagged instance touched")
        self.assertEqual(sched.names, {"zhida-bench-r2"})

    def test_second_cleanup_is_a_no_op(self):
        ec2, sched = world(), FakeScheduler({"zhida-bench-r1"})
        janitor.clean(ec2, sched, run_id="r1", now=NOW, poll_s=0, log=lambda m: None)
        ec2.calls.clear()
        done = janitor.clean(ec2, sched, run_id="r1", now=NOW, poll_s=0, log=lambda m: None)
        self.assertEqual(done, {"instances": [], "addresses": [], "network_interfaces": [], "volumes": [], "schedules": []})
        self.assertEqual(ec2.calls, [])

    def test_expired_cleanup_takes_only_runs_past_their_ttl(self):
        ec2 = world()
        done = janitor.clean(ec2, None, run_id=None, now=NOW, poll_s=0, log=lambda m: None)
        self.assertEqual(sorted(done["instances"]), ["i-a", "i-b"])
        self.assertEqual(ec2.instances["i-c"]["State"]["Name"], "running")
        later = NOW + datetime.timedelta(hours=1)
        done = janitor.clean(ec2, None, run_id=None, now=later, poll_s=0, log=lambda m: None)
        self.assertEqual(done["instances"], ["i-c"])

    def test_instances_that_never_terminate_time_out(self):
        ec2 = world()
        ec2.describe_instances_orig = ec2.describe_instances

        def stuck(Filters=(), InstanceIds=None):
            out = ec2.describe_instances_orig(Filters=Filters, InstanceIds=InstanceIds)
            for r in out["Reservations"]:
                for i in r["Instances"]:
                    ec2.instances[i["InstanceId"]].pop("_described", None)
            return out

        ec2.describe_instances = stuck
        with self.assertRaises(TimeoutError):
            janitor.clean(ec2, None, run_id="r1", now=NOW, wait_s=0, poll_s=0, log=lambda m: None)
        self.assertIn("eipalloc-1", ec2.addresses, "address released while its instance lives")


if __name__ == "__main__":
    unittest.main()
