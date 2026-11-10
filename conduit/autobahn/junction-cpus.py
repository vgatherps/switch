#!/usr/bin/env python3
"""Choose two role CPUs, extending a one-CPU DOCKER_CPUSET by one non-peer CPU."""
import sys


def expand(text):
    cpus = set()
    for part in text.split(","):
        bounds = part.split("-")
        if len(bounds) not in (1, 2) or any(not n.isdecimal() for n in bounds):
            raise ValueError(f"invalid cpuset {text!r}")
        first, last = int(bounds[0]), int(bounds[-1])
        if last < first:
            raise ValueError(f"invalid cpuset {text!r}")
        cpus.update(range(first, last + 1))
    return cpus


def choose(role, peer):
    cpus, peers = expand(role), expand(peer)
    if cpus & peers:
        raise ValueError("DOCKER_CPUSET and AUTOBAHN_CPUSET must not overlap")
    network = min(cpus)
    if len(cpus) == 1:
        app = 0
        while app in cpus or app in peers:
            app += 1
        cpus.add(app)
    else:
        app = min(cpus - {network})
    return network, app, ",".join(map(str, sorted(cpus)))


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        assert choose("0", "1") == (0, 2, "0,2")
        assert choose("2", "0-1") == (2, 3, "2,3")
        assert choose("3-4", "1") == (3, 4, "3,4")
        assert choose("0,2-3", "1") == (0, 2, "0,2,3")
        for role, peer in [("0-2", "1"), ("bad", "1"), ("3-1", "0")]:
            try:
                choose(role, peer)
            except ValueError:
                pass
            else:
                raise AssertionError((role, peer))
        print("junction-cpus: self-test PASS")
    else:
        try:
            print(*choose(*sys.argv[1:]))
        except (ValueError, TypeError) as error:
            sys.exit(f"junction-cpus: {error}")
