# Autobahn report checker

`expected-results-relay.json` holds relay's expected results (agent `relay`, the driver
`relay-autobahn` in relay/src/bin): tungstenite's `autobahn/expected-results.json` at tag v0.30.0
(commit 7f4aeaf094, sha256 `1ef1023a82e8602e10404c0d0cacfff9400b122131546972fe597d2298ab5c8f`),
cut to the compared fields, with these divergences:

| Case | tungstenite | relay | Why |
|---|---|---|---|
| 2.10, 2.11 | OK, close 1000 | FAILED, close none | Ten pings back to back get one pong, for the newest: a ping replaces an unsent pong (RFC 6455 §5.5.3 allows it; Autobahn wants every pong, "not required by the Spec"). Autobahn then closes with an empty close, which relay echoes |
| 6.4.3, 6.4.4 | NON-STRICT | OK | relay validates UTF-8 per frame as bytes arrive and fails fast on the first invalid byte; tungstenite waits for the message |

517 cases: 296 OK/OK, 216 UNIMPLEMENTED/OK (12.x and 13.x: no permessage-deflate),
3 INFORMATIONAL/INFORMATIONAL, 2 FAILED/OK.

```sh
python3 autobahn/check.py <outdir>/index.json [--expected <path>]
python3 autobahn/check.py --self-test    # make test-autobahn-check
```

Python 3 stdlib only. Both files must have exactly the one agent key `relay` and the same case
ids; each case must match on `behavior`, `behaviorClose` and `remoteCloseCode` (null is a value;
other fields such as `duration` are ignored). Input is malformed (exit 2) on unreadable JSON, a
duplicate object key at any level, a non-string `behavior` / `behaviorClose` / `reportfile`, or
a `remoteCloseCode` that is not an int or null (`1000.0` is rejected). Prints
per-(behavior, behaviorClose) counts of the report, then every diverging case (expected vs
actual, plus the case report file), missing and extra case ids (numeric id parts sort before
non-numeric ones). Exit 0 full match, 1 mismatch (including agent keys), 2 malformed input.
`--self-test` runs 15 scenarios, each checking the exit code, a scenario-specific output line
(`DIVERGES <id>`, `MISSING`, `EXTRA`, the agent-key or malformed reason) and the absence of a
traceback: the expected file against itself and with changed `duration`/`reportfile` (exit 0);
changed behavior, behaviorClose, close code number->number, null->number, number->null,
missing case, extra case, extra mixed ids (`99.9.9`, `extra`), wrong agent key, extra agent key
(exit 1); duplicate case key, null behavior, close code `1000.0` (exit 2).

## Junction backend

`make docker-autobahn-junction` runs both roles through a separate `junctiond` process,
including the client when the server fails; the target succeeds only if both roles pass.
`docker-autobahn-junction-server` and `docker-autobahn-junction-client` run one role.
`docker-autobahn` remains the conduit gate, unchanged. All use the same 517 cases,
agent, `run/<role>/` markers, peer container/network namespace, and expected checker.
Each role replaces its previous reports; preserve `run/` before comparing backends.

The junction role container starts `junctiond` on the first CPU in `DOCKER_CPUSET`
and the app on the second. If the set has only one CPU, it adds exactly one: the
lowest CPU not in `DOCKER_CPUSET` or `AUTOBAHN_CPUSET`. Defaults therefore extend
`0` to `0,2`; the Autobahn peer stays on `1`. The control thread shares the app CPU.
Overlapping role/peer CPU sets are rejected. Docker must provide those CPUs.

`junction-netconf.json` uses the conduit driver's tap name, MACs, IPv4 subnet,
MTU, descriptors and disabled checksum offloads. The daemon uses its normal
Stack/pool sizing. The app opens no EAL or Stack, and sizes its TX frame pool with
`tcp::tx_mbuf_budget`, as the socket test fixture does. After the same checker completes,
the app recovers `Net::into_session`, finishes it and waits for retirement before
exiting. `junction-role.sh` then sends SIGTERM to `junctiond`, reaps it and checks
both exit statuses. The outer role timeout bounds startup, run and shutdown.

`make test-autobahn-check` also checks CPU selection and the junction role wrapper:
socket readiness, separate CPU pins, app-before-daemon teardown, and failure of
either process. Those wrapper tests use fake processes; the junction make targets
exercise the real daemon and app.
