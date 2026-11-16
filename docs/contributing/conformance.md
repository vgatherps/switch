# Protocol conformance

Besides their own tests, the WebSocket and TLS implementations run against two outside test suites. The [Autobahn testsuite](https://github.com/crossbario/autobahn-testsuite) sends a WebSocket implementation 517 scripted cases, as a client and as a server; `make docker-test` runs it at the end of every full test run. [TLS-Anvil](https://github.com/tls-attacker/TLS-Anvil) tests a TLS client's handshake and error handling; it runs only when you ask for it.

Both need the Docker image (`make -C conduit docker-image`).

## Autobahn

1. From the repository root:

   ```sh
   make -C conduit docker-autobahn
   ```

   This builds the Autobahn image if needed, builds the driver `switch-ws-autobahn`, and runs two roles one after the other. In the server role, Autobahn's `wstest` is the client and connects to the driver's `ws://` echo server. In the client role, `wstest` is the server and the driver connects to it with switch-ws's client, once per case. The driver runs switch-ws over a tap device in one container (on `DOCKER_CPUSET`, default CPU 0); `wstest` runs in a second container on `AUTOBAHN_CPUSET` (default CPU 1) that shares the first one's network. The two CPU sets must differ.

2. Each role ends with one line:

   ```text
   docker-autobahn: server PASS: <seconds> s, driver exit 0, wstest 0, failures: none (logs in autobahn/run/server/)
   ```

   A role passes when the driver, `wstest` and the report check all succeed. The target exits 0 only if both roles pass.

3. If a role fails, look in `conduit/autobahn/run/<role>/`: `switch.log` (the driver and the check), `autobahn.log` (`wstest`), and Autobahn's own report under `reports/`, with `index.json` and one file per case.

Only one Autobahn run at a time can use a checkout. A second one stops with `docker-autobahn: lock .../conduit/autobahn/run.lock exists: another run in this checkout, or a stale lock (remove it by hand)`; after a crash, delete that directory.

### What counts as passing

`conduit/autobahn/check.py` compares the report with `conduit/autobahn/expected-results-switch-ws.json`. Each case must match the expected `behavior`, `behaviorClose` and `remoteCloseCode`. To run it on a report yourself:

```sh
python3 conduit/autobahn/check.py conduit/autobahn/run/server/reports/index.json
```

It prints counts per outcome, then each case that differs (`DIVERGES <case> expected=... actual=... report=...`), and missing and extra cases. It ends with `PASS: 517 cases match ...` or `FAIL: ...`, and exits 0 on a match, 1 on a mismatch and 2 on malformed input. `--expected PATH` checks against another file.

The expected results start from those of [tungstenite](https://github.com/snapview/tungstenite-rs), a Rust WebSocket library, and record where switch-ws differs. Of the 517 cases, 296 to 298 pass, 216 are unimplemented, 3 are informational and 0 to 2 fail:

- The 216 unimplemented cases are sections 12 and 13, which test permessage-deflate compression. switch-ws offers no extensions, so these cases never negotiate it.
- Cases 2.10 and 2.11 send ten pings back to back. When switch-ws reads several pings before it has sent the pong for the first, it answers only the newest, which RFC 6455 section 5.5.3 allows. Autobahn wants a pong for every ping, marks the case failed and closes with an empty close frame. When the pings arrive apart, each gets its pong and the case passes. Which happens depends on timing, so the expected results accept either outcome for both cases.
- Cases 6.4.3 and 6.4.4 send invalid UTF-8 in text messages. switch-ws checks UTF-8 as the bytes of each frame arrive and fails the connection at the first invalid byte, so these cases pass where tungstenite, which waits for the whole message, is rated non-strict.

When you change switch-ws so that a case's outcome changes on purpose, edit its entry in the expected results file. An entry may be a list of accepted outcomes, as 2.10 and 2.11 are.

`make -C conduit test-autobahn-check` runs the checker's self-tests and those of the scripts around it. It needs Linux but not DPDK, and the full `make test` in the container runs it.

### Sharing a card through the daemon

The same cases can run with the driver sharing the tap device through the daemon, `switch-junctiond`, instead of owning it:

```sh
make -C conduit docker-autobahn-junction          # both roles
make -C conduit docker-autobahn-junction-server   # server role only
make -C conduit docker-autobahn-junction-client   # client role only
```

The daemon runs on the first CPU of `DOCKER_CPUSET` and the driver on the second; with one CPU in the set, the target adds the lowest CPU that is in neither `DOCKER_CPUSET` nor `AUTOBAHN_CPUSET`, so the defaults become CPUs 0 and 2. It prints the CPUs it chose before it starts. After the cases the driver closes its session and the daemon is stopped; both must exit 0. The cases, the expected results and the `run/<role>/` directories are the same as for `docker-autobahn`, so each run replaces the last one's reports.

## TLS-Anvil

```sh
make -C conduit docker-tls-anvil
```

The driver is the `tls_anvil` example of switch-tls. It runs switch-tls's TLS 1.3 client over a tap device and opens one connection each time TLS-Anvil asks for one: the handshake, a short request, reading until the server's close_notify, then its own close_notify. Its certificate check accepts any certificate, because TLS-Anvil presents its own. TLS-Anvil runs in a second container sharing the driver's network, on `TLS_ANVIL_CPUSET` (default CPUs 1 to 3). Its published image is amd64 only, so on an arm64 machine Docker emulates it and the run is slow; `TLS_ANVIL_TIMEOUT` (default 7200 seconds) bounds it.

The run ends with:

```text
docker-tls-anvil: <seconds> s, TLS-Anvil exit 0, driver exit 0; results in .../conduit/tls-anvil/run/results
```

followed by the comparison of the results with `conduit/tls-anvil/known-failures.txt`, which lists the tests that fail for reasons outside switch-tls, each with its reason. The comparison prints `NEW FAILURE <test id> <test>` for each failure not on the list and `known failure no longer fails: <test id> <result>: <reason>` for each listed test that did not fail, and ends with a count line starting `compare.py:`. That line also appears for a listed test that was disabled or did not run, so check its result before you remove the test from the list. The target exits 0 only if TLS-Anvil and the driver exited 0 and there is no new failure.

`TLS_ANVIL_ARGS` passes more options to TLS-Anvil, for example `TLS_ANVIL_ARGS='-testPackage de.rub.nds.tlstest.suite.tests.client.tls13'` to run a subset.
