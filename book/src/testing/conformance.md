# Protocol conformance: Autobahn and TLS-Anvil

The [TLS and WebSocket part](../protocols/index.md) relies on two protocol implementations: switch-ws for WebSocket and switch-tls for TLS 1.3. Besides their own tests, which run like the others on [Running the test suite](suite.md) and [Writing tests that use DPDK](writing-tests.md), both are checked against outside conformance suites. switch-ws is run through the [Autobahn testsuite](https://github.com/crossbario/autobahn-testsuite), which sends a WebSocket implementation several hundred scripted cases, as a client and as a server. switch-tls is run through [TLS-Anvil](https://github.com/tls-attacker/TLS-Anvil), which tests a TLS client's handshake and error handling. This page explains how each run works, how to start it, and which results differ from what other implementations get. Like the DPDK half of the test suite, both runs need Docker and the DPDK image.

## Autobahn

The run uses the `switch-ws-autobahn` binary, a small driver built from the switch-ws crate (feature `e2e`). It runs switch-ws on a [tap device](../getting-started/first-connection.md) in one container, and Autobahn's `wstest` tool runs in a second container that shares its network. There are two roles, run one after the other:

- In the server role, `wstest` acts as the client (`fuzzingclient`) and connects to the driver's `ws://` echo server, the one shown on [Serving ws:// connections](../protocols/ws-server.md).
- In the client role, `wstest` acts as the server (`fuzzingserver`) and the driver connects to it with switch-ws's `ws://` client, running each case and echoing what it receives.

Each role writes Autobahn's report into a fresh `conduit/autobahn/run/<role>/` directory. A checker script, `conduit/autobahn/check.py`, then compares the report with `conduit/autobahn/expected-results-switch-ws.json`, the expected results for the agent `switch-ws` (Autobahn's name for the implementation under test). Each case must match the expected `behavior`, `behaviorClose` and `remoteCloseCode`.

## Running

From `conduit/`:

```sh
make docker-autobahn
```

This builds the Autobahn image if needed, builds the driver, runs the server role and then the client role, and exits 0 only if both roles match the expected results. `make docker-test` ends with this target, so a full Docker test run includes it.

The same suite can run through the [junction daemon](../junction/index.md) instead of a Stack in the driver's own process:

```sh
make docker-autobahn-junction           # both roles
make docker-autobahn-junction-server    # server role only
make docker-autobahn-junction-client    # client role only
```

Here the role container starts `switch-junctiond` on the first CPU of the container's CPU set and the driver on the second, with the Autobahn peer on a separate CPU. After the cases, the driver recovers its session with `Net::into_session`, [finishes it](../junction/closing.md), and the daemon is stopped; both exit statuses must be 0. All junction targets use the same cases and the same expected results as `docker-autobahn`. Each run replaces the previous reports in `run/`, so copy them away first if you want to compare backends.

The checker can also be run by hand on any report:

```sh
python3 autobahn/check.py <outdir>/index.json [--expected <path>]
```

It prints counts per outcome, then every case that differs from the expected result, with both outcomes and the case's report file, and lists missing and extra cases. It exits 0 on a full match, 1 on a mismatch and 2 on malformed input. It needs only the Python 3 standard library. `make test-autobahn-check` runs the self-tests of the checker and of the helper scripts; these need neither Docker nor DPDK.

## Expected results

The expected results start from those of [tungstenite](https://github.com/snapview/tungstenite-rs), a widely used Rust WebSocket library, and record where switch-ws legitimately differs. Of the 517 cases, 296 or 297 pass, 216 are reported as unimplemented, 3 are informational, and 1 or 2 fail by design.

The 216 unimplemented cases are sections 12 and 13 of the suite, which test permessage-deflate compression. switch-ws offers no extensions, so the server and client never agree to compress and these cases do not run.

The failing cases come from ping handling. When a ping arrives while the pong for an earlier ping has not been sent yet, switch-ws replaces that pong and answers only the newest ping. RFC 6455 section 5.5.3 allows this; Autobahn wants a pong for every ping. Case 2.10 therefore fails, and Autobahn closes the connection with an empty WebSocket close frame, which switch-ws echoes. Case 2.11 sends its pings a byte at a time, so depending on timing, switch-ws either answers every ping (the case passes) or replaces an unsent pong (the case fails). The expected results accept both outcomes for 2.11, which is why the counts above vary by one.

Cases 6.4.3 and 6.4.4 send text messages containing invalid UTF-8 and check how quickly the receiver gives up. switch-ws checks the UTF-8 of each WebSocket frame as its bytes arrive and fails the connection at the first invalid byte, so these cases pass outright, where tungstenite, which waits for the whole message, is rated "non-strict".

## TLS-Anvil

TLS-Anvil runs its client tests against switch-tls. From `conduit/`:

```sh
make docker-tls-anvil
```

The driver is the `tls_anvil` example of switch-tls, the connection logic shown on [TLS without WebSocket](../protocols/tls.md). It takes TLS-Anvil's address on the tap device and opens one TLS 1.3 connection each time TLS-Anvil asks for one: handshake, a short request, reading until close_notify, then close_notify and FIN. When a test makes the handshake fail, switch-tls sends the alert itself. The driver's certificate check accepts any certificate (it still checks the handshake signatures), because TLS-Anvil presents its own. TLS-Anvil's report is compared with a list of known failures in `conduit/tls-anvil/`, and the target exits 0 only if both containers exited 0 and no test failed outside that list. This run is not part of `make docker-test`.

The next part is the [Reference](../reference/crates.md), starting with the crate map.
