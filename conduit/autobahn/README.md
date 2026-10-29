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
