# switch-macros

The `#[task]` attribute for [`switch`](../switch/README.md). Use it as `#[switch::task]`; switch re-exports it, so depend on switch, not on this crate.

```rust
#[switch::task]
async fn session(net: NetHandle, id: u32) { /* ... */ }

#[switch::task(capacity = 8)]
fn wrapped(value: u8) -> impl Future<Output = ()> { async move { /* ... */ } }

spawner.spawn(session(net.clone(), 7))?;
```

The marked function keeps its name, visibility and attributes, but now returns a `switch::SpawnToken` that holds the arguments. Spawning it builds the future in a task slot. The original body moves into a nested function, and a per-function thread-local pool is declared next to it. Inside a generic `impl`, the nested function cannot use the impl's parameters, so the task still has one future type. The expansion is built on switch's hidden `__private` items.

`capacity = N` is the most occupied slots of this function per runtime. Without it the capacity is one 64 KiB block's worth of slots. N must be a constant from 1 to 2^25 − 1; any other value fails to compile.

The function can be an `async fn` returning `()`, or a `fn` returning `impl Future<Output = ()>`. Rejected with a compile error:

- generic parameters (type, lifetime or const), a `where` clause, or `impl Trait` in an argument type. The pool is a static, and statics are shared by every instantiation of a generic function, so one pool would hold futures of different layouts. The error is "switch tasks cannot be generic: write a concrete wrapper task";
- a `self` receiver;
- argument patterns other than a plain identifier;
- arguments that are not `'static` (the compiler reports these);
- any other output type (the compiler reports these).

## Tests

```sh
cargo test -p switch-macros
```

`tests/ui` holds trybuild cases: one file that must compile and run, and one file per group of rejected forms with its expected compiler output. After an intended change to the messages, regenerate the expected output with `TRYBUILD=overwrite cargo test -p switch-macros` and review the diff.
