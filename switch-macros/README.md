# switch-macros

See [Writing and spawning tasks](../book/src/runtime/tasks.md) in the switch book.
Generate the API reference with `cargo doc -p switch-macros --no-deps`.

The `#[task]` attribute for [`switch-runtime`](../switch-runtime/README.md). Use it as `#[switch_runtime::task]`; switch-runtime re-exports it, so depend on switch-runtime, not on this crate.

```rust
#[switch_runtime::task]
async fn session(net: NetHandle, id: u32) { /* ... */ }

#[switch_runtime::task]
fn wrapped(value: u8) -> impl Future<Output = Result<u32, Error>> { async move { /* ... */ } }

spawner.spawn(session(net.clone(), 7))?;
let handle = spawner.spawn_join(wrapped(3))?; // resolves to Ok(the task's Result)
```

The marked function keeps its name, visibility, generics and attributes, but returns a `switch_runtime::SpawnToken` that holds the arguments; the token's task output is the function's return value. Spawning it builds the future in a task slot. The expansion turns the body into a closure that makes the future: for an `async fn` the closure returns an async block, and every argument is rebound inside the future, so unused arguments drop with the future, as in an `async fn`. For a plain `fn`, arguments the returned future does not keep drop when the closure returns, at spawn. `#[cfg]` and `#[cfg_attr]` on arguments apply to both. The expansion contains no `unsafe` and declares no item or local that the signature or body can see, so user names keep their meaning…

Each task function has its own pool of task blocks, and so does each instantiation of a generic one: generic parameters, `impl Trait` arguments (including one produced by a type macro) and the parameters of an enclosing generic `impl`, even when the future type does not depend on them. The pool is keyed by the type of a non-capturing closure in the expansion, which carries all of these and adds no `'static` requirement. Instantiations that differ only in lifetimes share a pool. A pool has no slot limit. The attribute takes no arguments.

The function can be an `async fn`, or a `fn` returning `impl Future<Output = T>`, optionally `+ 'static`. The future and its value must be `'static`. Rejected with a compile error:

- a `self` receiver;
- `const`, `unsafe` or `extern` functions;
- argument patterns other than a plain identifier;
- any other return type, including other bounds such as `+ Send`;
- futures that are not `'static`, such as arguments borrowing for a shorter lifetime (the compiler reports these).

## Tests

```sh
cargo test -p switch-macros
```

`tests/ui` holds trybuild cases: `pass_*.rs` files that must compile and run, and one `fail_*.rs` file per group of rejected forms with its expected compiler output. After an intended change to the messages, regenerate the expected output with `TRYBUILD=overwrite cargo test -p switch-macros` and review the diff.
