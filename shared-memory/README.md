# shared-memory

Memory that two processes share: a named POSIX shared-memory object or file, mapped once or twice back to back. The junction receive ring (`slice_ringbuffer`) and the cephalopod send rings (`ceph-ring`) are built on it.

API reference: `cargo doc -p shared-memory --no-deps`.

```sh
cargo test -p shared-memory    # part of the root `make test`
cephalopod/scripts/test-linux.sh test -p shared-memory    # Linux in Docker, with a hugetlbfs mount for the huge-page test
```

Without a hugetlbfs mount and free huge pages, the huge-page test skips.
