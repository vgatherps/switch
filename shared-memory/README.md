# shared-memory

Unix backing ownership and single/double mappings used by `ceph-ring` and
`slice_ringbuffer`. Depends only on `libc`; no queue protocol or synchronization.

- `Backing::create_shm` / `create_file`: exclusive creation, mode `0600`, owned
  descriptor and name. `open_shm` / `open_file`: owned descriptor only.
- `resize`, `len`, `page_size`, `fd`, `name`: backing operations. `unlink` removes
  an owned name once; drop removes it if still owned. Openers cannot unlink it.
  Closing descriptors or unlinking names does not invalidate existing mappings.
  `allocate(len)` reserves `[0, len)` with checked `posix_fallocate` on Linux;
  macOS uses `resize`. RX rings allocate before mapping so filesystem exhaustion
  is returned as an error rather than a later mapping fault.
- `Mapping::new` / `read_only`: map a nonzero file extent at a page-aligned offset.
  `len` is the requested usable length; ownership covers the page-rounded mapping.
- `DoubleMapping::new(fd, offset, len, populate)`: map the same extent twice at
  adjacent addresses, so a span crossing the first view's end stays contiguous.
  `len` is one view's length and must be a multiple of the backing page size.
  Linux `populate` requests `MAP_POPULATE`.
- `as_ptr` exposes raw memory only. The caller owns synchronization, initialization,
  valid access lengths, and references' lifetimes. Do not truncate a live backing.
  `lock` requests `mlock`; the double-mapping version locks both aliases.

Double mapping reserves one contiguous address range, then replaces its two halves
with `MAP_FIXED` aliases of the file extent. Alignment slack is released; drop
releases exactly the two aliases. On a failed replacement, cleanup leaves that
half alone because its address ownership is unknown, potentially leaking at most
one view's address space. Other ranges still owned by the constructor are released.

Linux hugetlbfs files use the mount's page size for offsets, lengths and virtual
alignment. Provision the huge-page pool and pass a file on that mount; this crate
does not configure the system or silently fall back to ordinary pages. POSIX shm
uses native pages. `page_size_in(dir)` supports sizing before file creation.

`ceph-ring` retains its hugetlbfs directory configuration. `slice_ringbuffer` still
uses POSIX shm and does not yet expose a huge-page backing choice.

```sh
cargo test -p shared-memory
cephalopod/scripts/test-linux.sh test -p shared-memory -- --nocapture
```

The Linux runner provisions hugetlbfs for the huge-page test. That test otherwise
skips when no mount or huge pages are available.
