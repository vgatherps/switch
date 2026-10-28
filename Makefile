# Host (macOS, or Linux without DPDK) and Docker entry points. Bare `cargo test`
# builds every member, DPDK crates included; use `make test` on a host without it.
.PHONY: vendor-check test docker-test miri

vendor-check:
	vendor/rustls.sh check

# Crates that build and test without DPDK. sheath without features (its
# `switchnet` feature needs DPDK).
HOST_PACKAGES = switch switch-macros depot relay sheath \
	ceph-rseq ceph-ring ceph-fifo ceph-proto ceph-control

# Then the vendored smoltcp tests. TIMEOUT is cleared: GNU timeout is not on macOS.
test:
	cargo test $(addprefix -p ,$(HOST_PACKAGES))
	$(MAKE) -C conduit test-smoltcp TIMEOUT=

# Everything, DPDK included, in conduit's Docker image.
docker-test:
	$(MAKE) -C conduit docker-test

# switch's unsafe code under Miri, Stacked and Tree Borrows.
miri:
	cargo +nightly miri test -p switch --lib --test oneshot_join
	MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test -p switch --lib --test oneshot_join
