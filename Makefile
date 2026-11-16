# Host (macOS, or Linux without DPDK) and Docker entry points. Bare `cargo test`
# builds every member, DPDK crates included; use `make test` on a host without it.
.PHONY: vendor-check test docker-test miri

vendor-check:
	vendor/rustls.sh check

# Crates that build and test without DPDK. switch-tls without features (its
# `switch-net` feature needs DPDK).
HOST_PACKAGES = switch switch-macros switch-depot switch-ws switch-tls \
	ceph-rseq ceph-ring ceph-fifo ceph-proto ceph-control

# Then the vendored smoltcp tests (TIMEOUT is cleared: GNU timeout is not on macOS) and the EC2
# host setup script's tests (deploy/README.md).
test:
	cargo test $(addprefix -p ,$(HOST_PACKAGES))
	$(MAKE) -C conduit test-smoltcp TIMEOUT=
	sh deploy/test-hft-host.sh

# Everything, DPDK included, in conduit's Docker image.
docker-test:
	$(MAKE) -C conduit docker-test

# switch's unsafe code under Miri, Stacked and Tree Borrows; switch-ws's core and its one unsafe (Message::text).
miri:
	cargo +nightly miri test -p switch --lib --test oneshot_join
	MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test -p switch --lib --test oneshot_join
	cargo +nightly miri test -p switch-ws --lib
