# Host (macOS, or Linux without DPDK) and Docker entry points. Bare `cargo test`
# builds every member, DPDK crates included; use `make test` on a host without it.
.PHONY: vendor-check test docker-test miri docs book api-docs

vendor-check:
	vendor/rustls.sh check

# Crates that build and test without DPDK. switch-tls without features (its
# `switch-net` feature needs DPDK).
HOST_PACKAGES = switch switch-macros switch-depot switch-frame shared-memory slice_ringbuffer switch-ws switch-tls \
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

# `make docs` builds the book and every crate's API reference and opens both in a browser.
# The API reference builds in conduit's Docker image, because most crates need DPDK
# (build the image once with `make -C conduit docker-image`); it is written to target/api-docs.
OPEN ?= $(if $(filter Darwin,$(shell uname -s)),open,xdg-open)

docs: book api-docs

book:
	@command -v mdbook >/dev/null || { echo 'Missing mdbook: cargo install mdbook' >&2; exit 1; }
	mdbook build book
	$(OPEN) book/book/index.html

api-docs:
	$(MAKE) -C conduit docker-doc
	$(OPEN) target/api-docs/switch_net/index.html

# switch's unsafe code under Miri, Stacked and Tree Borrows; switch-ws's core and its one unsafe (Message::text).
miri:
	cargo +nightly miri test -p switch --lib --test oneshot_join
	MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test -p switch --lib --test oneshot_join
	cargo +nightly miri test -p switch-ws --lib
