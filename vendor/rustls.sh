#!/bin/sh
# vendor/rustls is generated: the crates.io release of rustls with vendor/patches/rustls/*.patch
# applied. Never edit it by hand; change a patch and run `update`.
#
#   vendor/rustls.sh update <version>   regenerate vendor/rustls from rustls-<version>.crate
#   vendor/rustls.sh check              fail if vendor/rustls differs from release + patches
#
# The .crate comes from the cargo registry cache and must match the crates.io SHA-256 below.
# Patches are `git format-patch` output against the upstream repository. Only hunks for the
# crate (rustls/...) apply here; upstream-only files (rustls/tests/, which the published crate
# does not ship, and anything outside rustls/, e.g. fuzz/) are skipped.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
vendored=$here/rustls
patches=$here/patches/rustls

# crates.io checksum of rustls-<version>.crate (the `checksum` field of its Cargo.lock entry)
expected_sha256() {
    case $1 in
    0.23.43) echo 0283386ce02abc0151e1761d08802dfe86c173b0b494af5cbc086574e453da06 ;;
    *)
        echo "no recorded crates.io checksum for rustls $1; add it to $0" >&2
        return 1
        ;;
    esac
}

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

# build <version> <dir>: unpack the verified release into <dir>/rustls, apply patches
build() {
    expected=$(expected_sha256 "$1")
    crate=$(ls "${CARGO_HOME:-$HOME/.cargo}"/registry/cache/*/"rustls-$1.crate" 2>/dev/null | head -n1)
    if [ -z "$crate" ]; then
        echo "rustls-$1.crate is not in the cargo registry cache; fetch it first" >&2
        return 1
    fi
    actual=$(sha256 "$crate")
    if [ "$actual" != "$expected" ]; then
        echo "$crate: sha256 $actual, expected $expected" >&2
        return 1
    fi
    tar -xzf "$crate" -C "$2"
    mv "$2/rustls-$1" "$2/rustls"
    for patch in "$patches"/*.patch; do
        [ -e "$patch" ] || continue
        (cd "$2" && git apply -p1 --exclude='rustls/tests/*' --include='rustls/*' "$patch") || {
            echo "$patch does not apply to rustls $1" >&2
            return 1
        }
    done
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

case "${1:-}" in
update)
    [ $# -eq 2 ] || { echo "usage: $0 update <version>" >&2; exit 2; }
    build "$2" "$tmp"
    rm -rf "$vendored"
    mv "$tmp/rustls" "$vendored"
    ;;
check)
    version=$(sed -n 's/^version = "\(.*\)"$/\1/p' "$vendored/Cargo.toml" | head -n1)
    build "$version" "$tmp"
    diff -r "$tmp/rustls" "$vendored"
    ;;
*)
    echo "usage: $0 update <version> | check" >&2
    exit 2
    ;;
esac
