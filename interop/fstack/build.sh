#!/usr/bin/env bash
# Build only against the reviewed F-Stack revision; never install system-wide.
set -euo pipefail
: "${FF_PATH:?Set FF_PATH to the F-Stack checkout}"
: "${PKG_CONFIG_PATH:?Set PKG_CONFIG_PATH to the private DPDK installation}"
revision=34065f1396c7695408066c4bccc9dc98c02f60dc
if [[ $(git -C "$FF_PATH" rev-parse HEAD) != "$revision" ]]; then
    echo "Expected F-Stack $revision (shim uses its ff_config.h)" >&2
    exit 1
fi
if [[ $(pkg-config --modversion libdpdk) != 24.11.6 ]]; then
    echo "Use DPDK 24.11.6 bundled with the pinned F-Stack checkout" >&2
    exit 1
fi
out=${1:?Usage: build.sh /absolute/path/libcakemaster_fstack.so}
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
# Fix the pinned upstream revision's argv ownership bug on ff_run shutdown.
patch="$script_dir/eal-argv.patch"
if git -C "$FF_PATH" apply --reverse --check "$patch" 2>/dev/null; then
    : # already applied
elif git -C "$FF_PATH" apply --check "$patch"; then
    git -C "$FF_PATH" apply "$patch"
else
    echo "F-Stack checkout conflicts with the required shutdown fix" >&2
    exit 1
fi
# All F-Stack objects, including the embedded FreeBSD stack, must be PIC.
make -C "$FF_PATH/lib" -j"${JOBS:-8}" CONF_CFLAGS='-fPIC -Wno-error'
# Intentional pkg-config word splitting: preserve DPDK's whole-archive PMD flags.
# shellcheck disable=SC2046
cc -shared -fPIC -O2 -Wall -Wextra -Werror \
    $(pkg-config --cflags libdpdk) -I"$FF_PATH/lib" "$script_dir/shim.c" \
    -Wl,--whole-archive "$FF_PATH/lib/libfstack.a" -Wl,--no-whole-archive \
    $(pkg-config --static --libs libdpdk) -lcrypto -lpthread -ldl -lrt -lm \
    -o "$out"
