#!/usr/bin/env bash
# Isolated software-PMD comparison. Requires unprivileged user namespaces, not sudo.
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
library=$(realpath "${1:?Usage: compare.sh library.so output-directory [compare|profile]}")
mode=${3:-compare}
case "$mode" in
    compare|profile) ;;
    *) echo "Expected compare or profile mode" >&2; exit 1 ;;
esac
mkdir -p "${2:?Missing output directory}"
out=$(realpath "$2")
if [[ -n $(find "$out" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    echo "Output directory must be empty (preserve prior runs): $out" >&2
    exit 1
fi
unshare --user --map-root-user --mount --net bash -s -- "$root" "$library" "$out" "$mode" <<'ISOLATED'
set -euo pipefail
root=$1 library=$2 out=$3 mode=$4
# Private /run avoids changing the host's named namespaces or DPDK runtime files.
mount --make-rprivate /
mount -t tmpfs tmpfs /run
mkdir -p /run/netns
ip netns add server
ip link add peer0 type veth peer name dpdk0
ip link set dpdk0 netns server
ip addr add 198.18.0.1/24 dev peer0
ip link set lo up
ip link set peer0 up
ip netns exec server ip link set lo up
ip netns exec server ip link set dpdk0 up
# AF_PACKET cannot consume Linux CHECKSUM_PARTIAL/GSO packets as a physical NIC
# would. Use the same offload-disabled veth path for the kernel baseline.
ethtool -K peer0 tx off rx off tso off gso off gro off > "$out/offloads-client.log" 2>&1
ip netns exec server ethtool -K dpdk0 tx off rx off tso off gso off gro off > "$out/offloads-server.log" 2>&1
sleep 1
python3 "$root/interop/fstack/$mode.py" "$root" "$library" "$out"
ISOLATED
