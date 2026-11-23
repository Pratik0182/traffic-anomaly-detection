#!/usr/bin/env bash
set -euo pipefail
if [[ "${EUID}" -ne 0 ]]; then exec sudo -E bash "$0" "$@"; fi
if ! ip link show tailscale0 >/dev/null 2>&1; then
  echo "tailscale0 is not present. Install and authenticate Tailscale before enabling this firewall rule." >&2
  exit 1
fi
ufw default deny incoming
ufw default allow outgoing
ufw allow in on tailscale0
ufw --force enable
ufw status verbose
echo "This allows node traffic only through tailscale0 at the host firewall."
echo "Use Tailscale ACLs to restrict membership; never expose Hadoop/Kafka/HBase ports publicly."
