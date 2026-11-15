#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo -E bash "$0" "$@"
fi

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y \
  openjdk-11-jdk python3.10 python3.10-venv python3-pip \
  openssh-server rsync curl wget ca-certificates tar gzip \
  gettext-base net-tools dnsutils iproute2 ufw jq

systemctl enable --now ssh

JAVA_HOME_DETECTED="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
cat >/etc/profile.d/bigdata-java.sh <<EOF
export JAVA_HOME="${JAVA_HOME_DETECTED}"
export PATH="\$JAVA_HOME/bin:\$PATH"
EOF

echo "Installed prerequisites:"
java -version
python3.10 --version
echo "JAVA_HOME=${JAVA_HOME_DETECTED}"
echo "Next: configure Tailscale and verify all three nodes can resolve/ping one another."
