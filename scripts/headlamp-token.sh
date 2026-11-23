#!/usr/bin/env bash
# Print a short-lived login token for Headlamp (http://localhost:8080/headlamp/).
# The token belongs to ServiceAccount headlamp/headlamp-viewer (read-only
# ClusterRole "view", no Secrets). Run on the Mac with the admin kubeconfig.
#
# Usage: scripts/headlamp-token.sh [duration]     default 8h, e.g. 30m, 24h
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_tools kubectl
kc -n headlamp create token headlamp-viewer --duration="${1:-8h}"
