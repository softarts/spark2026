#!/usr/bin/env bash
# Stop the Spark Platform Learning Lab.
#
# Usage:
#   ./stopcluster.sh          Stop all lab Pods (scale to 0). PVC data
#                             (Notebook home, Silo objects, Hub DB) is kept;
#                             ./startcluster.sh brings everything back.
#                             Docker Desktop keeps port 8080 bound while the
#                             Traefik Service exists, but nothing answers on it.
#   ./stopcluster.sh --purge  Uninstall everything and DELETE all lab data
#                             (namespaces spark-lab, traefik and every
#                             user-<name>, all PVCs).
#
# Docker Desktop itself keeps running (other containers may depend on it).
set -euo pipefail
source "$(dirname "$0")/scripts/common.sh"

PURGE=0
for arg in "$@"; do
  case "$arg" in
    --purge) PURGE=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) die "unknown option: $arg" ;;
  esac
done

require_tools kubectl helm
kc get --raw /readyz >/dev/null 2>&1 || { log "Kubernetes ($LAB_CONTEXT) is not running; nothing to stop"; exit 0; }

if [ "$PURGE" -eq 1 ]; then
  warn "This deletes Notebook files, Silo objects and the JupyterHub database."
  read -r -p "Type '$LAB_NAMESPACE' to confirm: " answer
  [ "$answer" = "$LAB_NAMESPACE" ] || die "aborted"
  hm uninstall jupyterhub -n "$LAB_NAMESPACE" --wait >/dev/null 2>&1 || true
  hm uninstall traefik -n "$INGRESS_NAMESPACE" --wait >/dev/null 2>&1 || true
  # Per-user namespaces (each carries label spark-lab/tenant=true).
  kc delete namespace -l spark-lab/tenant=true --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kc delete namespace "$LAB_NAMESPACE" "$INGRESS_NAMESPACE" --ignore-not-found --wait=true
  log "Purged. Local credentials are kept in .lab/credentials.env (delete it to regenerate)."
  exit 0
fi

log "Stopping user Notebook servers (unsaved changes and running kernels are lost)"
# Notebooks, Drivers and Executors now live in per-user namespaces.
for ns in $(kc get ns -l spark-lab/tenant=true -o jsonpath='{.items[*].metadata.name}'); do
  kc -n "$ns" delete pods --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
done

log "Scaling lab Deployments to 0"
kc -n "$LAB_NAMESPACE" scale deployment --all --replicas=0 >/dev/null
kc -n "$INGRESS_NAMESPACE" scale deployment --all --replicas=0 >/dev/null 2>&1 || true

kc -n "$LAB_NAMESPACE" wait --for=delete pod --all --timeout=120s >/dev/null 2>&1 || true
kc -n "$INGRESS_NAMESPACE" wait --for=delete pod --all --timeout=60s >/dev/null 2>&1 || true

log "Stopped. Remaining PVCs (data kept):"
kc -n "$LAB_NAMESPACE" get pvc
