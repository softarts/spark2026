# Helper functions shared by the lab scripts. Source, do not execute.
# shellcheck shell=bash

LAB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lab.env
source "$LAB_ROOT/scripts/lab.env"

RUNTIME_IMAGE="${RUNTIME_NAME}:${RUNTIME_TAG}"
CRED_FILE="$LAB_ROOT/.lab/credentials.env"

kc() { kubectl --context "$LAB_CONTEXT" "$@"; }
hm() { helm --kube-context "$LAB_CONTEXT" "$@"; }

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mWARN:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

require_tools() {
  local t
  for t in "$@"; do
    command -v "$t" >/dev/null 2>&1 || die "'$t' not found in PATH"
  done
}

# Start Docker Desktop if needed and wait for its Kubernetes API.
ensure_cluster() {
  if ! docker info >/dev/null 2>&1; then
    log "Docker Desktop is not running; starting it"
    open -a Docker || die "could not start Docker Desktop"
  fi
  local i
  for i in $(seq 1 60); do
    if docker info >/dev/null 2>&1 && kc get --raw /readyz >/dev/null 2>&1; then
      break
    fi
    [ "$i" -eq 60 ] && die "Kubernetes context '$LAB_CONTEXT' is not reachable. Enable Kubernetes in Docker Desktop > Settings > Kubernetes."
    sleep 5
  done
  kc wait --for=condition=Ready node --all --timeout=120s >/dev/null \
    || die "Kubernetes node is not Ready"
}

HASH_LABEL=org.sparklab.inputs-hash

# Hash of everything that goes into the runtime image.
inputs_hash() {
  (cd "$LAB_ROOT" && find images jobs notebooks -type f \
      ! -path '*/__pycache__/*' ! -path '*/.ipynb_checkpoints/*' ! -name .DS_Store \
    | LC_ALL=C sort | xargs shasum -a 256 | shasum -a 256 | cut -c1-16)
}

# Docker Desktop's kind-based Kubernetes runs its own containerd: images built
# with `docker build` are NOT visible to Pods until imported into the node.
# A short-lived privileged Pod enters the node's mount namespace and runs
# `ctr images import`; `crictl` is used to read back the imported image label.
LOADER_POD=spark-lab-image-loader
LOADER_IMAGE=nginxinc/nginx-unprivileged:1.29-alpine@sha256:0c79d56aee561a1d81c63f00eee5fb5fe29279560cdc55e91425133104c7fbe6

start_loader() {
  kc -n kube-system delete pod "$LOADER_POD" --ignore-not-found --now >/dev/null
  kc apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $LOADER_POD
  namespace: kube-system
  labels: {app.kubernetes.io/part-of: spark-platform-learning-lab}
spec:
  hostPID: true
  restartPolicy: Never
  tolerations: [{operator: Exists}]
  containers:
    - name: loader
      image: $LOADER_IMAGE
      imagePullPolicy: IfNotPresent
      securityContext: {privileged: true, runAsUser: 0}
      command: ["sleep", "900"]
EOF
  kc -n kube-system wait --for=condition=Ready "pod/$LOADER_POD" --timeout=180s >/dev/null
}
stop_loader() { kc -n kube-system delete pod "$LOADER_POD" --ignore-not-found --now >/dev/null 2>&1 || true; }
on_node() { kc -n kube-system exec -i "$LOADER_POD" -- nsenter -t 1 -m -- "$@"; }

# Usage: sync_image_to_node <image> <expected-hash>
sync_image_to_node() {
  local image="$1" want="$2" have
  start_loader
  have=$(on_node crictl inspecti -o go-template \
          --template "{{index .info.imageSpec.config.Labels \"$HASH_LABEL\"}}" \
          "docker.io/library/$image" 2>/dev/null || true)
  if [ "$have" = "$want" ]; then
    log "    node already has $image (inputs $want)"
  else
    log "    importing $image into the Kubernetes node (about 20 s)"
    docker save --platform linux/arm64 "$image" \
      | on_node ctr -n k8s.io images import --platform linux/arm64 - >/dev/null
  fi
  stop_loader
}

lan_ips() {
  local ifc ip
  for ifc in en0 en1; do
    ip=$(ipconfig getifaddr "$ifc" 2>/dev/null) && [ -n "$ip" ] && echo "$ip"
  done
}
