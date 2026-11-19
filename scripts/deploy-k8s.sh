#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE=sistem-informasi-kebencanaan-bnpb
env_file="${REPO_ROOT}/.env"
image_prefix=aat
image_tag=m2-dev
tag_supplied=false
build=false
push=false
context=""
target=""
storage_class=""
timeout=300s

usage() {
  cat <<'EOF'
Usage: bash scripts/deploy-k8s.sh [options]

Deploy the M2 application to an already configured Kubernetes cluster.
If .env does not exist, setup-env.sh creates it without replacing existing secrets.

  --minikube                 Build and load local images, then deploy to Minikube
  --k3s                      Deploy pre-imported images to the active k3s context
  --build                    Build applications and the observability image
  --push                     Push application images to --image-prefix
  --context CONTEXT          Use a specific kubeconfig context
  --image-prefix PREFIX      Image prefix, e.g. docker.io/your-user (default: aat)
  --image-tag TAG            Image tag (default: timestamp with --build, else m2-dev)
  --storage-class NAME       Node-local StorageClass (default: cluster default)
  --env-file PATH            Credentials/configuration file (default: .env)
  --timeout DURATION         Wait timeout per workload (default: 300s)
  -h, --help                 Show help

Examples:
  bash scripts/deploy-k8s.sh --minikube
  bash scripts/deploy-k8s.sh --k3s
  bash scripts/deploy-k8s.sh --build --push --image-prefix docker.io/your-user
EOF
}

while (( $# )); do
  case "$1" in
    --minikube|--k3s)
      selected_target="${1#--}"
      if [[ -n "${target}" && "${target}" != "${selected_target}" ]]; then
        echo 'error: choose either --minikube or --k3s' >&2; exit 2
      fi
      target="${selected_target}"; shift ;;
    --build) build=true; shift ;;
    --push) push=true; shift ;;
    --context|--image-prefix|--image-tag|--storage-class|--env-file|--timeout)
      if (( $# < 2 )) || [[ -z "$2" || "$2" == --* ]]; then
        echo "error: $1 requires a value" >&2; exit 2
      fi
      case "$1" in
        --context) context="$2" ;;
        --image-prefix) image_prefix="${2%/}" ;;
        --image-tag) image_tag="$2"; tag_supplied=true ;;
        --storage-class) storage_class="$2" ;;
        --env-file) env_file="$2" ;;
        --timeout) timeout="$2" ;;
      esac
      shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown option $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "${target}" == minikube ]]; then
  if [[ "${push}" == true ]]; then
    echo 'error: --minikube loads local images and cannot be combined with --push' >&2; exit 2
  fi
  context="${context:-minikube}"
  build=true
  for executable in minikube docker; do
    command -v "${executable}" >/dev/null || { echo "error: ${executable} is required for --minikube" >&2; exit 1; }
  done
fi

for executable in kubectl python3; do
  command -v "${executable}" >/dev/null || { echo "error: ${executable} is required" >&2; exit 1; }
done
if [[ ! -f "${env_file}" ]]; then
  if [[ "${env_file}" == "${REPO_ROOT}/.env" ]]; then
    bash "${SCRIPT_DIR}/setup-env.sh"
  else
    echo "error: env file does not exist" >&2; exit 1
  fi
fi
if [[ "${build}" == true && "${tag_supplied}" == false ]]; then
  image_tag="m2-$(date -u +%Y%m%d%H%M%S)"
fi

kube=(kubectl)
[[ -z "${context}" ]] || kube+=(--context "${context}")
render_dir="$(mktemp -d "${REPO_ROOT}/k8s/.render.XXXXXX")"
trap 'rm -rf -- "${render_dir}"' EXIT

# p1: validate cluster storage before applying resources.
"${kube[@]}" get nodes --request-timeout=10s
"${kube[@]}" get storageclasses -o json > "${render_dir}/storageclasses.json"
storage_class="$(python3 - "${render_dir}/storageclasses.json" "${storage_class}" <<'PY'
import json
import sys
classes = json.load(open(sys.argv[1]))['items']
name = sys.argv[2]
if name:
    selected = [item for item in classes if item['metadata']['name'] == name]
else:
    selected = [item for item in classes if any(
        item['metadata'].get('annotations', {}).get(key) == 'true' for key in (
            'storageclass.kubernetes.io/is-default-class',
            'storageclass.beta.kubernetes.io/is-default-class'))]
if len(selected) != 1:
    sys.exit('error: select an existing node-local StorageClass with --storage-class')
item = selected[0]
if item['provisioner'] not in ('rancher.io/local-path', 'k8s.io/minikube-hostpath', 'kubernetes.io/no-provisioner'):
    sys.exit('error: this migration requires node-local storage, not a network StorageClass')
print(item['metadata']['name'])
PY
)"
"${kube[@]}" -n "${NAMESPACE}" get secrets postgres-credentials auth-postgres-credentials aggregator-coordinator-credentials \
  --ignore-not-found -o json > "${render_dir}/existing-secrets.json"
"${kube[@]}" -n "${NAMESPACE}" get configmap postgres-config \
  --ignore-not-found -o json > "${render_dir}/existing-postgres-config.json"
render_args=(--env-file "${env_file}" --output-dir "${render_dir}"
             --image-prefix "${image_prefix}" --image-tag "${image_tag}")
[[ -z "${storage_class}" ]] || render_args+=(--storage-class "${storage_class}")
render_args+=(--existing-secrets "${render_dir}/existing-secrets.json"
              --existing-postgres-config "${render_dir}/existing-postgres-config.json")
python3 "${SCRIPT_DIR}/k8s_config.py" "${render_args[@]}"
for stage in platform jobs apps; do
  "${kube[@]}" kustomize "${render_dir}/${stage}" > "${render_dir}/${stage}.yaml"
done

services=(aggregator auth bmkg client-api dashboard-consumer notifier-consumer pvmbg observability)
if [[ "${build}" == true ]]; then
  command -v docker >/dev/null || { echo 'error: Docker is required for --build' >&2; exit 1; }
  for service in "${services[@]}"; do
    docker build -t "${image_prefix}/${service}:${image_tag}" "${REPO_ROOT}/services/${service}"
  done
fi
if [[ "${push}" == true ]]; then
  for service in "${services[@]}"; do
    docker push "${image_prefix}/${service}:${image_tag}"
  done
fi

if [[ "${target}" == minikube ]]; then
  # Docker's image store is separate from the nodes' container runtimes.
  for service in "${services[@]}"; do
    minikube -p "${context}" image load --daemon "${image_prefix}/${service}:${image_tag}"
  done
fi

# Dependencies start first, so application startup cannot race topic creation.
"${kube[@]}" apply -f "${render_dir}/platform.yaml"
# p1: initialize all three storage units before starting applications.
for workload in postgres-a postgres-b postgres-c aggregator-coordinator auth-postgres kafka; do
  "${kube[@]}" -n "${NAMESPACE}" rollout status "statefulset/${workload}" --timeout="${timeout}"
done
# Only the idempotent topic-provisioning Job is replaced, never PVCs or data.
"${kube[@]}" -n "${NAMESPACE}" delete job kafka-init --ignore-not-found --wait=true --timeout=60s
"${kube[@]}" apply -f "${render_dir}/jobs.yaml"
"${kube[@]}" -n "${NAMESPACE}" wait --for=condition=complete job/kafka-init --timeout="${timeout}"
"${kube[@]}" create --dry-run=client -f "${render_dir}/apps.yaml" -o json > "${render_dir}/apps.json"
python3 - "${render_dir}" <<'PY'
import json, pathlib, sys
directory = pathlib.Path(sys.argv[1])
remaining = (directory / 'apps.json').read_text().lstrip()
decoder = json.JSONDecoder()
items = []
while remaining:
    resource, end = decoder.raw_decode(remaining)
    items.extend(resource.get('items', [resource]))
    remaining = remaining[end:].lstrip()
sensor = [r for r in items if r['kind'] == 'Deployment' and r['metadata']['name'] == 'sensor-pantai']
others = [r for r in items if r not in sensor]
for name, group in [('core-apps', others), ('sensor-app', sensor)]:
    (directory / (name + '.json')).write_text(json.dumps({'apiVersion':'v1', 'kind':'List', 'items':group}))
PY
"${kube[@]}" apply -f "${render_dir}/core-apps.json"
for service in aggregator aggregator-gateway auth bmkg client-api pvmbg; do
  "${kube[@]}" -n "${NAMESPACE}" rollout status "deployment/${service}" --timeout="${timeout}"
done
for service in dashboard-consumer notifier-consumer; do
  "${kube[@]}" -n "${NAMESPACE}" rollout status "statefulset/${service}" --timeout="${timeout}"
done
# On first deployment, do not start the sensor's clock before receivers exist.
# Existing sensor pods are left running during upgrades (never scaled down).
"${kube[@]}" apply -f "${render_dir}/sensor-app.json"
"${kube[@]}" -n "${NAMESPACE}" rollout status deployment/sensor-pantai --timeout="${timeout}"
"${kube[@]}" -n "${NAMESPACE}" get pods -o wide
# p1
echo "Deployed M2 Kubernetes stack with image tag ${image_tag}."
