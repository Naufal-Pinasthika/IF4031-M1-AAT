#!/usr/bin/env bash
set -euo pipefail
REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
export KUBECONFIG="${REPO}/.e2e/kubeconfig"
cd "${REPO}"
tag="${IMAGE_TAG:-m2-e2e}"
if [[ -z "${IMAGE_TAG:-}" && -s "${REPO}/.e2e/image-tag" ]]; then tag="$(<"${REPO}/.e2e/image-tag")"; fi
bash scripts/m2/e2e/lab.sh images
bash scripts/deploy-k8s.sh --k3s --image-tag "${tag}" --timeout 600s
python3 - <<'PY' | kubectl apply -f -
import json
services=[]
for name,port,nodeport in [('aggregator',8083,30083),('client-api',8080,30080),('auth',8084,30084),('sensor-pantai',8080,30090),('notifier-consumer',8086,30086)]:
    services.append({'apiVersion':'v1','kind':'Service','metadata':{'name':name+'-e2e','namespace':'sistem-informasi-kebencanaan-bnpb'},'spec':{'type':'NodePort','selector':{'app':name},'ports':[{'name':'http','port':port,'targetPort':port,'nodePort':nodeport}]}})
print(json.dumps({'apiVersion':'v1','kind':'List','items':services}))
PY
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
kubectl -n tester-agent rollout status daemonset/tester-agent --timeout=300s
kubectl -n tester-agent exec daemonset/tester-agent -- /app list
