#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
ENV_EXAMPLE="${REPO_ROOT}/.env.example"
ENV_FILE="${REPO_ROOT}/.env"

usage() {
  cat <<'EOF'
Usage: bash ./scripts/setup-env.sh [--force]

Create .env from .env.example, generate unique local secrets, and generate
the RSA private key used by Auth Service.

Options:
  --force  Replace an existing .env file.
  -h, --help  Show this help message.
EOF
}

force=false
case "${1:-}" in
  "") ;;
  --force) force=true ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

if [[ ! -f "${ENV_EXAMPLE}" ]]; then
  echo "error: ${ENV_EXAMPLE} does not exist" >&2
  exit 1
fi

if [[ -e "${ENV_FILE}" && "${force}" != true ]]; then
  echo "error: ${ENV_FILE} already exists" >&2
  echo "Run with --force only if the existing local configuration may be replaced." >&2
  exit 1
fi

for command_name in openssl base64 tr sed grep mktemp; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "error: required command '${command_name}' was not found" >&2
    exit 1
  fi
done

TEMP_ENV="$(mktemp "${REPO_ROOT}/.env.tmp.XXXXXX")"
trap 'rm -f "${TEMP_ENV}"' EXIT
cp "${ENV_EXAMPLE}" "${TEMP_ENV}"
sed -i 's/\r$//' "${TEMP_ENV}"

replace_secret() {
  local placeholder="$1"
  local value
  value="$(openssl rand -hex 24)"
  sed -i "s|${placeholder}|${value}|g" "${TEMP_ENV}"
}

for placeholder in \
  replace-with-bmkg-api-key \
  replace-with-pvmbg-token \
  replace-with-aggregator-api-key \
  replace-with-postgres-admin-password \
  replace-with-aggregator-app-password \
  replace-with-auth-postgres-password \
  replace-with-unique-media-secret \
  replace-with-unique-field-secret \
  replace-with-unique-ops-secret
do
  replace_secret "${placeholder}"
done

JWT_KEY="$(openssl genpkey \
  -algorithm RSA \
  -pkeyopt rsa_keygen_bits:2048 2>/dev/null \
  | base64 \
  | tr -d '\n')"
if [[ -z "${JWT_KEY}" ]]; then
  echo "error: JWT key generator returned an empty value" >&2
  exit 1
fi
sed -i "s|replace-with-base64-pem-rsa-private-key|${JWT_KEY}|g" "${TEMP_ENV}"
unset JWT_KEY

if grep -q 'replace-with-' "${TEMP_ENV}"; then
  echo "error: one or more placeholders were not replaced" >&2
  exit 1
fi

mv -f "${TEMP_ENV}" "${ENV_FILE}"
trap - EXIT
chmod 600 "${ENV_FILE}" 2>/dev/null || true

echo "Created ${ENV_FILE} with unique local secrets and a generated JWT key."
echo "Review the file, then run: docker compose config --quiet"
