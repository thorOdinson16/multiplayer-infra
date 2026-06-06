#!/usr/bin/env bash
set -euo pipefail

required=(kubectl helm docker go curl)
missing=()

for bin in "${required[@]}"; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    missing+=("${bin}")
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  printf 'Missing required tools: %s\n' "${missing[*]}" >&2
  exit 1
fi

kubectl version --client >/dev/null
helm version --short >/dev/null
docker version >/dev/null
go version >/dev/null

printf 'All required local dependencies are available.\n'
