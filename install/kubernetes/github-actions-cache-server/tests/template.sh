#!/usr/bin/env bash

set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
default_render="$(mktemp)"
configured_render="$(mktemp)"
trap 'rm -f "$default_render" "$configured_render"' EXIT

helm lint "$chart_dir"
helm template cache-server "$chart_dir" >"$default_render"
helm template cache-server "$chart_dir" \
  --set persistentVolumeClaim.existingClaim=terraform-managed-cache \
  --set deploymentStrategy.type=Recreate \
  >"$configured_render"

grep -q '^kind: PersistentVolumeClaim$' "$default_render"
if grep -q '^  strategy:$' "$default_render"; then
  echo 'default render unexpectedly contains a Deployment strategy' >&2
  exit 1
fi

if grep -q '^kind: PersistentVolumeClaim$' "$configured_render"; then
  echo 'existingClaim render unexpectedly contains a PersistentVolumeClaim' >&2
  exit 1
fi
grep -q '^            claimName: terraform-managed-cache$' "$configured_render"
grep -q '^  strategy:$' "$configured_render"
grep -q '^    type: Recreate$' "$configured_render"

echo 'Helm template validation passed'
