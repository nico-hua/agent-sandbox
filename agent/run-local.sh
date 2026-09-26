#!/usr/bin/env sh
set -eu

network_name=agent-sandbox-internal
workspace_volume=agent-sandbox-workspace

if ! docker network inspect "$network_name" >/dev/null 2>&1; then
  docker network create --driver bridge --internal "$network_name" >/dev/null
fi

network_settings=$(docker network inspect "$network_name" --format '{{.Driver}} {{.Internal}}')
if [ "$network_settings" != 'bridge true' ]; then
  printf 'network %s must be an internal bridge (got %s)\n' "$network_name" "$network_settings" >&2
  exit 1
fi

if ! docker volume inspect "$workspace_volume" >/dev/null 2>&1; then
  docker volume create "$workspace_volume" >/dev/null
fi

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
exec docker compose -f "$script_dir/compose.local.yml" up -d --build --wait --wait-timeout 30
