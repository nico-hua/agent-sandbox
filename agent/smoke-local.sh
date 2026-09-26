#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
cd "$script_dir"
public_ip=${1:-1.1.1.1}
smoke_port=${SMOKE_PORT:-18081}
base_url="http://127.0.0.1:$smoke_port"

docker compose -f compose.local.yml config --quiet
[ "$(docker network inspect agent-sandbox-internal --format '{{.Driver}} {{.Internal}}')" = 'bridge true' ]
[ "$(docker inspect agent-sandbox-dev --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}')" = 'agent-sandbox-internal' ]
[ "$(docker inspect agent-sandbox-dev --format '{{json .HostConfig.PortBindings}}')" = '{}' ]
[ "$(docker inspect agent-sandbox-dev --format '{{range .Mounts}}{{if eq .Destination "/workspace"}}{{.Name}}{{end}}{{end}}')" = 'agent-sandbox-workspace' ]
[ "$(docker inspect agent-sandbox-dev --format '{{len .NetworkSettings.Networks}}')" = '1' ]

[ "$(docker inspect agent-sandbox-proxy-dev --format '{{len .NetworkSettings.Networks}}')" = '2' ]
[ "$(docker inspect agent-sandbox-proxy-dev --format '{{if index .NetworkSettings.Networks "agent-sandbox-internal"}}yes{{end}}')" = 'yes' ]
[ "$(docker inspect agent-sandbox-proxy-dev --format '{{if index .NetworkSettings.Networks "agent-sandbox-local_ingress"}}yes{{end}}')" = 'yes' ]
[ "$(docker port agent-sandbox-proxy-dev 8080/tcp)" = "127.0.0.1:$smoke_port" ]
case "$(docker inspect agent-sandbox-proxy-dev --format '{{range .Mounts}}{{.Destination}} {{end}}')" in
  *'/workspace'*|*'/var/run/docker.sock'*) echo 'proxy has a forbidden mount' >&2; exit 1 ;;
esac

[ "$(docker inspect agent-sandbox-dev --format '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}} {{.HostConfig.NanoCpus}} {{.HostConfig.PidsLimit}} {{.HostConfig.ReadonlyRootfs}} {{.HostConfig.Init}} {{json .HostConfig.CapDrop}} {{json .HostConfig.SecurityOpt}} {{.Config.User}}')" = '268435456 268435456 1000000000 64 true true ["ALL"] ["no-new-privileges:true"] sandbox' ]

health=$(curl --noproxy 127.0.0.1 -fsS --max-time 5 "$base_url/healthz")
[ "$health" = '{"status":"ok"}' ]
command_result=$(curl --noproxy 127.0.0.1 -fsS --max-time 5 \
  -H 'Content-Type: application/json' \
  -d '{"argv":["pwd"]}' \
  "$base_url/v1/commands:run")
case "$command_result" in
  *'"exit_code":0,"stdout":"/workspace\n","stderr":""'*) ;;
  *) echo "unexpected command response: $command_result" >&2; exit 1 ;;
esac

docker exec agent-sandbox-dev sh -c 'command -v nc >/dev/null'
if ! docker run --rm --network bridge --entrypoint nc agent-sandbox:dev -n -z -w 3 "$public_ip" 80; then
  echo "ordinary bridge cannot reach $public_ip:80; egress isolation is unverified" >&2
  exit 1
fi
if docker exec agent-sandbox-dev nc -n -z -w 3 "$public_ip" 80; then
  echo "sandbox unexpectedly reached $public_ip:80" >&2
  exit 1
fi

echo 'local sandbox smoke checks passed'
