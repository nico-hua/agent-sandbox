#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
cd "$script_dir"
public_ip=${1:-1.1.1.1}
smoke_port=${SMOKE_PORT:-18081}
base_url="http://127.0.0.1:$smoke_port"

docker compose -f compose.yml config --quiet
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
host_proxy_hash=$(sha256sum proxy.conf | cut -d ' ' -f1)
container_proxy_hash=$(docker exec agent-sandbox-proxy-dev sha256sum /etc/nginx/nginx.conf | cut -d ' ' -f1)
if [ "$host_proxy_hash" != "$container_proxy_hash" ]; then
  echo 'proxy config is stale; recreate the proxy with docker compose -f compose.yml up -d --force-recreate --no-deps proxy' >&2
  exit 1
fi

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

file_tag=$(od -An -N16 -tx1 /dev/urandom | tr -d '[:space:]')
[ -n "$file_tag" ]
file_input="smoke-$file_tag-input.txt"
file_output="smoke-$file_tag-output.txt"
file_input_created=no
file_output_created=no

sse_output=
sse_pid=

# cleanup_smoke removes only artifacts created by this smoke invocation.
cleanup_smoke() {
  if [ -n "$sse_pid" ]; then
    kill "$sse_pid" 2>/dev/null || true
    wait "$sse_pid" 2>/dev/null || true
  fi
  if [ -n "$sse_output" ]; then
    rm -f -- "$sse_output"
  fi
  if [ "$file_input_created" = yes ]; then
    docker exec agent-sandbox-dev rm -- "/workspace/$file_input" >/dev/null || printf 'could not clean up %s\n' "$file_input" >&2
  fi
  if [ "$file_output_created" = yes ]; then
    docker exec agent-sandbox-dev rm -- "/workspace/$file_output" >/dev/null || printf 'could not clean up %s\n' "$file_output" >&2
  fi
}
trap cleanup_smoke EXIT

upload_code=$(printf hello | curl --noproxy 127.0.0.1 -sS --max-time 10 \
  -o /dev/null -w '%{http_code}' --data-binary @- "$base_url/v1/files?path=$file_input")
[ "$upload_code" = 201 ]
file_input_created=yes
file_command_result=$(curl --noproxy 127.0.0.1 -fsS --max-time 10 \
  -H 'Content-Type: application/json' \
  -d "{\"argv\":[\"sh\",\"-c\",\"set -C; tr a-z A-Z < $file_input > $file_output\"],\"cwd\":\"/workspace\"}" \
  "$base_url/v1/commands:run")
case "$file_command_result" in
  *'"exit_code":0,"stdout":"","stderr":""'*) file_output_created=yes ;;
  *) echo "unexpected file command response: $file_command_result" >&2; exit 1 ;;
esac
downloaded=$(curl --noproxy 127.0.0.1 -fsS --max-time 10 "$base_url/v1/files?path=$file_output")
[ "$downloaded" = HELLO ]

sse_output=$(mktemp /tmp/agent-sandbox-sse.XXXXXX)
curl -N --noproxy 127.0.0.1 -fsS --max-time 8 \
  -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf first; sleep 3; printf second"]}' \
  "$base_url/v1/commands:stream" > "$sse_output" &
sse_pid=$!
sse_seen=no
attempt=0
while [ "$attempt" -lt 40 ]; do
  if grep -Fq '"data_base64":"Zmlyc3Q="' "$sse_output"; then
    sse_seen=yes
    break
  fi
  if ! kill -0 "$sse_pid" 2>/dev/null; then
    echo 'SSE curl ended before the first output frame arrived' >&2
    exit 1
  fi
  sleep 0.05
  attempt=$((attempt + 1))
done
if [ "$sse_seen" != yes ] || ! kill -0 "$sse_pid" 2>/dev/null; then
  echo 'first SSE frame did not arrive before the command finished' >&2
  exit 1
fi
wait "$sse_pid"
sse_pid=
grep -Fq '"data_base64":"c2Vjb25k"' "$sse_output"
grep -Fq 'event: complete' "$sse_output"

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
