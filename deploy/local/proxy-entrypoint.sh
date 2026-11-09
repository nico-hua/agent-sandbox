#!/bin/sh
# Render a private token-bearing configuration and fail closed without diagnostics.
set -eu
# Report a fixed failure without printing token-bearing configuration or diagnostics.
fail() {
    echo 'proxy authentication configuration failed' >&2
    exit 1
}
[ "${SANDBOX_PROXY_AUTH_VERSION:-}" = 1 ] || fail
SANDBOX_ACCESS_TOKEN=${SANDBOX_ACCESS_TOKEN:-}
[ "${#SANDBOX_ACCESS_TOKEN}" = 43 ] 2>/dev/null || fail
case "${SANDBOX_ACCESS_TOKEN:-}" in *[!A-Za-z0-9_-]*|'') fail ;; esac

# Accept only four canonical decimal octets, before any configuration is rendered.
validate_ipv4() {
    case "$1" in ''|*[!0-9.]*|.*|*.|*..*) fail ;; esac
    saved_ifs=$IFS
    IFS=.
    set -- $1
    IFS=$saved_ifs
    [ "$#" = 4 ] || fail
    for octet do
        case "$octet" in 0|[1-9]|[1-9][0-9]|[1-9][0-9][0-9]) ;; *) fail ;; esac
        [ "$octet" -le 255 ] || fail
    done
}

# All template substitutions are derived here; ambient internal values are ignored.
PROXY_AGENT_AUTHORITY=sandbox:8080
PROXY_SERVICE_HOST=sandbox
PROXY_RESOLVER='resolver 127.0.0.11 valid=5s ipv6=off;'
PROXY_SERVICES_GUARD=''
PROXY_CONNECT_TIMEOUT=''
if [ "${SANDBOX_AGENT_UPSTREAM_IPV4+x}" = x ]; then
    validate_ipv4 "$SANDBOX_AGENT_UPSTREAM_IPV4"
    PROXY_AGENT_AUTHORITY="$SANDBOX_AGENT_UPSTREAM_IPV4:8080"
    PROXY_SERVICE_HOST=$SANDBOX_AGENT_UPSTREAM_IPV4
    PROXY_RESOLVER=''
    # The Kubernetes Agent Service exposes only 8080; never route arbitrary ports.
    PROXY_SERVICES_GUARD='return 400;'
    # A denied ClusterIP connection must fail promptly rather than wait 60 seconds.
    PROXY_CONNECT_TIMEOUT='proxy_connect_timeout 2s;'
fi
export SANDBOX_ACCESS_TOKEN PROXY_AGENT_AUTHORITY PROXY_SERVICE_HOST PROXY_RESOLVER PROXY_SERVICES_GUARD PROXY_CONNECT_TIMEOUT
umask 077
config_dir=$(mktemp -d /tmp/proxy-auth.XXXXXX) || fail
chmod 700 "$config_dir" || fail
config="$config_dir/nginx.conf"
envsubst '${SANDBOX_ACCESS_TOKEN} ${PROXY_AGENT_AUTHORITY} ${PROXY_SERVICE_HOST} ${PROXY_RESOLVER} ${PROXY_SERVICES_GUARD} ${PROXY_CONNECT_TIMEOUT}' < /etc/nginx/proxy.conf.template > "$config" 2>/dev/null || fail
chmod 600 "$config" || fail
unset SANDBOX_ACCESS_TOKEN
nginx -t -q -c "$config" >/dev/null 2>&1 || fail
exec nginx -c "$config" -g 'daemon off;'
