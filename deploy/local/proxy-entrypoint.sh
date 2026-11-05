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
umask 077
config_dir=$(mktemp -d /tmp/proxy-auth.XXXXXX) || fail
chmod 700 "$config_dir" || fail
config="$config_dir/nginx.conf"
envsubst '${SANDBOX_ACCESS_TOKEN}' < /etc/nginx/proxy.conf.template > "$config" || fail
chmod 600 "$config" || fail
unset SANDBOX_ACCESS_TOKEN
nginx -t -q -c "$config" >/dev/null 2>&1 || fail
exec nginx -c "$config" -g 'daemon off;'
