#!/usr/bin/env bash
# Starts or stops a single-node TiKV cluster (one PD, one TiKV) with `tiup playground`, listening
# on 127.0.0.1:2379. `start` leaves the cluster empty: it stops and wipes any earlier playground
# first, so a caller gets a keyspace that holds no data from a previous suite.
set -euo pipefail

tiup="${HOME}/.tiup/bin/tiup"
tag=testing

# `tiup clean` cannot remove a tagged playground's data directory, so it is deleted directly once
# every cluster process has exited.
stop() {
  killall -q tiup-playground || true
  for _ in $(seq 1 30); do
    pgrep -f 'tiup-playground|pd-server|tikv-server' > /dev/null || break
    sleep 1
  done
  pkill -9 -f 'pd-server|tikv-server' || true
  rm -rf "${HOME}/.tiup/data/${tag}"
}

start() {
  if [ ! -x "${tiup}" ]; then
    curl --proto '=https' --tlsv1.2 -sSf --connect-timeout 30 --max-time 120 https://tiup-mirrors.pingcap.com/install.sh | sh
  fi
  stop
  "${tiup}" install pd tikv playground
  nohup "${tiup}" playground --mode tikv-slim --kv 1 --pd 1 --db 0 --ticdc 0 --tiflash 0 --without-monitor --tag "${tag}" > /tmp/tiup.log 2>&1 &
  for _ in $(seq 1 24); do
    sleep 5
    if "${tiup}" playground display > /dev/null 2>&1; then
      return 0
    fi
  done
  echo "::error::the TiKV playground did not start"
  cat /tmp/tiup.log
  return 1
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  *) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac
