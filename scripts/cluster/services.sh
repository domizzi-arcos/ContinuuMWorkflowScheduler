#!/usr/bin/env bash
# Start or stop a private Redis + RabbitMQ pair from Apptainer images, without
# root, Docker or daemons, on ports derived from PORT_BASE so several jobs can
# share a node. Processes are plain background children of the calling shell
# (inside a Slurm job they end with the job).
#
#   source scripts/cluster/services.sh
#   services_start  PORT_BASE STATE_DIR   # exports REDIS_ADDR and AMQP_URL
#   services_stop   STATE_DIR
#
# Ports: Redis PORT_BASE, AMQP PORT_BASE+1, Erlang distribution PORT_BASE+2,
# epmd PORT_BASE+3. Images: $SIF_DIR/redis.sif and $SIF_DIR/rabbitmq.sif
# (see scripts/cluster/README.md for `apptainer pull`).

SIF_DIR=${SIF_DIR:-$HOME/sif}

_port_free() {
  ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

_wait_port() {  # port timeout_s
  local t=0
  until ! _port_free "$1"; do
    sleep 1
    t=$((t + 1))
    if [ "$t" -ge "$2" ]; then return 1; fi
  done
}

services_start() {
  local base=${1:?PORT_BASE} dir=${2:?STATE_DIR}
  local redis_port=$base amqp_port=$((base + 1)) dist_port=$((base + 2)) epmd_port=$((base + 3))
  for p in $redis_port $amqp_port $dist_port $epmd_port; do
    if ! _port_free "$p"; then
      echo "services: port $p is in use; choose another PORT_BASE" >&2
      return 1
    fi
  done
  mkdir -p "$dir/redis" "$dir/rabbitmq/mnesia" "$dir/rabbitmq/log" "$dir/rabbitmq/home"

  # --pid gives each service its own PID namespace: stopping the main
  # process also ends its children (RabbitMQ's epmd).
  apptainer exec --cleanenv --containall --pid --bind "$dir/redis:/data" "$SIF_DIR/redis.sif" \
    redis-server --bind 127.0.0.1 --port "$redis_port" --save "" --appendonly no \
    > "$dir/redis.log" 2>&1 &
  echo $! > "$dir/redis.pid"

  local node="rabbit_${base}@localhost"
  apptainer exec --cleanenv --containall --pid \
    --bind "$dir/rabbitmq/home:/var/lib/rabbitmq" \
    --env HOME=/var/lib/rabbitmq \
    --env RABBITMQ_NODENAME="$node" \
    --env RABBITMQ_NODE_IP_ADDRESS=127.0.0.1 \
    --env RABBITMQ_NODE_PORT="$amqp_port" \
    --env RABBITMQ_DIST_PORT="$dist_port" \
    --env ERL_EPMD_PORT="$epmd_port" \
    --env ERL_EPMD_ADDRESS=127.0.0.1 \
    --env RABBITMQ_MNESIA_BASE=/var/lib/rabbitmq/mnesia \
    --env RABBITMQ_LOG_BASE=/var/lib/rabbitmq/log \
    --env RABBITMQ_LOGS=- \
    "$SIF_DIR/rabbitmq.sif" rabbitmq-server > "$dir/rabbitmq.log" 2>&1 &
  echo $! > "$dir/rabbitmq.pid"

  if ! _wait_port "$redis_port" 60; then
    echo "services: Redis did not open port $redis_port (see $dir/redis.log)" >&2
    return 1
  fi
  if ! _wait_port "$amqp_port" 120; then
    echo "services: RabbitMQ did not open port $amqp_port (see $dir/rabbitmq.log)" >&2
    return 1
  fi
  export REDIS_ADDR="localhost:$redis_port"
  export AMQP_URL="amqp://guest:guest@localhost:$amqp_port/"
  echo "services: Redis $REDIS_ADDR, RabbitMQ $AMQP_URL"
}

services_stop() {
  local dir=${1:?STATE_DIR}
  for svc in rabbitmq redis; do
    if [ -f "$dir/$svc.pid" ]; then
      kill "$(cat "$dir/$svc.pid")" 2>/dev/null || true
      rm -f "$dir/$svc.pid"
    fi
  done
  wait 2>/dev/null || true
}
