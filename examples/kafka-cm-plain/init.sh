#!/usr/bin/env bash

# Stop if not interactive mode
[[ $- != *i* ]] && echo "Usage: source init.sh" && exit 1

TEST_DIR="$PWD/test"; mkdir -p "$TEST_DIR"
declare -A server0=([id]="0" [rep]="2181" [jmx]="7000" [fol]="5000" [oth]="6000")
declare -A server1=([id]="1" [rep]="6001" [jmx]="7001" [cli]="9091")
declare -A server2=([id]="2" [rep]="6002" [jmx]="7002" [cli]="9092")
declare -A server3=([id]="3" [rep]="6003" [jmx]="7003")
declare -A server4=([id]="4" [rep]="6004" [jmx]="7004" [cli]="9094")
declare -A server5=([id]="5" [rep]="6005" [jmx]="7005" [cli]="9095")
SRC_BOOTSTRAP="localhost:${server1[cli]},localhost:${server2[cli]}"
SRC_HOME="$TEST_DIR/kafka-src"
DST_BOOTSTRAP="localhost:${server4[cli]},localhost:${server5[cli]}"
DST_HOME="$TEST_DIR/kafka-dst"

get-kafka() {
  local repo="$1" rev="$2" dir="$3"
  echo "Getting Kafka from $repo with revision $rev into $dir"
  git clone --depth 1 -b "$rev" "$repo" "$dir"
  cd "$dir"
  ./gradlew jar \
    -x test \
    -x compileTestJava \
    -x compileTestScala \
    -x checkstyleMain \
    -x checkstyleTest \
    -x spotlessCheck \
    -x trogdor:compileJava \
    -x jmh-benchmarks:compileJava
  cd -
}

build-con-boot() {
  local start="$1" end="$2"
  local boot=""
  for i in $(seq "$start" "$end"); do
    declare -n ref="server$i"
    boot+="${boot:+,}localhost:${ref[rep]}"
  done
  echo "$boot"
}

wait-for-meta-sync() {
  local bootstrap="$1" cmd_config="${2-}"
  local cmd_opts=""
  [[ -n "$cmd_config" ]] && cmd_opts="--command-config $cmd_config"
  local interval_ms
  interval_ms=$("$DST_HOME"/bin/kafka-configs.sh --bootstrap-server "$bootstrap" --entity-type brokers --all --describe $cmd_opts 2>/dev/null \
    | grep -oP 'mirror.metadata.refresh.interval.ms=\K[0-9]+' | head -1 || echo 30000)
  local interval_s=$(( interval_ms / 1000 ))
  echo "Waiting ${interval_s}s for metadata sync"
  sleep "$interval_s"
}

wait-for-state() {
  local bootstrap="$1" state="$2" mirror_re="${3-.*}" topic_re="${4-.*}" cmd_config="${5-}"
  local cmd_opts=""
  [[ -n "$cmd_config" ]] && cmd_opts="--command-config $cmd_config"
  echo "Waiting for state $state (mirror: $mirror_re, topic: $topic_re)"
  for i in $(seq 1 30); do
    "$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$bootstrap" --describe --json $cmd_opts 2>/dev/null | \
      jq -e --arg s "$state" --arg m "$mirror_re" --arg t "$topic_re" \
        '[.[] | select((.mirror | test($m)) and (.topic | test($t)))] | length > 0 and all(.state == $s)' &>/dev/null && return 0
    sleep 2
  done
  return 1
}

wait-for-lag-zero() {
  local bootstrap="$1" mirror_re="${2-.*}" topic_re="${3-.*}" cmd_config="${4-}"
  local cmd_opts=""
  [[ -n "$cmd_config" ]] && cmd_opts="--command-config $cmd_config"
  echo "Waiting for lag zero (mirror: $mirror_re, topic: $topic_re)"
  for i in $(seq 1 30); do
    "$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$bootstrap" --describe --json $cmd_opts 2>/dev/null | \
      jq -e --arg m "$mirror_re" --arg t "$topic_re" \
        '[.[] | select((.mirror | test($m)) and (.topic | test($t)))] | length > 0 and all(.state == "MIRRORING" and .lag == 0)' &>/dev/null && return 0
    sleep 2
  done
  return 1
}

# Source cluster ZK server (uses downloaded binary)
start-zserver() {
  local id="$1" SRC_HOME="$2"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/data
  cp "$SRC_HOME"/config/log4j.properties "$TEST_DIR"/server"${ref[id]}"/config/log4j.properties

  echo "${ref[id]}" >"$TEST_DIR"/server"${ref[id]}"/data/myid
  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
dataDir=$TEST_DIR/server${ref[id]}/data
clientPort=${ref[rep]}
maxClientCnxns=0
admin.enableServer=false
initLimit=10
syncLimit=5
server.${server0[id]}=localhost:${server0[fol]}:${server0[oth]}
EOF

  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$TEST_DIR/server${ref[id]}/config/log4j.properties" \
    "$SRC_HOME"/bin/zookeeper-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

# Source cluster ZK broker (uses downloaded binary)
start-broker-zk() {
  local id="$1" zconn="$2" SRC_HOME="$3"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR/server${ref[id]}/config" "$TEST_DIR/server${ref[id]}/logs" "$TEST_DIR/server${ref[id]}/data"
  cp "$SRC_HOME"/config/log4j.properties "$TEST_DIR/server${ref[id]}/config"

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
broker.id=${ref[id]}
log.dirs=$TEST_DIR/server${ref[id]}/data
zookeeper.connect=$zconn
zookeeper.connection.timeout.ms=18000
listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}
advertised.listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}
listener.security.protocol.map=REPLICATION:PLAINTEXT,BROKER:PLAINTEXT
inter.broker.listener.name=REPLICATION
default.replication.factor=2
min.insync.replicas=1
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
EOF

  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$TEST_DIR/server${ref[id]}/config/log4j.properties" \
    "$SRC_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

# Destination cluster controller (uses local build)
start-controller() {
  local id="$1" cid="$2" boot="$3"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/data
  cp "$DST_HOME"/config/log4j2.yaml "$TEST_DIR"/server"${ref[id]}"/config/log4j2.yaml

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
node.id=${ref[id]}
process.roles=controller
log.dirs=$TEST_DIR/server${ref[id]}/data
controller.quorum.bootstrap.servers=$boot
listeners=CONTROLLER://localhost:${ref[rep]}
advertised.listeners=CONTROLLER://localhost:${ref[rep]}
listener.security.protocol.map=CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
num.partitions=5
default.replication.factor=2
min.insync.replicas=1
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=2
share.coordinator.state.topic.min.isr=1
mirror.state.topic.replication.factor=2
mirror.state.topic.min.isr=1
unstable.api.versions.enable=true
unstable.feature.versions.enable=true
EOF

  "$DST_HOME"/bin/kafka-storage.sh format -t "$cid" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties -s
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server"${ref[id]}"/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server"${ref[id]}"/config/log4j2.yaml" \
    "$DST_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

# Destination cluster broker (uses local build)
start-broker() {
  local id="$1" cid="$2" boot="$3"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/data
  cp "$DST_HOME"/config/log4j2.yaml "$TEST_DIR"/server"${ref[id]}"/config/log4j2.yaml

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
node.id=${ref[id]}
process.roles=broker
log.dirs=$TEST_DIR/server${ref[id]}/data
controller.quorum.bootstrap.servers=$boot
auto.create.topics.enable=false
mirror.coordinator.threads=3
mirror.num.replica.fetchers=3
mirror.metadata.refresh.interval.ms=5000
listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}
advertised.listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,REPLICATION:PLAINTEXT,BROKER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=REPLICATION
num.partitions=5
default.replication.factor=2
min.insync.replicas=1
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=2
share.coordinator.state.topic.min.isr=1
mirror.state.topic.replication.factor=2
mirror.state.topic.min.isr=1
unstable.api.versions.enable=true
unstable.feature.versions.enable=true
EOF

  "$DST_HOME"/bin/kafka-storage.sh format -t "$cid" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server"${ref[id]}"/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server"${ref[id]}"/config/log4j2.yaml" \
    "$DST_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

test-teardown() {
  echo "Killing all running procs"
  pkill -SIGKILL -ef "kafka.tools" ||true
  pkill -SIGKILL -ef "kafka.Kafka" ||true
  pkill -SIGKILL -ef "quorum.QuorumPeerMain" ||true
}

cleanup() {
  read -p "Are you sure you want to delete $TEST_DIR? (yes/no): " confirm
  if [[ "$confirm" == "yes" ]]; then
    rm -rf "$TEST_DIR"
    echo "Cleanup complete"
  else
    echo "Cleanup cancelled"
  fi
}

test-setup() {
  test-teardown
  cleanup
  get-kafka https://github.com/apache/kafka.git 3.9.2 "$SRC_HOME"
  get-kafka https://github.com/showuon/kafka.git cm-20261027 "$DST_HOME"

  echo "Starting Kafka 3.9.2 (ZK) as source cluster"
  cd "$SRC_HOME"
  start-zserver 0 "$SRC_HOME"
  for i in 1 2; do start-broker-zk "$i" "localhost:${server0[rep]}" "$SRC_HOME"; done
  sleep 2
  bin/zookeeper-shell.sh localhost:2181 <<< "ls /brokers/ids" | tail -n1
  cd -

  echo "Starting POC Kafka (KRaft) as destination cluster"
  local dst_cid="$("$DST_HOME"/bin/kafka-storage.sh random-uuid)" dst_boot="$(build-con-boot 3 3)"
  start-controller 3 "$dst_cid" "$dst_boot"
  for i in 4 5; do start-broker "$i" "$dst_cid" "$dst_boot"; done
  sleep 2
  "$DST_HOME"/bin/kafka-metadata-quorum.sh --bootstrap-server "$DST_BOOTSTRAP" describe --re --hu

  "$DST_HOME"/bin/kafka-features.sh --bootstrap-server "$DST_BOOTSTRAP" upgrade --feature mirror.version=1

  sleep 2
}

test-setup
