#!/usr/bin/env bash

[[ $- != *i* ]] && echo "Usage: source init.sh" && exit 1

TEST_DIR="$PWD/test"; mkdir -p "$TEST_DIR"
declare -A server0=([id]="0" [rep]="6000" [jmx]="7000" [did]="")
declare -A server1=([id]="1" [rep]="6001" [jmx]="7001" [did]="")
declare -A server2=([id]="2" [rep]="6002" [jmx]="7002" [did]="")
declare -A server3=([id]="3" [rep]="6003" [jmx]="7003" [cli]="9092")
declare -A server4=([id]="4" [rep]="6004" [jmx]="7004" [cli]="9093")
declare -A server5=([id]="5" [rep]="6005" [jmx]="7005" [cli]="9094")
BOOTSTRAP="localhost:${server3[cli]},localhost:${server4[cli]},localhost:${server5[cli]}"
KAFKA_HOME="$TEST_DIR/kafka"

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

build-init-con() {
  local start="$1" end="$2"
  local init=""
  for i in $(seq "$start" "$end"); do
    declare -n ref="server$i"
    init+="${init:+,}${ref[id]}@localhost:${ref[rep]}:${ref[did]}"
  done
  echo "$init"
}

start-controller() {
  local id="$1" cid="$2" boot="$3" init="$4"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs \
    "$TEST_DIR"/server"${ref[id]}"/mdata "$TEST_DIR"/server"${ref[id]}"/data
  cp "$KAFKA_HOME"/config/log4j2.yaml "$TEST_DIR"/server"${ref[id]}"/config/log4j2.yaml

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
node.id=${ref[id]}
process.roles=controller
log.dirs=$TEST_DIR/server${ref[id]}/data
metadata.log.dir=$TEST_DIR/server${ref[id]}/mdata
controller.quorum.bootstrap.servers=$boot
listeners=CONTROLLER://localhost:${ref[rep]}
advertised.listeners=CONTROLLER://localhost:${ref[rep]}
listener.security.protocol.map=CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
num.partitions=5
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
share.coordinator.state.topic.replication.factor=3
share.coordinator.state.topic.min.isr=2
EOF

  "$KAFKA_HOME"/bin/kafka-storage.sh format -t "$cid" -I "$init" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server${ref[id]}/config/log4j2.yaml" \
    "$KAFKA_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

start-broker() {
  local id="$1" cid="$2" boot="$3"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/data
  cp "$KAFKA_HOME"/config/log4j2.yaml "$TEST_DIR"/server"${ref[id]}"/config/log4j2.yaml

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
node.id=${ref[id]}
process.roles=broker
log.dirs=$TEST_DIR/server${ref[id]}/data
controller.quorum.bootstrap.servers=$boot
auto.create.topics.enable=false
listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}
advertised.listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,REPLICATION:PLAINTEXT,BROKER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=REPLICATION
num.partitions=5
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
share.coordinator.state.topic.replication.factor=3
share.coordinator.state.topic.min.isr=2
EOF

  "$KAFKA_HOME"/bin/kafka-storage.sh format -t "$cid" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server${ref[id]}/config/log4j2.yaml" \
    "$KAFKA_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

start-node() {
  declare -n ref="server$1"
  echo "Starting node ${ref[id]}"
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server${ref[id]}/config/log4j2.yaml" \
    "$KAFKA_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

stop-node() {
  declare -n ref="server$1"
  echo "Stopping node ${ref[id]}"
  local pid=$(pgrep -f "server${ref[id]}/config" || true)
  if [[ -n "$pid" ]]; then
    kill -SIGTERM $pid &>/dev/null
    for ((i=0; i<30; i++)); do kill -0 $pid 2>/dev/null || break; sleep 1; done
    kill -0 $pid 2>/dev/null && { kill -SIGKILL $pid &>/dev/null; sleep 1; }
  fi
  return 0
}

restart-node() {
  declare -n ref="server$1"
  stop-node "${ref[id]}"
  start-node "${ref[id]}"
  sleep 5
}

get-cp() {
  local id="${1-}" part="${2-50}"
  echo 'public void run(String id, int part) { System.out.println(abs(id.hashCode()) % part); }
    private int abs(int n) { return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n); }
    run("'"$id"'", '"$part"');' | jshell -
}

test-setup() {
  test-teardown
  cleanup
  get-kafka https://github.com/apache/kafka.git 4.3.1 "$KAFKA_HOME"

  for i in 0 1 2; do
    declare -n ref="server$i"
    ref[did]="$("$KAFKA_HOME"/bin/kafka-storage.sh random-uuid)"
  done

  echo "Starting cluster"
  local cid="$("$KAFKA_HOME"/bin/kafka-storage.sh random-uuid)" boot="$(build-con-boot 0 2)" init="$(build-init-con 0 2)"
  for i in {0..2}; do start-controller "$i" "$cid" "$boot" "$init"; done
  for i in {3..5}; do start-broker "$i" "$cid" "$boot"; done
  sleep 2
  "$KAFKA_HOME"/bin/kafka-metadata-quorum.sh --bootstrap-server :"${server3[rep]}" describe --re --hu

  sleep 2
}

test-teardown() {
  echo "Killing all running procs"
  pkill -SIGKILL -ef "kafka.tools" ||true
  pkill -SIGKILL -ef "cli.ConnectDistributed" ||true
  pkill -SIGKILL -ef "kafka.Kafka" ||true
}

cleanup() {
  read -r -p "Delete $TEST_DIR? [Y/n]: " confirm
  if [[ -z "$confirm" || "${confirm^^}" == "Y" ]]; then
    rm -rf -- "$TEST_DIR" && echo "DONE"
  fi
}

test-setup
