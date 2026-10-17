#!/usr/bin/env bash
[[ $- != *i* ]] && echo "Usage: source init.sh" && exit 1

TEST_DIR="$PWD/test"; mkdir -p "$TEST_DIR"
declare -A server0=([id]="0" [jmx]="7000" [rep]="2181" [fol]="5000" [oth]="6000")
declare -A server1=([id]="1" [jmx]="7001" [rep]="2182" [fol]="5001" [oth]="6001")
declare -A server2=([id]="2" [jmx]="7002" [rep]="2183" [fol]="5002" [oth]="6002")
declare -A server3=([id]="3" [jmx]="7003" [rep]="8000" [cli]="9092")
declare -A server4=([id]="4" [jmx]="7004" [rep]="8001" [cli]="9093")
declare -A server5=([id]="5" [jmx]="7005" [rep]="8002" [cli]="9094")
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
    -x trogdor:compileJava \
    -x jmh-benchmarks:compileJava
  cd -
}

start-zserver() {
  local id="$1"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/data
  cp "$KAFKA_HOME"/config/log4j.properties "$TEST_DIR"/server"${ref[id]}"/config/log4j.properties

  echo "${ref[id]}" >"$TEST_DIR"/server"${ref[id]}"/data/myid
  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
dataDir=$TEST_DIR/server${ref[id]}/data
clientPort=${ref[rep]}
maxClientCnxns=0
admin.enableServer=false
initLimit=10
syncLimit=5
server.${server0[id]}=localhost:${server0[fol]}:${server0[oth]}
server.${server1[id]}=localhost:${server1[fol]}:${server1[oth]}
server.${server2[id]}=localhost:${server2[fol]}:${server2[oth]}
EOF

  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$TEST_DIR/server${ref[id]}/config/log4j.properties" \
    "$KAFKA_HOME"/bin/zookeeper-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

start-broker() {
  local id="$1" zconn="$2"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/data
  cp "$KAFKA_HOME"/config/log4j.properties "$TEST_DIR"/server"${ref[id]}"/config/log4j.properties

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
broker.id=${ref[id]}
log.dirs=$TEST_DIR/server${ref[id]}/data
zookeeper.connect=$zconn
zookeeper.connection.timeout.ms=18000
listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}
advertised.listeners=BROKER://localhost:${ref[cli]},REPLICATION://localhost:${ref[rep]}
listener.security.protocol.map=REPLICATION:PLAINTEXT,BROKER:PLAINTEXT
inter.broker.listener.name=REPLICATION
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
EOF

  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$TEST_DIR/server${ref[id]}/config/log4j.properties" \
    "$KAFKA_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

test-teardown() {
  echo "Killing all running procs"
  pkill -SIGKILL -ef "kafka.tools" ||true
  pkill -SIGKILL -ef "cli.ConnectDistributed" ||true
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
  get-kafka https://github.com/apache/kafka.git 3.9.2 "$KAFKA_HOME"

  echo "Starting cluster"
  for i in {0..2}; do start-zserver "$i"; done
  for i in {3..5}; do start-broker "$i" "localhost:${server0[rep]}"; done

  sleep 2
}

test-setup
