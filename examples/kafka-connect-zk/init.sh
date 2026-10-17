#!/usr/bin/env bash
[[ $- != *i* ]] && echo "Usage: source init.sh" && exit 1

TEST_DIR="$PWD/test"; mkdir -p "$TEST_DIR"
declare -A server0=([id]="0" [jmx]="7000" [cli]="2181" [fol]="5000" [oth]="6000")
declare -A server1=([id]="1" [jmx]="7001" [cli]="9092" [rep]="4000")
declare -A server2=([id]="2" [jmx]="7002" [cli]="8083")
declare -A server3=([id]="3" [jmx]="7003" [cli]="8084")
declare -A server4=([id]="4" [jmx]="7004" [cli]="8085")
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
clientPort=${ref[cli]}
maxClientCnxns=0
admin.enableServer=false
initLimit=10
syncLimit=5
server.${server0[id]}=localhost:${server0[fol]}:${server0[oth]}
EOF

  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$TEST_DIR/server${ref[id]}/config/log4j.properties" \
    "$KAFKA_HOME"/bin/zookeeper-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

start-broker() {
  local id="$1" zconn="$2"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR/server${ref[id]}/config" "$TEST_DIR/server${ref[id]}/logs" "$TEST_DIR/server${ref[id]}/data"
  cp "$KAFKA_HOME"/config/log4j.properties "$TEST_DIR/server${ref[id]}/config"

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
broker.id=${ref[id]}
log.dirs=$TEST_DIR/server${ref[id]}/data
zookeeper.connect=$zconn
zookeeper.connection.timeout.ms=18000
listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}
advertised.listeners=BROKER://localhost:${ref[cli]},REPLICATION://localhost:${ref[rep]}
listener.security.protocol.map=REPLICATION:PLAINTEXT,BROKER:PLAINTEXT
inter.broker.listener.name=REPLICATION
num.partitions=5
default.replication.factor=2
min.insync.replicas=1
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
EOF

  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$TEST_DIR/server${ref[id]}/config/log4j.properties" \
    "$KAFKA_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

start-connect() {
  local id="$1"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/plugins
  cp "$KAFKA_HOME"/config/log4j2.yaml "$TEST_DIR"/server"${ref[id]}"/config/log4j2.yaml

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
plugin.path=$TEST_DIR/server${ref[id]}/plugins
listeners=HTTP://localhost:${ref[cli]}
bootstrap.servers=localhost:${server1[cli]}
group.id=my-connect-cluster
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
key.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter.schemas.enable=false
offset.storage.topic=__my-connect-offsets
offset.storage.replication.factor=-1
config.storage.topic=__my-connect-configs
config.storage.replication.factor=-1
status.storage.topic=__my-connect-status
status.storage.replication.factor=-1
EOF

  KAFKA_HEAP_OPTS="-Xmx3G -Xms3G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server${ref[id]}/config/log4j2.yaml" \
    "$KAFKA_HOME"/bin/connect-distributed.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
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

  echo "Starting Kafka"
  start-zserver 0
  start-broker 1 "localhost:${server0[cli]}"

  echo "Starting Connect cluster"
  for i in {2..4}; do start-connect "$i"; done

  sleep 2
}

test-setup
