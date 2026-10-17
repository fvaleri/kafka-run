#!/usr/bin/env bash
[[ $- != *i* ]] && echo "Usage: source init.sh" && exit 1

TEST_DIR="$PWD/test"; mkdir -p "$TEST_DIR"
declare -A server0=([id]="0" [jmx]="7002" [cli]="9092" [rep]="6000")
declare -A server1=([id]="1" [jmx]="7003" [cli]="8083")
declare -A server2=([id]="2" [jmx]="7004" [cli]="8084")
declare -A server3=([id]="3" [jmx]="7005" [cli]="8085")
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

start-kafka() {
  local cid="$1"
  mkdir -p "$TEST_DIR"/server"${server0[id]}"/config "$TEST_DIR"/server"${server0[id]}"/logs "$TEST_DIR"/server"${server0[id]}"/data
  cp "$KAFKA_HOME"/config/log4j2.yaml "$TEST_DIR"/server"${server0[id]}"/config/log4j2.yaml

  cat <<EOF > "$TEST_DIR"/server"${server0[id]}"/config/server.properties
node.id=${server0[id]}
process.roles=controller,broker
log.dirs=$TEST_DIR/server${server0[id]}/data
controller.quorum.bootstrap.servers=localhost:${server0[rep]}
listeners=CONTROLLER://localhost:${server0[rep]},BROKER://localhost:${server0[cli]}
advertised.listeners=CONTROLLER://localhost:${server0[rep]},BROKER://localhost:${server0[cli]}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=BROKER
EOF

  "$KAFKA_HOME"/bin/kafka-storage.sh format -t "$cid" -s -c "$TEST_DIR"/server"${server0[id]}"/config/server.properties
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${server0[jmx]}" LOG_DIR="$TEST_DIR/server${server0[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server${server0[id]}/config/log4j2.yaml" \
    "$KAFKA_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${server0[id]}"/config/server.properties
}

start-connect() {
  local id="$1"
  declare -n ref="server$id"
  mkdir -p "$TEST_DIR"/server"${ref[id]}"/config "$TEST_DIR"/server"${ref[id]}"/logs "$TEST_DIR"/server"${ref[id]}"/plugins
  cp "$KAFKA_HOME"/config/log4j2.yaml "$TEST_DIR"/server"${ref[id]}"/config/log4j2.yaml

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
plugin.path=$TEST_DIR/server${ref[id]}/plugins
listeners=HTTP://localhost:${ref[cli]}
bootstrap.servers=localhost:${server0[cli]}
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
  get-kafka https://github.com/apache/kafka.git 4.3.1 "$KAFKA_HOME"

  echo "Starting Kafka"
  local cid="$("$KAFKA_HOME"/bin/kafka-storage.sh random-uuid)"
  start-kafka "$cid"

  echo "Starting Connect cluster"
  for i in {1..3}; do start-connect "$i"; done

  sleep 2
}

test-setup
