#!/usr/bin/env bash
[[ $- != *i* ]] && echo "Usage: source init.sh" && exit 1

TEST_DIR="$PWD/test"; mkdir -p "$TEST_DIR"
declare -A server0=([id]="0" [rep]="6000" [jmx]="7002" [cli]="9092")
BOOTSTRAP="localhost:${server0[cli]}"
KAFKA_HOME="$TEST_DIR/kafka"

get-kafka() {
  local repo="$1" rev="$2" dir="$3"
  echo "Getting Kafka from $repo with revision $rev into $dir"
  git clone --depth 1 -b "$rev" "$repo" "$dir"
  cd "$dir"
  ./gradlew jar testJar \
    -x checkstyleMain \
    -x checkstyleTest \
    -x jmh-benchmarks:compileJava
  cd -
}

start-kafka() {
  local cid="$1"
  local version="$("$KAFKA_HOME"/bin/kafka-topics.sh --version)"
  mkdir -p "$TEST_DIR"/server"${server0[id]}"/config "$TEST_DIR"/server"${server0[id]}"/logs "$TEST_DIR"/server"${server0[id]}"/data
  cp "$KAFKA_HOME"/config/log4j2.yaml "$TEST_DIR"/server"${server0[id]}"/config/log4j2.yaml

  cat <<EOF > "$TEST_DIR"/server"${server0[id]}"/config/server.properties
node.id=${server0[id]}
process.roles=controller,broker
log.dirs=$TEST_DIR/server${server0[id]}/data
controller.quorum.bootstrap.servers=localhost:${server0[rep]}
log.retention.check.interval.ms=10000
listeners=CONTROLLER://localhost:${server0[rep]},BROKER://localhost:${server0[cli]}
advertised.listeners=CONTROLLER://localhost:${server0[rep]},BROKER://localhost:${server0[cli]}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=BROKER
remote.log.storage.system.enable=true
remote.log.manager.task.interval.ms=5000
remote.fetch.max.wait.ms=3000
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
remote.log.metadata.manager.listener.name=BROKER
rlmm.config.remote.log.metadata.topic.num.partitions=1
rlmm.config.remote.log.metadata.topic.replication.factor=1
remote.log.storage.manager.class.path=$KAFKA_HOME/storage/build/libs/kafka-storage-$version-test.jar
remote.log.storage.manager.class.name=org.apache.kafka.server.log.remote.storage.LocalTieredStorage
remote.log.storage.manager.impl.prefix=rsm.config.
rsm.config.dir=$TEST_DIR
EOF

  "$KAFKA_HOME"/bin/kafka-storage.sh format -t "$cid" -s -c "$TEST_DIR"/server"${server0[id]}"/config/server.properties
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${server0[jmx]}" LOG_DIR="$TEST_DIR/server${server0[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server${server0[id]}/config/log4j2.yaml" \
    "$KAFKA_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${server0[id]}"/config/server.properties
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

  sleep 2
}

test-setup
