#!/usr/bin/env bash
[[ $- != *i* ]] && echo "Usage: source init.sh" && exit 1

TEST_DIR="$PWD/test"; mkdir -p "$TEST_DIR"
declare -A server0=([id]="0" [jmx]="7000" [cli]="2181" [fol]="5000" [oth]="6000")
declare -A server1=([id]="1" [jmx]="7001" [cli]="9092" [rep]="8000")
declare -A server2=([id]="2" [jmx]="7002" [cli]="9093" [rep]="8001")
declare -A server3=([id]="3" [jmx]="7003" [cli]="2182" [fol]="5001" [oth]="6001")
declare -A server4=([id]="4" [jmx]="7004" [cli]="9094" [rep]="8002")
declare -A server5=([id]="5" [jmx]="7005" [cli]="9095" [rep]="8003")
declare -A server6=([id]="6" [jmx]="7006" [cli]="8083")
SRC_BOOTSTRAP="localhost:${server1[cli]},localhost:${server2[cli]}"
DST_BOOTSTRAP="localhost:${server4[cli]},localhost:${server5[cli]}"
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

  local server_line=""
  if [[ $id -eq 0 ]]; then
    server_line="server.${server0[id]}=localhost:${server0[fol]}:${server0[oth]}"
  elif [[ $id -eq 3 ]]; then
    server_line="server.${server3[id]}=localhost:${server3[fol]}:${server3[oth]}"
  else
    echo "Invalid ZK server id: $id" >&2
    return 1
  fi

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
dataDir=$TEST_DIR/server${ref[id]}/data
clientPort=${ref[cli]}
maxClientCnxns=0
admin.enableServer=false
initLimit=10
syncLimit=5
$server_line
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
  cp "$KAFKA_HOME"/config/log4j.properties "$TEST_DIR/server${ref[id]}/config"

  cat <<EOF > "$TEST_DIR"/server"${ref[id]}"/config/server.properties
listeners=HTTP://localhost:${ref[cli]}
bootstrap.servers=$DST_BOOTSTRAP
group.id=my-connect-cluster
plugin.path=$TEST_DIR/server${ref[id]}/plugins
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
offset.flush.interval.ms=10000
EOF

  KAFKA_HEAP_OPTS="-Xmx3G -Xms3G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$TEST_DIR/server${ref[id]}/config/log4j.properties" \
    "$KAFKA_HOME"/bin/connect-distributed.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

config_source() {
  curl -s -o /dev/null -S -X POST -H "Content-Type:application/json" http://localhost:8083/connectors -d \
  '{
      "name": "a-to-b-source",
      "config": {
          "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
          "replication.policy.class": "org.apache.kafka.connect.mirror.IdentityReplicationPolicy",
          "clusters": "a, b",
          "source.cluster.alias": "a",
          "source.cluster.bootstrap.servers": "'"$SRC_BOOTSTRAP"'",
          "target.cluster.alias": "b",
          "target.cluster.bootstrap.servers": "'"$DST_BOOTSTRAP"'",
          "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
          "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
          "offset-syncs.topic.location": "target",
          "consumer.isolation.level": "read_committed",
          "tasks.max": "10",
          "replication.factor": "-1",
          "offset-syncs.topic.replication.factor": "-1",
          "heartbeats.replication.enabled": "false",
          "sync.topic.acls.enabled": "false",
          "refresh.topics.interval.seconds": "20",
          "sync.topic.configs.interval.seconds": 20,
          "sync.topic.acls.interval.seconds": 20,
          "producer.override.batch.size": "327680"
      }
  }'

  sleep 2
  "$KAFKA_HOME"/bin/kafka-configs.sh --bootstrap-server "$DST_BOOTSTRAP" --alter \
    --add-config segment.bytes=536870912,min.compaction.lag.ms=172800000 \
    --entity-type topics --entity-name mm2-offset-syncs.a.internal
}

config_checkp() {
  curl -s -o /dev/null -S -X POST -H "Content-Type:application/json" http://localhost:8083/connectors -d \
  '{
      "name": "a-to-b-checkpoint",
      "config": {
          "connector.class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
          "replication.policy.class": "org.apache.kafka.connect.mirror.IdentityReplicationPolicy",
          "clusters": "a, b",
          "source.cluster.alias": "a",
          "source.cluster.bootstrap.servers": "'"$SRC_BOOTSTRAP"'",
          "target.cluster.alias": "b",
          "target.cluster.bootstrap.servers": "'"$DST_BOOTSTRAP"'",
          "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
          "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
          "offset-syncs.topic.location": "target",
          "tasks.max": "10",
          "checkpoints.topic.replication.factor": "-1",
          "sync.group.offsets.enabled": "true",
          "refresh.groups.interval.seconds": "20",
          "emit.checkpoints.interval.seconds": "20",
          "sync.group.offsets.interval.seconds": "20",
          "groups": ".*"
      }
  }'

  sleep 2
  "$KAFKA_HOME"/bin/kafka-configs.sh --bootstrap-server "$DST_BOOTSTRAP" --alter \
    --add-config segment.bytes=52428800 --entity-type topics \
    --entity-name a.checkpoints.internal
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

  echo "Starting cluster A (source)"
  start-zserver 0
  for i in 1 2; do start-broker "$i" "localhost:${server0[cli]}"; done
  sleep 2

  echo "Starting cluster B (destination)"
  start-zserver 3
  for i in 4 5; do start-broker "$i" "localhost:${server3[cli]}"; done
  sleep 2

  echo "Starting MM2 (A -> B)"
  start-connect 6
  sleep 5

  config_source
  sleep 2
  curl -s -w "\n" http://localhost:8083/connectors/a-to-b-source/status

  config_checkp
  sleep 2
  curl -s -w "\n" http://localhost:8083/connectors/a-to-b-checkpoint/status

  sleep 2
}

test-setup
