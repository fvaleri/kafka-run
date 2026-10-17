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

# This needs to be exported to be used by the env config provider
export PASSWD="changeit"

create-crt() {
  local dir="$1"
  local cn="localhost" o="Fede" st="Rome" c="IT" domain="f12i.io"
  local config="
[req]
prompt=no
distinguished_name=dn
x509_extensions=ext
[dn]
countryName=$c
stateOrProvinceName=$st
organizationName=$o
commonName=$cn
[ext]
subjectAltName=@san
[san]
DNS.1=$cn
DNS.2=$cn.$domain
DNS.3=www.$cn.$domain
"
  echo "Creating self-signed TLS certificate"
  mkdir -p "$dir"
  openssl genrsa -out "$dir"/"$cn".key 2048
  openssl req -new -x509 -days 3650 -key "$dir"/"$cn".key -out "$dir"/"$cn".crt -config <(echo "$config")
}

create-jks() {
  local dir="$1"
  echo "Creating JKS keystore and truststore from PEM certificate"
  openssl pkcs12 -export -in "$dir"/localhost.crt -inkey "$dir"/localhost.key \
    -out "$dir"/keystore.p12 -name localhost -password pass:"$PASSWD"
  keytool -importkeystore -srckeystore "$dir"/keystore.p12 -srcstoretype PKCS12 \
    -srcstorepass "$PASSWD" -destkeystore "$dir"/keystore.jks -deststoretype JKS \
    -deststorepass "$PASSWD" -noprompt
  keytool -import -file "$dir"/localhost.crt -alias localhost \
    -keystore "$dir"/truststore.jks -storepass "$PASSWD" -noprompt
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

# Source cluster ZK broker (uses downloaded binary, JKS for SSL compatibility with older versions)
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
listener.security.protocol.map=REPLICATION:SASL_SSL,BROKER:SASL_SSL
inter.broker.listener.name=REPLICATION
default.replication.factor=2
min.insync.replicas=1
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
super.users=User:kafka
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
sasl.enabled.mechanisms=PLAIN
sasl.mechanism.inter.broker.protocol=PLAIN
listener.name.broker.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\
    username="kafka" password="changeit" user_kafka="changeit" user_client="changeit";
listener.name.replication.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\
    username="kafka" password="changeit" user_kafka="changeit" user_client="changeit";
ssl.keystore.type=JKS
ssl.keystore.location=$TEST_DIR/keystore.jks
ssl.keystore.password=$PASSWD
ssl.key.password=$PASSWD
ssl.truststore.type=JKS
ssl.truststore.location=$TEST_DIR/truststore.jks
ssl.truststore.password=$PASSWD
ssl.endpoint.identification.algorithm=HTTPS
EOF

  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server${ref[id]}/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$TEST_DIR/server${ref[id]}/config/log4j.properties" \
    "$SRC_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

# Destination cluster controller (uses local build, PEM for SSL)
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
listener.security.protocol.map=CONTROLLER:SASL_SSL
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
config.providers=env,dir
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
config.providers.env.param.allowlist.pattern=.*
config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
config.providers.dir.param.allowlist.pattern=.*
super.users=User:kafka
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
sasl.enabled.mechanisms=
sasl.mechanism.controller.protocol=PLAIN
listener.name.controller.sasl.enabled.mechanisms=PLAIN
listener.name.controller.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\
    username="kafka" password="\${env:PASSWD}" user_kafka="\${env:PASSWD}" user_client="\${env:PASSWD}";
listener.name.controller.ssl.keystore.type=PEM
listener.name.controller.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}
listener.name.controller.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}
listener.name.controller.ssl.truststore.type=PEM
listener.name.controller.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
ssl.endpoint.identification.algorithm=HTTPS
unstable.api.versions.enable=true
unstable.feature.versions.enable=true
EOF

  "$DST_HOME"/bin/kafka-storage.sh format -t "$cid" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties -s
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server"${ref[id]}"/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server"${ref[id]}"/config/log4j2.yaml" \
    "$DST_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

# Destination cluster broker (uses local build, PEM for SSL)
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
listener.security.protocol.map=CONTROLLER:SASL_SSL,REPLICATION:SASL_SSL,BROKER:SASL_SSL
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
config.providers=env,dir
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
super.users=User:kafka
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
sasl.enabled.mechanisms=
sasl.mechanism.controller.protocol=PLAIN
listener.name.controller.sasl.enabled.mechanisms=PLAIN
listener.name.controller.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\
    username="kafka" password="\${env:PASSWD}" user_kafka="\${env:PASSWD}" user_client="\${env:PASSWD}";
sasl.mechanism.inter.broker.protocol=PLAIN
listener.name.broker.sasl.enabled.mechanisms=PLAIN
listener.name.broker.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\
    username="kafka" password="\${env:PASSWD}" user_kafka="\${env:PASSWD}" user_client="\${env:PASSWD}";
listener.name.replication.sasl.enabled.mechanisms=PLAIN
listener.name.replication.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\
    username="kafka" password="\${env:PASSWD}" user_kafka="\${env:PASSWD}" user_client="\${env:PASSWD}";
listener.name.controller.ssl.keystore.type=PEM
listener.name.controller.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}
listener.name.controller.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}
listener.name.controller.ssl.truststore.type=PEM
listener.name.controller.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
listener.name.broker.ssl.keystore.type=PEM
listener.name.broker.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}
listener.name.broker.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}
listener.name.broker.ssl.truststore.type=PEM
listener.name.broker.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
listener.name.replication.ssl.keystore.type=PEM
listener.name.replication.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}
listener.name.replication.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}
listener.name.replication.ssl.truststore.type=PEM
listener.name.replication.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
ssl.endpoint.identification.algorithm=HTTPS
unstable.api.versions.enable=true
unstable.feature.versions.enable=true
EOF

  "$DST_HOME"/bin/kafka-storage.sh format -t "$cid" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties
  KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" JMX_PORT="${ref[jmx]}" LOG_DIR="$TEST_DIR/server"${ref[id]}"/logs" \
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$TEST_DIR/server"${ref[id]}"/config/log4j2.yaml" \
    "$DST_HOME"/bin/kafka-server-start.sh -daemon "$TEST_DIR"/server"${ref[id]}"/config/server.properties
}

test-setup() {
  test-teardown
  cleanup
  get-kafka https://github.com/apache/kafka.git 3.9.2 "$SRC_HOME"
  get-kafka https://github.com/showuon/kafka.git cm-20261018 "$DST_HOME"
  create-crt "$TEST_DIR"
  create-jks "$TEST_DIR"

  echo "Creating properties files"
  cat <<EOF > "$TEST_DIR"/kafka.properties
sasl.mechanism=PLAIN
security.protocol=SASL_SSL
config.providers=env,dir
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\
  username="kafka" \\
  password="\${env:PASSWD}";
ssl.truststore.type=PEM
ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
EOF

  cat <<EOF > "$TEST_DIR"/client.properties
sasl.mechanism=PLAIN
security.protocol=SASL_SSL
config.providers=env,dir
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\
  username="client" \\
  password="\${env:PASSWD}";
ssl.truststore.type=PEM
ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
EOF

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
  "$DST_HOME"/bin/kafka-metadata-quorum.sh --bootstrap-server "$DST_BOOTSTRAP" \
    --command-config "$TEST_DIR"/kafka.properties describe --re --hu

  # Enable cluster mirroring on destination if not enabled
  "$DST_HOME"/bin/kafka-features.sh --bootstrap-server "$DST_BOOTSTRAP" \
    --command-config "$TEST_DIR"/kafka.properties upgrade --feature mirror.version=1

  sleep 2

  echo "Granting client permissions on source"
  for topic in my-topic-a my-topic-b new-topic-a; do
    "$SRC_HOME"/bin/kafka-acls.sh --bootstrap-server "$SRC_BOOTSTRAP" --add --allow-principal User:client \
      --producer --topic "$topic" --command-config "$TEST_DIR"/kafka.properties
    "$SRC_HOME"/bin/kafka-acls.sh --bootstrap-server "$SRC_BOOTSTRAP" --add --allow-principal User:client \
      --consumer --topic "$topic" --group my-group --command-config "$TEST_DIR"/kafka.properties
  done

  sleep 2
}

test-teardown() {
  echo "Killing all running procs"
  pkill -SIGKILL -f "update.py" ||true
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

test-setup
