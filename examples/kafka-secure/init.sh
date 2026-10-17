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

export PASSWD="changeit"

build-con-init() {
  local start="$1" end="$2"
  local init=""
  for i in $(seq "$start" "$end"); do
    declare -n ref="server$i"
    init+="${init:+,}${ref[id]}@localhost:${ref[rep]}:${ref[did]}"
  done
  echo "$init"
}

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
listener.security.protocol.map=CONTROLLER:SSL
controller.listener.names=CONTROLLER
num.partitions=5
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
share.coordinator.state.topic.replication.factor=3
share.coordinator.state.topic.min.isr=2
config.providers=env,dir
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
super.users=User:CN=localhost,O=Fede,ST=Rome,C=IT;User:kafka
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
listener.name.controller.ssl.client.auth=required
listener.name.controller.ssl.keystore.type=PEM
listener.name.controller.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}
listener.name.controller.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}
listener.name.controller.ssl.truststore.type=PEM
listener.name.controller.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
ssl.endpoint.identification.algorithm=HTTPS
EOF

  "$KAFKA_HOME"/bin/kafka-storage.sh format -t "$cid" -I "$init" -c "$TEST_DIR"/server"${ref[id]}"/config/server.properties \
    --add-scram "SCRAM-SHA-512=[name=\"kafka\",password=\"$PASSWD\"]"
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
listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}
advertised.listeners=REPLICATION://localhost:${ref[rep]},BROKER://localhost:${ref[cli]}
listener.security.protocol.map=CONTROLLER:SSL,REPLICATION:SSL,BROKER:SASL_SSL
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
config.providers=env,dir
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
super.users=User:CN=localhost,O=Fede,ST=Rome,C=IT;User:kafka
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
ssl.endpoint.identification.algorithm=HTTPS
listener.name.controller.ssl.keystore.type=PEM
listener.name.controller.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}
listener.name.controller.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}
listener.name.controller.ssl.truststore.type=PEM
listener.name.controller.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
listener.name.replication.ssl.keystore.type=PEM
listener.name.replication.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}
listener.name.replication.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}
listener.name.replication.ssl.truststore.type=PEM
listener.name.replication.ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
sasl.enabled.mechanisms=
listener.name.broker.sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.broker.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\
    username="kafka" password="\${env:PASSWD}";
listener.name.broker.ssl.keystore.type=PEM
listener.name.broker.ssl.keystore.certificate.chain=\${dir:$TEST_DIR:localhost.crt}
listener.name.broker.ssl.keystore.key=\${dir:$TEST_DIR:localhost.key}
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
  create-crt "$TEST_DIR"

  server0[did]="$("$KAFKA_HOME"/bin/kafka-storage.sh random-uuid)"
  server1[did]="$("$KAFKA_HOME"/bin/kafka-storage.sh random-uuid)"
  server2[did]="$("$KAFKA_HOME"/bin/kafka-storage.sh random-uuid)"

  echo "Creating properties files"
  cat <<EOF > "$TEST_DIR"/kafka.properties
config.providers=env,dir
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka" password="\${env:PASSWD}";
ssl.truststore.type=PEM
ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
EOF

  cat <<EOF > "$TEST_DIR"/client.properties
config.providers=env,dir
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="client" password="\${env:PASSWD}";
ssl.truststore.type=PEM
ssl.truststore.certificates=\${dir:$TEST_DIR:localhost.crt}
EOF

  echo "Starting cluster"
  local cid="$("$KAFKA_HOME"/bin/kafka-storage.sh random-uuid)" boot="$(build-con-boot 0 2)" init="$(build-con-init 0 2)"
  for i in {0..2}; do start-controller "$i" "$cid" "$boot" "$init"; done
  for i in {3..5}; do start-broker "$i" "$cid" "$boot"; done
  sleep 2
  "$KAFKA_HOME"/bin/kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" --command-config "$TEST_DIR"/kafka.properties describe --re --hu

  sleep 2
}

test-setup
