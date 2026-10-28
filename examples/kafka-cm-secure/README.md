# Cluster Mirroring (Secure)

Cluster A (source) runs on Kafka 3.9.2 with ZK mode, JKS keystores and legacy AclAuthorizer.
Cluster B (destination) runs on Kafka 4.2.0 POC with KRaft mode, PEM certificates and StandardAuthorizer.
Both clusters run on localhost and use SASL_SSL with PLAIN authentication sharing a self-signed TLS certificate.

## Start clusters

```sh
source examples/kafka-cm-secure/init.sh
```

## Create topics with custom configs on source

```sh
"$SRC_HOME"/bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --create --topic my-topic-a \
  --partitions 3 --config retention.ms=86400000 --config max.message.bytes=2097152 \
  --command-config "$TEST_DIR"/kafka.properties
"$SRC_HOME"/bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --create --topic my-topic-b \
  --partitions 5 --config retention.ms=172800000 --config max.message.bytes=1048576 \
  --command-config "$TEST_DIR"/kafka.properties
"$SRC_HOME"/bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --create --topic new-topic-a \
  --partitions 2 --config retention.ms=604800000 --config max.message.bytes=4194304 \
  --command-config "$TEST_DIR"/kafka.properties
```

## List topics

```sh
echo "__________ SRC __________"
"$SRC_HOME"/bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
echo "__________ DST __________"
"$DST_HOME"/bin/kafka-topics.sh --bootstrap-server "$DST_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
```

## Start clients

```sh
# Start 3 producers at 1000 msg/s each and 2 consumers in my-group on source
# No key is set so the default partitioner round-robins across all partitions
for topic in my-topic-a my-topic-b new-topic-a; do
  "$SRC_HOME"/bin/kafka-producer-perf-test.sh --topic "$topic" --throughput 1000 \
    --num-records 10000000 --record-size 512 --producer.config "$TEST_DIR"/client.properties \
    --producer-props bootstrap.servers="$SRC_BOOTSTRAP" >/dev/null 2>&1 &
  disown
done
"$SRC_HOME"/bin/kafka-console-consumer.sh --bootstrap-server "$SRC_BOOTSTRAP" --include "my-topic-.*" \
  --group my-group --from-beginning --consumer.config "$TEST_DIR"/client.properties >/dev/null >/dev/null 2>&1 &
disown
"$SRC_HOME"/bin/kafka-console-consumer.sh --bootstrap-server "$SRC_BOOTSTRAP" --include "new-topic-a" \
  --group my-group --from-beginning --consumer.config "$TEST_DIR"/client.properties >/dev/null >/dev/null 2>&1 &
disown
```

## Create a mirror

```sh
cat <<EOF > "$TEST_DIR"/a-to-b.properties
bootstrap.servers=$SRC_BOOTSTRAP
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
topics.include=my-topic.*,new-topic-a
fetch.max.bytes=10485760
fetch.response.max.bytes=52428800
fetch.wait.max.ms=500
socket.receive.buffer.bytes=1048576
EOF

"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --create --mirror a-to-b \
  --mirror-config "$TEST_DIR"/a-to-b.properties --command-config "$TEST_DIR"/kafka.properties

wait-for-state "$DST_BOOTSTRAP" "MIRRORING" ".*" ".*" "$TEST_DIR"/kafka.properties
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --describe \
  --command-config "$TEST_DIR"/kafka.properties
```

## Stop all clients

```sh
pkill -SIGKILL -ef "ProducerPerformance" ||true
pkill -SIGKILL -ef "ConsoleConsumer" ||true

wait-for-lag-zero "$DST_BOOTSTRAP" ".*" ".*" "$TEST_DIR"/kafka.properties
wait-for-meta-sync "$DST_BOOTSTRAP" "$TEST_DIR"/kafka.properties
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --describe \
  --command-config "$TEST_DIR"/kafka.properties
```

## Verify synchronization

```sh
# Verify log convergence by comparing last 5 records on my-topic-b partition 2
echo "__________ SRC __________"
"$SRC_HOME"/bin/kafka-dump-log.sh --files "$TEST_DIR"/server1/data/my-topic-b-2/*.log --print-data-log 2>/dev/null | tail -5
echo "__________ DST __________"
"$DST_HOME"/bin/kafka-dump-log.sh --files "$TEST_DIR"/server4/data/my-topic-b-2/*.log --print-data-log 2>/dev/null | tail -5

# Verify topic configuration on new-topic-a
echo "__________ SRC __________"
"$SRC_HOME"/bin/kafka-configs.sh --bootstrap-server "$SRC_BOOTSTRAP" --entity-type topics --entity-name new-topic-a \
  --describe --command-config "$TEST_DIR"/kafka.properties
echo "__________ DST __________"
"$DST_HOME"/bin/kafka-configs.sh --bootstrap-server "$DST_BOOTSTRAP" --entity-type topics --entity-name new-topic-a \
  --describe --command-config "$TEST_DIR"/kafka.properties

# Verify access control list
echo "__________ SRC __________"
"$SRC_HOME"/bin/kafka-acls.sh --bootstrap-server "$SRC_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
echo "__________ DST __________"
"$DST_HOME"/bin/kafka-acls.sh --bootstrap-server "$DST_BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties

# Verify consumer group offsets for my-group
# Row ordering may be different because the old Kafka version lacks topic+partition sorting
echo "__________ SRC __________"
"$SRC_HOME"/bin/kafka-consumer-groups.sh --bootstrap-server "$SRC_BOOTSTRAP" --group my-group \
  --describe --command-config "$TEST_DIR"/kafka.properties
echo -e "\n__________ DST __________"
"$DST_HOME"/bin/kafka-consumer-groups.sh --bootstrap-server "$DST_BOOTSTRAP" --group my-group \
  --describe --command-config "$TEST_DIR"/kafka.properties
```

## Failover to destination

```sh
# This operation produces a pid-reset control record for each mirror partition
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --stop \
  --mirror a-to-b --topics ".*" --command-config "$TEST_DIR"/kafka.properties

wait-for-state "$DST_BOOTSTRAP" "STOPPED" ".*" ".*" "$TEST_DIR"/kafka.properties
wait-for-meta-sync "$DST_BOOTSTRAP" "$TEST_DIR"/kafka.properties
"$DST_HOME"/bin/kafka-cluster-mirrors.sh --bootstrap-server "$DST_BOOTSTRAP" --describe \
  --command-config "$TEST_DIR"/kafka.properties
```

## Drain all messages from destination

```sh
# The total consumed messages should be equal to the sum of lag values minus 10 pid-reset control records
"$DST_HOME"/bin/kafka-console-consumer.sh --bootstrap-server "$DST_BOOTSTRAP" \
  --group my-group --include ".*" --timeout-ms 10000 \
  --consumer-property group.protocol=consumer \
  --consumer-property auto.commit.interval.ms=10 \
  --consumer.config "$TEST_DIR"/client.properties 2>&1 >/dev/null | grep "Processed"
"$DST_HOME"/bin/kafka-consumer-groups.sh --bootstrap-server "$DST_BOOTSTRAP" --group my-group \
  --describe --command-config "$TEST_DIR"/kafka.properties
```

## Cleanup

```sh
test-teardown
cleanup
```
