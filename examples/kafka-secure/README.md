# Secure (KRaft)

KRaft cluster with 3 controllers and 3 brokers using TLS encryption and SCRAM-SHA-512 authentication.

## Start cluster

```sh
source examples/kafka-secure/init.sh
cd "$KAFKA_HOME"
```

## Add all ACLs to the kafka user

```sh
bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --cluster --add --allow-principal User:kafka --operation All --command-config "$TEST_DIR"/kafka.properties
bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --add --allow-principal User:kafka --operation All --topic * --command-config "$TEST_DIR"/kafka.properties
bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --add --allow-principal User:kafka --operation All --group * --command-config "$TEST_DIR"/kafka.properties
bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --add --allow-principal User:kafka --operation All --transactional-id * --command-config "$TEST_DIR"/kafka.properties

bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
```

## Remove the SCRAM super-user and do a rolling update

```sh
for i in {0..5}; do restart-node "$i"; done
```

## Create new user with some ACLs

```sh
bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --alter --entity-type users --entity-name client --add-config "SCRAM-SHA-512=[password=$PASSWD]" --command-config "$TEST_DIR"/kafka.properties

bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --add --allow-principal User:client --operation Create --topic my-topic --command-config "$TEST_DIR"/kafka.properties
bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --add --allow-principal User:client --producer --topic my-topic --command-config "$TEST_DIR"/kafka.properties
bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --add --allow-principal User:client --consumer --topic my-topic --group my-group --command-config "$TEST_DIR"/kafka.properties

bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --list --command-config "$TEST_DIR"/kafka.properties
```

## Run clients

```sh
bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic my-topic --partitions 5 --replication-factor 3 --command-config "$TEST_DIR"/client.properties
echo -e "hello\nworld" | bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic my-topic --producer.config "$TEST_DIR"/client.properties
bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic my-topic --group my-group --from-beginning --max-messages 2 --consumer.config "$TEST_DIR"/client.properties
```

## Cleanup

```sh
cd -
test-teardown
cleanup
```
