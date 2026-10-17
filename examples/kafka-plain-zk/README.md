# Plain (ZK)

ZooKeeper cluster with 3 ZK servers and 3 brokers using PLAINTEXT.

## Start cluster

```sh
source examples/kafka-plain-zk/init.sh
cd "$KAFKA_HOME"
```

## Check metadata

```sh
bin/zookeeper-shell.sh localhost:2181 ls /
bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids
bin/zookeeper-shell.sh localhost:2181 get /controller
bin/zookeeper-shell.sh localhost:2181 get /config/topics/my-topic
```

## Run clients

```sh
bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic my-topic --partitions 5 --replication-factor 3
echo -e "hello\nworld" | bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic my-topic
bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic my-topic --from-beginning --max-messages 2
```

## Cleanup

```sh
cd -
test-teardown
cleanup
```
