# MirrorMaker 2 (ZK)

Deploy MM2 close to the destination cluster to reduce the latency in accessing internal topics.
This is a typical setup for the disaster recovery or cluster migration use cases.

## Start clusters

```sh
source examples/kafka-mm2-zk/init.sh
cd "$KAFKA_HOME"
```

## Test mirroring

Create a test topic on source cluster and send some data.

```sh
bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --create --topic my-topic --partitions 2
echo -e "a\nb\nc" | bin/kafka-console-producer.sh --bootstrap-server "$SRC_BOOTSTRAP" --topic my-topic
```

List topics on both clusters.

```sh
bin/kafka-topics.sh --bootstrap-server "$SRC_BOOTSTRAP" --list
bin/kafka-topics.sh --bootstrap-server "$DST_BOOTSTRAP" --list
```

Try to consume from destination.

```sh
bin/kafka-console-consumer.sh --bootstrap-server "$DST_BOOTSTRAP" --topic my-topic --from-beginning --max-messages 3
```

## Cleanup

```sh
cd -
test-teardown
cleanup
```
