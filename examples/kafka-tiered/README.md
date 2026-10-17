# Tiered Storage

Single combined KRaft node with tiered storage using LocalTieredStorage for testing.

## Start cluster

```sh
source examples/kafka-tiered/init.sh
cd "$KAFKA_HOME"
```

## Create test topics

Segment 1 GiB, local retention: 1 byte, global retention: ~ 3.7 GiB.

```sh
bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic t0 --partitions 2 --replication-factor 1 \
  --config remote.storage.enable=true --config segment.bytes=1000000000 --config file.delete.delay.ms=1000 \
  --config retention.bytes=4000000000 --config local.retention.bytes=1
bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic t1 --partitions 2 --replication-factor 1 \
  --config remote.storage.enable=true --config segment.bytes=1000000000 --config file.delete.delay.ms=1000 \
  --config retention.bytes=4000000000 --config local.retention.bytes=1
```

## Check topics and config

```sh
bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe
bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --entity-type topics --entity-name t0 --describe --all
```

## Observe local and remote storage

```sh
watch -n1 'echo "____LOCAL____" && ls -lh '$TEST_DIR'/server0/data/t0* && echo -e "\n____REMOTE____" && ls -lh '$TEST_DIR'/kafka-tiered-storage/t0*'
```

## Produce to create remote logs (~ 2.8 GiB per topic)

```sh
bin/kafka-producer-perf-test.sh --producer-props bootstrap.servers="$BOOTSTRAP" linger.ms=0 batch.size=100000 \
  --topic t0 --throughput -1 --record-size 1000 --num-records 3000000
bin/kafka-producer-perf-test.sh --producer-props bootstrap.servers="$BOOTSTRAP" linger.ms=0 batch.size=100000 \
  --topic t1 --throughput -1 --record-size 1000 --num-records 3000000
```

## Consume from remote partitions

Set `RemoteLogReader` log to DEBUG to see parallel remote read tasks.

```sh
bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --include "t.*" --from-beginning --property print.offset=true

bin/kafka-consumer-perf-test.sh --bootstrap-server "$BOOTSTRAP" --include t.* \
  --fetch-size 52428800 --messages 3000000 --timeout 30000 | tail -n1 \
  | awk -F', ' '{print "Throughput (MB/s): " $4 ", Fetch time (ms): " $8}'
```

## Check metadata

```sh
bin/kafka-dump-log.sh --remote-log-metadata-decoder \
  --files "$TEST_DIR"/server0/data/__remote_log_metadata-0/00000000000000000000.log
```

## Set max upload/download quotas

```sh
bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --entity-type brokers --entity-default \
  --alter --add-config remote.log.manager.copy.max.bytes.per.second=52428800
bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --entity-type brokers --entity-default \
  --alter --add-config remote.log.manager.fetch.max.bytes.per.second=52428800
```

## Disable keeping remote segments as read-only

```sh
bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics --entity-name my-topic \
   --add-config 'remote.storage.enable=true,remote.log.copy.disable=true,local.retention.ms=-2,local.retention.bytes=-2'
```

## Enable

```sh
bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics --entity-name my-topic \
  --add-config 'remote.storage.enable=true,remote.log.copy.disable=false,retention.bytes=3000000000,local.retention.bytes=1'
```

## Disable deleting remote log segments

```sh
bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics --entity-name my-topic \
   --add-config 'remote.storage.enable=false,remote.log.delete.on.disable=true,local.retention.ms=-2,local.retention.bytes=-2'
```

## Cleanup

```sh
cd -
test-teardown
cleanup
```
