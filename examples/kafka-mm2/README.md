# MirrorMaker 2 (KRaft)

Deploy MM2 close to the destination cluster to reduce the latency in accessing internal topics.
This is a typical setup for the disaster recovery or cluster migration use cases.

## Start clusters

```sh
source examples/kafka-mm2/init.sh
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

## Start mirroring from exact position

1. Get the source offset to start mirroring from using UNIX timestamp in millis.

```sh
bin/kafka-get-offsets.sh --bootstrap-server "$SRC_BOOTSTRAP" \
  --time 1696500000000 --topic-partitions my-topic
```

2. Stop and clear the position if the connector is already running.

```sh
curl -s -X PUT http://localhost:8083/connectors/a-to-b-source/stop
curl -s -X DELETE http://localhost:8083/connectors/a-to-b-source/offsets
```

3. Create the connector in stopped state (or skip if it already exists from step 2).

```sh
curl -s -X POST -H "Content-Type:application/json" http://localhost:8083/connectors -d \
'{
    "name": "a-to-b-source",
    "config": {
        "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
        "tasks.max": "10",
        ...
    },
    "initial_state": "STOPPED"
}'
```

4. Set the starting offset.

```sh
curl -s -X PATCH -H "Content-Type:application/json" http://localhost:8083/connectors/a-to-b-source/offsets -d \
'{
    "offsets": [
        {
            "partition": {
                "cluster": "A",
                "topic": "my-topic",
                "partition": 0
            },
            "offset": {
                "offset": 100
            }
        }
    ]
}'
```

5. Resume the connector.

```sh
curl -s -X PUT http://localhost:8083/connectors/a-to-b-source/resume
```

## Restart mirroring from scratch

1. Stop mirror connectors.

```sh
curl -s -X PUT http://localhost:8083/connectors/a-to-b-source/stop
curl -s -w "\n" http://localhost:8083/connectors/a-to-b-source/status

curl -s -X PUT http://localhost:8083/connectors/a-to-b-checkpoint/stop
curl -s -w "\n" http://localhost:8083/connectors/a-to-b-checkpoint/status
```

2. Update the offset reset policy of source connector.

```sh
curl -s http://localhost:8083/connectors/a-to-b-source/config \
  | jq '.["consumer.auto.offset.reset"] = "earliest"' \
  | curl -s -X PUT -H "Content-Type:application/json" http://localhost:8083/connectors/a-to-b-source/config -d @-
```

3. Resets the offset on both connectors.

```sh
curl -s -X DELETE http://localhost:8083/connectors/a-to-b-source/offsets
curl -s -X DELETE http://localhost:8083/connectors/a-to-b-checkpoint/offsets
```

4. Delete all mirrored topics on destination cluster.

```sh
bin/kafka-topics.sh --bootstrap-server "$DST_BOOTSTRAP" --delete --topic my-topic
bin/kafka-topics.sh --bootstrap-server "$DST_BOOTSTRAP" --list
```

5. Delete MM2 internal topics.
   Note: in this example we have offset-syncs topic on the destination, but it is created on the source by default.

```sh
bin/kafka-topics.sh --bootstrap-server "$DST_BOOTSTRAP" --delete --topic mm2-offset-syncs.a.internal
bin/kafka-topics.sh --bootstrap-server "$DST_BOOTSTRAP" --delete --topic a.checkpoints.internal
bin/kafka-topics.sh --bootstrap-server "$DST_BOOTSTRAP" --list
```

6. Resume the connectors.

```sh
curl -s -X PUT http://localhost:8083/connectors/a-to-b-source/resume
curl -s -w "\n" http://localhost:8083/connectors/a-to-b-source/status

curl -s -X PUT http://localhost:8083/connectors/a-to-b-checkpoint/resume
curl -s -w "\n" http://localhost:8083/connectors/a-to-b-source/status
```

7. Check that topics have been mirrored from scratch.

```sh
bin/kafka-topics.sh --bootstrap-server "$DST_BOOTSTRAP" --list
bin/kafka-console-consumer.sh --bootstrap-server "$DST_BOOTSTRAP" --topic my-topic --from-beginning --max-messages 3
```

## Inspect internal topics

This topic stores the MirrorMaker2 Source Connector and Checkpoint Connector offsets.
The name of this topic can be configured using `offset.storage.topic`.

```sh
bin/kafka-console-consumer.sh --bootstrap-server "$DST_BOOTSTRAP" \
  --topic __my-connect-offsets --from-beginning --property print.key=true \
  | grep MirrorSourceConnector | grep -i "$TOPIC_NAME"
```

This topic is used to track the synchronization of messages between source and target clusters for the topics that are configured to be replicated.
Each message in this internal topic contains information about the data topics, partitions, the offset on source and the offset on target.
The Source Connector writes to this topic, while the Checkpoint Connector reads from it.

```sh
bin/kafka-console-consumer.sh --bootstrap-server "$DST_BOOTSTRAP" \
  --formatter "org.apache.kafka.connect.mirror.formatters.OffsetSyncFormatter" \
  --topic mm2-offset-syncs.a.internal --from-beginning | grep "$TOPIC_NAME"
```

This topic contains information about the last committed offset in the source and target clusters for replicated topics and consumer groups.
This topic is controlled by the Checkpoint Connector.

```sh
bin/kafka-console-consumer.sh --bootstrap-server "$DST_BOOTSTRAP" \
  --formatter "org.apache.kafka.connect.mirror.formatters.CheckpointFormatter" \
  --topic a.checkpoints.internal --from-beginning | grep "$TOPIC_NAME"
```

## Cleanup

```sh
cd -
test-teardown
cleanup
```
