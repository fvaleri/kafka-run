# Connect (ZK)

ZooKeeper with a single broker and a 3-node Kafka Connect cluster.

## Start clusters

```sh
source examples/kafka-connect-zk/init.sh
cd "$KAFKA_HOME"
```

## Use REST API

```sh
curl -s http://localhost:8083
curl -s http://localhost:8083/connector-plugins?connectorsOnly=false
```

## Create connector

`tasks.max` tasks are started (some will remain idle if there are less partitions).

```sh
curl -s -X POST -H "Content-Type:application/json" http://localhost:8083/connectors -d \
'{
  "name": "my-file-sink",
  "config": {
    "connector.class": "FileStreamSink",
    "tasks.max": 1,
    "topics": "my-topic",
    "file": "/tmp/test.txt",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "my-dlq"
  }
}'
```

## Check status and config

```sh
curl -s http://localhost:8083/connector-plugins
curl -s http://localhost:8083/connector-plugins/$CLASS_NAME/config

curl -s http://localhost:8083/connectors/$CONNECTOR_NAME/config
curl -s http://localhost:8083/connectors/$CONNECTOR_NAME/status

curl -s http://localhost:8083/connectors/tasks
curl -s http://localhost:8083/connectors/$CONNECTOR_NAME/tasks
curl -s http://localhost:8083/connectors/$CONNECTOR_NAME/tasks/0/status

curl -s http://localhost:8083/connectors/topics
```

## Update connector configuration

```sh
curl -s -X PUT -H "Content-Type:application/json" http://localhost:8083/connectors/my-file-sink/config -d \
'{
  "connector.class": "FileStreamSink",
  "topics.regex": "my.*"
}'
```

## Stop, restart, pause connector and tasks

Restarting a connector doesn't restart the tasks.

```sh
curl -s -X POST -H "Content-Type:application/json" http://localhost:8083/connectors/my-file-sink/stop
curl -s -X POST -H "Content-Type:application/json" http://localhost:8083/connectors/my-file-sink/restart?includeTasks=true&onlyFailed=true
curl -s -X POST -H "Content-Type:application/json" http://localhost:8083/connectors/my-file-sink/tasks/0/restart
curl -s -X POST -H "Content-Type:application/json" http://localhost:8083/connectors/my-file-sink/pause
curl -s -X POST -H "Content-Type:application/json" http://localhost:8083/connectors/my-file-sink/resume
```

## Delete connector

```sh
curl -s -X DELETE http://localhost:8083/connectors/my-file-sink
```

## Change log level

```sh
curl -s http://localhost:8083/admin/loggers
curl -s -X PUT -H "Content-Type:application/json" http://localhost:8083/admin/loggers/org.apache.kafka.kafka?scope=cluster -d '{"level": "DEBUG"}'
```

## Cleanup

```sh
cd -
test-teardown
cleanup
```
