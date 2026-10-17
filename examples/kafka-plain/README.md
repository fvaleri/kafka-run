# Plain (KRaft)

KRaft cluster with 3 controllers and 3 brokers using PLAINTEXT.

## Start cluster

```sh
source examples/kafka-plain/init.sh
cd "$KAFKA_HOME"
```

## Check metadata

```sh
bin/kafka-dump-log.sh --cluster-metadata-decoder --files "$TEST_DIR"/server0/mdata/__cluster_metadata-0/*.log \
  | awk '!/NO_OP_RECORD/ && !/baseOffset:/'
```

## Run clients

```sh
bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic my-topic --partitions 5 --replication-factor 3
echo -e "hello\nworld" | bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic my-topic
bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic my-topic --from-beginning --max-messages 2
```

## Run rolling update

```sh
for i in {0..5}; do restart-node "$i"; done
```

## Add controller

```sh
declare -A server6=([id]="6" [rep]="6006" [jmx]="7006" [cid]="$(cat $TEST_DIR/server0/mdata/meta.properties | grep cluster.id | awk -F'=' '{print $2}')")
start-controller 6 "${server6[cid]}" "$(build-con-boot 0 2)" "$(build-init-con 0 2)"
sleep 5
bin/kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" --command-config "$TEST_DIR"/server"${server6[id]}"/config/server.properties add-controller
```

## Remove controller

```sh
declare -A server6=([id]="6" [rep]="6006" [jmx]="7006" [did]="$(cat $TEST_DIR/server0/mdata/meta.properties | grep directory.id | awk -F'=' '{print $2}')")
pkill -SIGTERM -ef "server6${ref[id]}"
bin/kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" remove-controller --controller-id "${server6[id]}" --controller-directory-id "${server6[did]}"
```

## Add broker

```sh
declare -A server7=([id]="7" [rep]="6007" [jmx]="7007" [cli]="9095" [cid]="$(cat $TEST_DIR/server4/data/meta.properties | grep cluster.id | awk -F'=' '{print $2}')")
start-broker 7 "${server7[cid]}" "$(build-con-boot 0 2)"
# Rebalance the cluster using Cruise Control after this
```

## Remove broker

```sh
# Move out all partitions from this broker using Cruise Control before this
declare -A server7=([id]="7" [rep]="6007" [jmx]="7007" [cli]="9095")
pkill -SIGTERM -ef "server7${ref[id]}"
bin/kafka-cluster.sh unregister --bootstrap-server "$BOOTSTRAP" --id "${server7[id]}"
# It takes some time to actually remove it from the cluster metadata
```

## Cleanup

```sh
cd -
test-teardown
cleanup
```
