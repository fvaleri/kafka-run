# Kafka Run

Examples for running Apache Kafka locally for development and testing.
Each example clones and builds Kafka from source.
Prerequisites: bash 4+, curl, git, tar, jq, java 11+.
The secure examples also require openssl and keytool (from Java JDK).

- [Plain (KRaft)](examples/kafka-plain/)
- [Plain (ZK)](examples/kafka-plain-zk/)
- [Secure (KRaft)](examples/kafka-secure/)
- [Connect (KRaft)](examples/kafka-connect/)
- [Connect (ZK)](examples/kafka-connect-zk/)
- [MirrorMaker 2 (KRaft)](examples/kafka-mm2/)
- [MirrorMaker 2 (ZK)](examples/kafka-mm2-zk/)
- [Tiered Storage](examples/kafka-tiered/)
- [Cluster Mirroring](examples/kafka-cm-plain/)
- [Cluster Mirroring Secure](examples/kafka-cm-secure/)
