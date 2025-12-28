# Venice Concepts

Core architecture and design principles.

![Architecture](../../assets/images/high_level_architecture.drawio.svg)

Venice combines the offline, nearline and online worlds, providing:

- **Batch ingestion** from Hadoop
- **Stream ingestion** from Kafka
- **Online writes** from Flink/Samza
- **Distributed storage** with active-active replication
- **Flexible reads** from stateless to stateful clients
