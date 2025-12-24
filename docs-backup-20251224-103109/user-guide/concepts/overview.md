# Venice Concepts

Core architecture and design principles.

![Architecture](../../assets/images/high_level_architecture.drawio.svg)

Venice straddles the offline, nearline and online worlds, providing:

- **Batch ingestion** from Hadoop
- **Stream ingestion** from Kafka/Samza
- **Online writes** via HTTP
- **Distributed storage** with active-active replication
- **Flexible reads** from stateless to stateful clients

For more details, see the [main documentation](../../index.md).
