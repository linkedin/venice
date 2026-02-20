# User Guide

This guide covers Venice's APIs for applications that need to read and write data.

![API Overview](../assets/images/api_overview.drawio.svg)

## Write APIs

Venice supports multiple ingestion patterns depending on your data freshness requirements:

- [Batch Push](write-apis/batch-push.md) - Full dataset replacement from Hadoop/Spark with zero-downtime version swaps
- [Stream Processor](write-apis/stream-processor.md) - Real-time updates via Apache Samza (requires hybrid stores)
- [Online Producer](write-apis/online-producer.md) - Direct writes from online services with durability guarantees

See [Write APIs Overview](write-apis/index.md) for details.

## Read APIs

Venice provides three client types with different latency/resource trade-offs:

| Client                                          | Latency | Network Hops   | Use Case           |
| ----------------------------------------------- | ------- | -------------- | ------------------ |
| [Thin Client](read-apis/thin-client.md)         | < 10ms  | 2 (via Router) | Simple integration |
| [Fast Client](read-apis/fast-client.md)         | < 2ms   | 1 (direct)     | Low latency        |
| [Da Vinci Client](read-apis/da-vinci-client.md) | < 1ms   | 0 (local)      | Ultra-low latency  |

See [Read APIs Overview](read-apis/index.md) for guidance on choosing a client.

## Additional Resources

For foundational concepts and deployment guides, see the [Getting Started](../getting-started/index.md) section.
