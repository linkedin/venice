# Read APIs

Venice provides multiple client types optimized for different latency and resource trade-offs:

## Point-in-Time Clients

These clients read the current value for a given key:

| Client                                | Network Hops   | Typical Latency | Best For                                   |
| ------------------------------------- | -------------- | --------------- | ------------------------------------------ |
| [Thin Client](thin-client.md)         | 2 (via Router) | < 10ms          | Simple integration, moderate latency needs |
| [Fast Client](fast-client.md)         | 1 (direct)     | < 2ms           | Low latency with partition-aware routing   |
| [Da Vinci Client](da-vinci-client.md) | 0 (local)      | < 1ms           | Ultra-low latency with local caching       |

All point-in-time clients share the same read APIs (`get`, `batchGet`, `compute`), making migration straightforward.

## Change Data Capture

For streaming all changes to a store:

| Client               | Description                               | Best For                                            |
| -------------------- | ----------------------------------------- | --------------------------------------------------- |
| [CDC Client](cdc.md) | Stream of all mutations with after images | Derived systems, event-driven workflows, cache sync |

## Choosing a Client

**For point-in-time reads:**

- **Start simple**: Use the Thin Client for initial integration
- **Need lower latency**: Upgrade to Fast Client for direct server routing
- **Need sub-millisecond**: Use Da Vinci Client with local data storage

**For change streams:**

- **React to changes**: Use the CDC client to consume all mutations as they occur
