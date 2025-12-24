# Venice

![Venice Logo](assets/style/venice_full_lion_logo.svg)

**Derived Data Platform for Planet-Scale Workloads**

[![Stable Release](https://img.shields.io/docker/v/venicedb/venice-router?label=stable&color=green&logo=docker)](https://blog.venicedb.org/stable-releases)
[![CI](https://img.shields.io/github/actions/workflow/status/linkedin/venice/VeniceCI-StaticAnalysisAndUnitTests.yml)](https://github.com/linkedin/venice/actions)

---

Venice is a derived data storage platform providing:

1. High throughput asynchronous ingestion from batch and streaming sources
2. Low latency online reads via remote queries or in-process caching
3. Active-active replication between regions with CRDT-based conflict resolution
4. Multi-cluster support within each region with operator-driven cluster assignment
5. Multi-tenancy, horizontal scalability and elasticity within each cluster

**ML Feature Store:** Venice is particularly suitable as the stateful component backing a Feature Store, such as [Feathr](https://github.com/feathr-ai/feathr). AI applications feed the output of their ML training jobs into Venice and then query the data for use during online inference workloads.

## Architecture

Venice bridges offline, nearline and online worlds:

![Architecture](assets/images/high_level_architecture.drawio.svg)

## Installation

### Gradle
```groovy
repositories {
  mavenCentral()
  maven {
    name 'VeniceJFrog'
    url 'https://linkedin.jfrog.io/artifactory/venice'
  }
}

dependencies {
  implementation 'com.linkedin.venice:venice-client:0.4.455'
}
```

### Maven
```xml
<repositories>
  <repository>
    <id>venice-jfrog</id>
    <url>https://linkedin.jfrog.io/artifactory/venice</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.linkedin.venice</groupId>
    <artifactId>venice-client</artifactId>
    <version>0.4.455</version>
  </dependency>
</dependencies>
```

## APIs

![API Overview](assets/images/api_overview.drawio.svg)

### Write APIs

| Write Type | Batch (Hadoop) | Stream (Samza) | Online (HTTP) |
|------------|:--------------:|:--------------:|:-------------:|
| Full dataset swap | ✅ | ✅ | |
| Row insertion | ✅ | ✅ | ✅ |
| Column updates | ✅ | ✅ | ✅ |

[Write API docs →](user-guide/write-apis/)

### Read APIs

| Client Type | Network Hops | Latency (p99) | State |
|-------------|:------------:|:-------------:|:-----:|
| Thin Client | 2 | <10ms | Stateless |
| Fast Client | 1 | <2ms | Minimal |
| Da Vinci (SSD) | 0 | <1ms | Full dataset on SSD |
| Da Vinci (RAM) | 0 | <10μs | Full dataset in RAM |

[Read API docs →](user-guide/read-apis/)

## Resources

**Getting Started**
- [Quickstart Guide](getting-started/)
- [User Guide](user-guide/)
- [Operations Guide](operations/)

**Community**
- [Slack](http://slack.venicedb.org)
- [GitHub](https://github.com/linkedin/venice)
- [Blog](https://blog.venicedb.org)

**Learn More**
- [Videos & Talks](resources/learn-more/)
- [Contributing Guide](contributing/)
- [API Reference](resources/api-reference/)
