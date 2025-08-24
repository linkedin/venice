# AGENTS.md

## Navigating the Project

The Venice codebase is split across these directories:

- `clients`, which contains the user-facing libraries that most Venice users might be interested in. Those include:
    - `da-vinci-client`, which is the stateful client, providing "eager caching" for the Venice datasets. [Learn more](../user_guide/read_api/da_vinci_client.md).
    - `venice-admin-tool`, which is the shell tool intended for Venice operators.
    - `venice-client`, which is a one-stop-shop for all clients which an online application might need, including thin-client, fast-client, da-vinci-client, consumer.
    - `venice-producer`, which enables an application to perform real-time writes to Venice.
    - `venice-push-job`, which enables an offline job to push batch data to Venice.
    - `venice-thin-client`, which is the most minimal dependency one can get to issue remote reads to the Venice backend, by delegating as much of the query logic as possible to the Venice router tier.

- `integrations`, which contains additional libraries that some Venice users might be interested in, to connect Venice with other third-party systems. The rule of thumb for including a module in this directory is that it should have minimal Venice-specific logic, and be mostly just glue code to satisfy the contracts expected by the third-party system. Also, these modules are intended to minimize the dependency burden of the other client libraries. Those include:
    - `venice-beam`, which implements the Beam Read API, enabling a Beam job to consume the Venice changelog.
    - `venice-pulsar`, which contains an implementation of a Pulsar [Sink](https://pulsar.apache.org/docs/next/io-overview/#sink), in order to feed data from Pulsar topics to Venice.
    - `venice-samza`, which contains an implementation of a Samza [SystemProducer](https://samza.apache.org/learn/documentation/latest/api/javadocs/org/apache/samza/system/SystemProducer.html), in order to let Samza stream processing jobs emit writes to Venice.

- `internal`, which contains libraries not intended for public consumption. Those include:
    - `alpini`, which is a Netty-based framework used by the router service. It was forked from some code used by LinkedIn's proprietary [Espresso](https://engineering.linkedin.com/espresso/introducing-espresso-linkedins-hot-new-distributed-document-store) document store. At this time, Venice is the only user of this library, so there should be no concern of breaking compatibility with other dependents.
    - `venice-client-common`, which is a minimal set of APIs and utilities which the thin-client and other modules need to depend on. This module used to be named `venice-schema-common`, as one can see if digging into the git history.
    - `venice-common`, which is a larger set of APIs and utilities used by most other modules, except the thin-client.

- `services`, which contains the deployable components of Venice. Those include:
    - `venice-controller`, which acts as the control plane for Venice. Dataset creation, deletion and configuration, schema evolution, dataset to cluster assignment, and other such tasks, are all handled by the controller.
    - `venice-router`, which is the stateless tier responsible for routing thin-client queries to the correct server instance. It can also field certain read-only metadata queries such as cluster discovery and schema retrieval to take pressure away from the controller.
    - `venice-server`, which is the stateful tier responsible for hosting data, serving requests from both routers and fast-client library users, executing write operations and performing cross-region replication.

- `tests`, which contains some modules exclusively used for testing. Note that unit tests do not belong here, and those are instead located into each of the other modules above.

Besides code, the repository also contains:

- `all-modules`, which is merely an implementation detail of the Venice build and release pipeline. No code is expected to go here.
- `docker`, which contains our various docker files.
- `docs`, which contains the wiki you are currently reading.
- `gradle`, which contains various hooks and plugins used by our build system.
- `scripts`, which contains a few simple operational scripts that have not yet been folded into the venice-admin-tool.
- `specs`, which contains formal specifications, in TLA+ and FizzBee, for some aspects of the Venice architecture.


---

## Code Coverage Guide

The Venice repository has two code coverage checks on GitHub Action: **submodule-level coverage verification** and **new-commit coverage verification**, which aim to improve the testability and code quality overall. The GitHub Action fails the Pull Request if the coverage check does not meet the requirements. This guide provides the details on how these reports are generated and how to debug them.

### Module structure

Venice has 4 modules: `all-modules`, `clients`, `internal` and `services`. Each module has its own submodules, except `all-modules`.

#### Clients
`da-vinci-client`, `venice-admin-tool`, `venice-client`, `venice-push-job`, `venice-samza`, `venice-thin-client`

#### Internal
`alpini`, `venice-avro-compatibility-test`, `venice-client-common`, `venice-common`, `venice-consumer`, `venice-jdk-compatibility-test`, `venice-test-common`

#### Services
`venice-router`, `venice-controller`, `venice-server`

---

## Submodule-level coverage verification

- **What it does**:  
  Checks overall code coverage at the **submodule level**.  
  Jacoco generates a report, verifies branch coverage, and fails the build if below the threshold.  
  Threshold is defined in each submodule’s `build.gradle`.

- **Run**:
  ```bash
  ./gradlew :<module>:<submodule>:jacocoTestCoverageVerification
  ./gradlew :clients:venice-push-job:jacocoTestCoverageVerification
  ```

- **Report location**:  
  `<module>/<submodule>/build/reports/jacoco/test/index.html`

- **Debugging**:
    - Running against a module only (e.g. `:clients`) does **not** run unit tests and will not generate a report.  
      Always run against the submodule.



## New-commit coverage verification

- **What it does**:  
  Checks coverage for new/changed code only.  
  Uses DiffCoverage (extension of Jacoco) comparing local branch vs. upstream main.  

- **Run**:
  ```bash
  ./gradlew :<module>:<submodule>:jacocoTestCoverageVerification diffCoverage --continue
  ./gradlew :clients:venice-push-job:jacocoTestCoverageVerification diffCoverage --continue

# Coverage Report and Unit Testing Guidelines

## DiffCoverage Report

**Report location:**  
`<module>/build/reports/jacoco/diffCoverage/html/index.html`

### Debugging Notes

- Integration tests do not count — only unit tests are recognized.
- Jacoco report not up to date → always run tests and generate Jacoco report before DiffCoverage.
- Tests in the wrong module → Jacoco only analyzes the current submodule; tests must live in the same submodule as the code.
- Unrelated files showing → rebase/sync your branch with upstream main to fix diff.

---

## Unit Testing

We use **TestNG** for unit tests. Keep tests **Java 8 compatible**.  
Use **Mockito** for mocking.

- Do **not** use PowerMockito.
- Do **not** mock static methods.
- Do **not** use `@Mock` annotations. Create mocks explicitly with `Mockito.mock(...)`.

### Structure and Conventions

- Put tests next to the code under `src/test/java`.
- Name test classes `<ClassName>Test`.
- Use `@BeforeMethod` and `@AfterMethod` for setup and teardown.
- Prefer small, focused test methods. One behavior per test.
- Use timeouts on tests that could potentially hang or take a long time.

---

## Integration Testing

Integration tests in Venice verify end-to-end functionality across multiple components. They are located in `src/integrationTest/java` directories and use real Venice services rather than mocks.

### Test Structure and Location

- **Location**: `src/integrationTest/java` (separate from unit tests in `src/test/java`)
- **Framework**: TestNG with Java 8 compatibility
- **Naming**: Test classes should end with `Test` (e.g., `TestActiveActiveIngestion`)
- **Execution**: Run via `./gradlew integrationTest` or specific test tasks

### ServiceFactory - Central Test Infrastructure

The `ServiceFactory` class (`internal/venice-test-common/src/integrationTest/java/com/linkedin/venice/integration/utils/ServiceFactory.java`) is the central factory for creating Venice services and external dependencies used in integration tests.

#### Core Services

**Infrastructure Services:**
- `getZkServer()` - Creates ZooKeeper server instances
- `getPubSubBroker()` - Creates Kafka/PubSub broker instances with configurable options
- `getHelixController()` - Creates Helix controller for cluster management

**Venice Core Services:**
- `getVeniceController()` - Creates Venice controller instances (parent or child)
- `getVeniceServer()` - Creates Venice server instances with multi-fabric support
- `getVeniceRouter()` - Creates Venice router instances for request routing

**Cluster Management:**
- `getVeniceCluster()` - Creates complete Venice cluster setups
- `getVeniceMultiClusterWrapper()` - Creates multi-cluster environments
- `getVeniceTwoLayerMultiRegionMultiClusterWrapper()` - Creates complex multi-region setups

#### Client Creation

**DaVinci Clients:**
- `getGenericAvroDaVinciClient()` - Creates generic Avro DaVinci clients
- `getGenericAvroDaVinciClientWithRetries()` - Creates clients with retry logic
- `getGenericAvroDaVinciFactoryAndClientWithRetries()` - Creates factory and client with retries

#### Mock Services

**Testing Utilities:**
- `getMockAdminSparkServer()` - Creates mock admin servers for testing
- `getMockVeniceRouter()` - Creates mock router instances
- `getMockD2Server()` - Creates mock D2 service discovery servers
- `getMockHttpServer()` - Creates simple HTTP servers for testing

#### Process Isolation

**Multi-Process Testing:**
- `startVeniceClusterInAnotherProcess()` - Starts Venice cluster in separate process for maximum isolation
- `stopVeniceClusterInAnotherProcess()` - Stops external process clusters

#### Key Features

**Retry Logic:**
- Configurable retry attempts via `withMaxAttempt()`
- Automatic retry on service startup failures
- Comprehensive error handling with detailed logging

**Resource Management:**
- Automatic cleanup of failed service instances
- Process lifecycle management
- Memory and file descriptor monitoring

**Pluggable Broker Support:**
- Configurable PubSub broker factory via system property `pubSubBrokerFactory`
- Default Kafka implementation with extensibility for other message brokers

### Handling Non-Deterministic Assertions

**⚠️ CRITICAL**: Integration tests often involve asynchronous operations and eventual consistency. Always use wait-based assertions for non-deterministic results to avoid flaky tests.

#### Use TestUtils.waitForNonDeterministicAssertion()

```java
// ❌ BAD - Direct assertion on async result
Assert.assertTrue(client.isReady()); // May fail due to timing

// ✅ GOOD - Wait for async condition
TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
  Assert.assertTrue(client.isReady());
});
```

#### Common Scenarios Requiring Wait-Based Assertions

1. **Data Ingestion and Replication**:
   ```java
   // Wait for data to be ingested and replicated
   TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
     String value = client.get(key).get();
     Assert.assertEquals(value, expectedValue);
   });
   ```

2. **Store State Changes**:
   ```java
   // Wait for store to become ready
   TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
     Assert.assertEquals(admin.getStore(storeName).getCurrentVersion(), expectedVersion);
   });
   ```

3. **Cluster Topology Updates**:
   ```java
   // Wait for partition assignment
   TestUtils.waitForNonDeterministicAssertion(45, TimeUnit.SECONDS, () -> {
     Assert.assertTrue(cluster.getVeniceServers().get(0).isPartitionReady(storeName, partition));
   });
   ```

4. **Cross-Region Replication**:
   ```java
   // Wait for cross-region data propagation
   TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, () -> {
     String remoteValue = remoteClient.get(key).get();
     Assert.assertEquals(remoteValue, localValue);
   });
   ```

#### Best Practices for Reliable Tests

- **Use appropriate timeouts**: 30s for local operations, 60-120s for cross-region
- **Provide meaningful error messages**: Include context about what was being waited for
- **Test cleanup**: Always clean up resources in `@AfterMethod` or `@AfterClass`
- **Avoid Thread.sleep()**: Use wait-based assertions instead of fixed delays
- **Test isolation**: Each test should be independent and not rely on previous test state

### Usage Patterns

```java
// Basic cluster setup
VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options);

// Multi-region setup
VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegion = 
    ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(options);

// DaVinci client creation with proper wait
DaVinciClient<String, String> client = 
    ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster);

// Wait for client to be ready before using
TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
  Assert.assertTrue(client.isReady());
});

// Process isolation for benchmarks
ServiceFactory.startVeniceClusterInAnotherProcess(clusterInfoFilePath);
```

### Error Handling and Debugging

- **Log extensively**: Use detailed logging to understand test failures
- **Resource cleanup**: Ensure proper cleanup even when tests fail
- **Timeout tuning**: Adjust timeouts based on test environment (CI vs local)
- **Retry logic**: Use ServiceFactory's built-in retry mechanisms for service startup


