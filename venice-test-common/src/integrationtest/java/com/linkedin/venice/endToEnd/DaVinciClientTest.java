package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.RemoteReadPolicy;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.IngestionIsolationMode;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;

import java.util.Properties;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.internal.thread.ThreadTimeoutException;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static org.testng.Assert.*;


public class DaVinciClientTest {
  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 60_000; // ms

  private VeniceClusterWrapper cluster;

  @BeforeClass
  public void setup() {
    Utils.thisIsLocalhost();
    // Reduce leader promotion delay to 3 seconds;
    Properties extraProperties = new Properties();
    extraProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    cluster = ServiceFactory.getVeniceCluster(1, 2, 1, 1,
        100, false, false, extraProperties);
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchStore() throws Exception {
    long memoryLimit = 1024 * 1024 * 1024; // 1GB

    String storeName1 = cluster.createStore(KEY_COUNT);
    String storeName2 = cluster.createStore(KEY_COUNT);

    String baseDataPath = TestUtils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder()
            .put(ConfigKeys.DATA_BASE_PATH, baseDataPath)
            .put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .build();

    D2Client d2Client = new D2ClientBuilder()
            .setZkHosts(cluster.getZk().getAddress())
            .setZkSessionTimeout(3, TimeUnit.SECONDS)
            .setZkStartupTimeout(3, TimeUnit.SECONDS)
            .build();
    D2ClientUtils.startClient(d2Client);

    MetricsRepository metricsRepository = new MetricsRepository();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setRocksDBMemoryLimit(memoryLimit);

    // Test multiple clients sharing the same ClientConfig/MetricsRepository & base data path
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(storeName1, daVinciConfig);
      Map<Integer, Integer> keyValueMap = new HashMap<>();
      client1.subscribeAll().get();

      double memoryUsage1 = metricsRepository.getMetric(".RocksDBMemoryStats--" + storeName1 + ".rocksdb.memory-usage.Gauge").value();
      assertNotEquals(memoryUsage1, 0);

      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client1.get(k).get(), 1);
        keyValueMap.put(k, 1);
      }

      // Test automatic new version ingestion
      DaVinciClient<Integer, Integer> client2 = factory.getAndStartGenericAvroClient(storeName2, daVinciConfig);
      client2.subscribeAll().get();

      assertEquals(client2.batchGet(keyValueMap.keySet()).get(), keyValueMap);

      for (int i = 0; i < 2; ++i) {
        // Test per-version partitioning parameters
        try (ControllerClient controllerClient = cluster.getControllerClient()) {
          ControllerResponse response = controllerClient.updateStore(
              storeName1,
              new UpdateStoreQueryParams()
                  .setPartitionerClass(ConstantVenicePartitioner.class.getName())
                  .setPartitionCount(i + 1)
                  .setPartitionerParams(
                      Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(i))
                  ));
          assertFalse(response.isError(), response.getError());
        }

        Integer expectedValue = cluster.createVersion(storeName1, KEY_COUNT);
        TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          for (int k = 0; k < KEY_COUNT; ++k) {
            assertEquals(client1.get(k).get(), expectedValue);
          }
        });
      }

      // Test multiple client ingesting different stores concurrently
      String storeName3 = cluster.createStore(KEY_COUNT);
      DaVinciClient<Integer, Integer> client3 = factory.getAndStartGenericAvroClient(storeName3, new DaVinciConfig());
      client3.subscribeAll().get();
      assertEquals(client3.batchGet(keyValueMap.keySet()).get(), keyValueMap);

      // Test read from a store that is being deleted concurrently
      try (ControllerClient controllerClient = cluster.getControllerClient()) {
        ControllerResponse response = controllerClient.disableAndDeleteStore(storeName3);
        assertFalse(response.isError(), response.getError());

        TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          assertThrows(VeniceNoStoreException.class, () -> client3.get(KEY_COUNT / 3).get());
        });
      }

      // Test data cleanup
      assertNotEquals(FileUtils.sizeOfDirectory(new File(baseDataPath)), 0);
      client1.unsubscribeAll();
      client2.unsubscribeAll();
      client3.unsubscribeAll();
      assertEquals(FileUtils.sizeOfDirectory(new File(baseDataPath)), 0);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDaVinciWithIngestionIsolation() throws Exception {
    final int partition = 1;
    final int partitionCount = 2;
    String storeName = TestUtils.getUniqueString("store");
    String storeName2 = cluster.createStore(KEY_COUNT);
    Consumer<UpdateStoreQueryParams> paramsConsumer =
            params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
                    .setPartitionCount(partitionCount)
                    .setPartitionerClass(ConstantVenicePartitioner.class.getName())
                    .setPartitionerParams(
                            Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition))
                    );
    setupHybridStore(storeName, paramsConsumer);

    D2Client d2Client = new D2ClientBuilder()
            .setZkHosts(cluster.getZk().getAddress())
            .setZkSessionTimeout(3, TimeUnit.SECONDS)
            .setZkStartupTimeout(3, TimeUnit.SECONDS)
            .build();
    D2ClientUtils.startClient(d2Client);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = TestUtils.getTempDataDirectory().getAbsolutePath();

    VeniceProperties backendConfig = new PropertyBuilder()
            .put(ConfigKeys.DATA_BASE_PATH, baseDataPath)
            .put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .put(ConfigKeys.SERVER_INGESTION_ISOLATION_MODE, IngestionIsolationMode.PARENT_CHILD)
            .build();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
      // subscribe to a partition without data
      int emptyPartition = (partition + 1) % partitionCount;
      client.subscribe(Collections.singleton(emptyPartition)).get();
      Utils.sleep(1000);
      for (int i = 0; i < KEY_COUNT; i++) {
        final int key = i;
        assertThrows(VeniceException.class, () -> client.get(key).get());
      }
      client.unsubscribe(Collections.singleton(emptyPartition));

      // subscribe to a partition with data
      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        Set<Integer> keySet = new HashSet<>();
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
          keySet.add(i);
        }
      });
    }
    // Restart Da Vinci client to test bootstrap logic.
    d2Client = new D2ClientBuilder()
        .setZkHosts(cluster.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
    metricsRepository = new MetricsRepository();
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        Set<Integer> keySet = new HashSet<>();
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
          keySet.add(i);
        }
      });
      // Make sure multiple clients can share same isolated ingestion service.
      DaVinciClient<Integer, Integer> client2 = factory.getAndStartGenericAvroClient(storeName2, new DaVinciConfig());
      client2.subscribeAll().get();
      for (int k = 0; k < KEY_COUNT; ++k) {
        int result = client2.get(k).get();
        assertEquals(result, 1);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testHybridStore() throws Exception {
    final int partition = 1;
    final int partitionCount = 2;
    String storeName = TestUtils.getUniqueString("store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionCount(partitionCount)
            .setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition))
            );
    setupHybridStore(storeName, paramsConsumer);

    try (DaVinciClient<Integer, Integer> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      // subscribe to a partition without data
      int emptyPartition = (partition + 1) % partitionCount;
      client.subscribe(Collections.singleton(emptyPartition)).get();
      Utils.sleep(1000);
      for (int i = 0; i < KEY_COUNT; i++) {
        final int key = i;
        assertThrows(VeniceException.class, () -> client.get(key).get());
      }
      client.unsubscribe(Collections.singleton(emptyPartition));

      // subscribe to a partition with data
      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        Set<Integer> keySet = new HashSet<>();
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
          keySet.add(i);
        }

        Map<Integer, Integer> valueMap = client.batchGet(keySet).get();
        assertNotNull(valueMap);
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(valueMap.get(i), i);
        }
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testAmplificationFactorInHybridStore()
      throws Exception {
    final int partition = 1;
    final int partitionCount = 2;
    final int amplificationFactor = 10;
    String storeName = TestUtils.getUniqueString("store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionCount(partitionCount)
            .setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setAmplificationFactor(amplificationFactor)
            .setLeaderFollowerModel(true)
            .setReplicationFactor(2)
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition))
            );
    setupHybridStore(storeName, paramsConsumer);
    try (DaVinciClient<Integer, Integer> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
        }
      });

      // unsubscribe to a partition with data
      client.unsubscribe(Collections.singleton(partition));
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          final int key = i;
          assertThrows(VeniceException.class, () -> client.get(key).get());
        }
      });

      // unsubscribe not subscribed partitions
      client.unsubscribe(Collections.singleton(0));

      // re-subscribe to a partition with data to test sub a unsub-ed partition
      client.subscribeAll().get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
        }
      });

      // test unsubscribe from all partitions
      client.unsubscribeAll();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          final int key = i;
          assertThrows(VeniceException.class, () -> client.get(key).get());
        }
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBootstrap() throws Exception {
    String storeName = cluster.createStore(KEY_COUNT);
    String baseDataPath = TestUtils.getTempDataDirectory().getAbsolutePath();
    try (DaVinciClient<Integer, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath)) {
      client.subscribeAll().get();
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client.get(k).get(), 1);
      }
    }

    try (DaVinciClient<Integer, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath)) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        try {
          Map<Integer, Integer> keyValueMap = new HashMap();
          for (int k = 0; k < KEY_COUNT; ++k) {
            assertEquals(client.get(k).get(), 1);
            keyValueMap.put(k, 1);
          }
          assertEquals(client.batchGet(keyValueMap.keySet()).get(), keyValueMap);
        } catch (VeniceException e) {
          throw new AssertionError("", e);
        }
      });
    }

    // Create a new version, so that old local version is removed during bootstrap and the access will fail.
    cluster.createVersion(storeName, KEY_COUNT);
    try (DaVinciClient<Integer, Integer> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath)) {
      assertThrows(VeniceException.class, () -> client.get(0).get());
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNonLocalRead() throws Exception {
    String storeName = cluster.createStore(KEY_COUNT);
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    VeniceProperties backendConfig = new PropertyBuilder()
            .put(ConfigKeys.DATA_BASE_PATH, TestUtils.getTempDataDirectory().getAbsolutePath())
            .put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .build();

    Map<Integer, Integer> keyValueMap = new HashMap<>();
    daVinciConfig.setRemoteReadPolicy(RemoteReadPolicy.QUERY_REMOTELY);
    try (DaVinciClient<Integer, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      client.subscribe(Collections.singleton(0)).get();
      // With QUERY_REMOTELY enabled, all key-value pairs should be found.
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client.get(k).get(), 1);
        keyValueMap.put(k, 1);
      }
      assertEquals(client.batchGet(keyValueMap.keySet()).get(), keyValueMap);
    }

    daVinciConfig.setRemoteReadPolicy(RemoteReadPolicy.FAIL_FAST);
    try (DaVinciClient<Integer, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      // We only subscribe to 1/3 of the partitions so some data will not be present locally.
      client.subscribe(Collections.singleton(0)).get();
      assertThrows(VeniceException.class, () -> client.batchGet(keyValueMap.keySet()).get());
    }

    // Update the store to use non-default partitioner
    try (ControllerClient client = cluster.getControllerClient()) {
      ControllerResponse response = client.updateStore(
          storeName,
          new UpdateStoreQueryParams()
              .setPartitionerClass(ConstantVenicePartitioner.class.getName())
              .setPartitionerParams(
                  Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(2))
              )
      );
      assertFalse(response.isError(), response.getError());
      cluster.createVersion(storeName, KEY_COUNT);
    }
    daVinciConfig.setRemoteReadPolicy(RemoteReadPolicy.QUERY_REMOTELY);
    try (DaVinciClient<Integer, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      client.subscribe(Collections.singleton(0)).get();
      // With QUERY_REMOTELY enabled, all key-value pairs should be found.
      assertEquals(client.batchGet(keyValueMap.keySet()).get().size(), keyValueMap.size());
    }
  }

  @Test(timeOut = TEST_TIMEOUT / 2, expectedExceptions = ThreadTimeoutException.class)
  // testRocksDBMemoryLimit will timeout since it can not ingest data due to memory limit
  public void testRocksDBMemoryLimit() throws Exception {
    String storeName = cluster.createStore(KEY_COUNT);

    VeniceProperties backendConfig = new PropertyBuilder()
        .put(ConfigKeys.DATA_BASE_PATH, TestUtils.getTempDataDirectory().getAbsolutePath())
        .put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
        .build();

    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setRocksDBMemoryLimit(1L);
    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      client.subscribeAll().get();
    }
  }

  private void setupHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer) throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams()
        .setHybridRewindSeconds(10)
        .setHybridOffsetLagThreshold(10);
    paramsConsumer.accept(params);
    try (ControllerClient client = cluster.getControllerClient()) {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      client.updateStore(storeName, params);
      cluster.createVersion(storeName, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, Stream.of());
      SystemProducer producer = TestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM,
          Pair.create(VeniceSystemFactory.VENICE_PARTITIONERS, ConstantVenicePartitioner.class.getName()));
      for (int i = 0; i < KEY_COUNT; i++) {
        TestPushUtils.sendStreamingRecord(producer, storeName, i, i);
      }
      producer.stop();
    }
  }
}
