package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_LATENCY;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_FULL_THRESHOLD;
import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.stats.ClientType.DAVINCI_CLIENT;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.StoreBackend;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.DiskLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientSubscribeTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientSubscribeTest.class);
  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 120_000;
  private VeniceClusterWrapper cluster;
  private D2Client d2Client;
  private PubSubProducerAdapterFactory pubSubProducerAdapterFactory;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(PUSH_STATUS_STORE_ENABLED, true);
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 3);
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(2)
        .numberOfRouters(1)
        .replicationFactor(2)
        .partitionSize(100)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(clusterConfig)
        .build();
    cluster = ServiceFactory.getVeniceCluster(options);
    d2Client = new D2ClientBuilder().setZkHosts(cluster.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    pubSubProducerAdapterFactory =
        cluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    D2ClientUtils.startClient(d2Client);
  }

  @AfterClass
  public void cleanUp() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testBatchStore(DaVinciConfig clientConfig) throws Exception {
    String storeName1 = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    String storeName2 = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    String storeName3 = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, true)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000)
        .build();

    MetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setServiceName(DAVINCI_CLIENT.getName())
            .setMetricPrefix(DAVINCI_CLIENT.getMetricsPrefix())
            .setEmitOtelMetrics(true)
            .setMetricEntities(CLIENT_METRIC_ENTITIES)
            .build());

    // Test multiple clients sharing the same ClientConfig/MetricsRepository & base data path
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(storeName1, clientConfig);

      // Test non-existent key access
      client1.subscribeAll().get();
      assertNull(client1.get(KEY_COUNT + 1).get());

      // Test single-get access
      Map<Integer, Integer> keyValueMap = new HashMap<>();
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client1.get(k).get(), 1);
        keyValueMap.put(k, 1);
      }

      // Test batch-get access
      assertEquals(client1.batchGet(keyValueMap.keySet()).get(), keyValueMap);

      // Test automatic new version ingestion
      for (int i = 0; i < 2; ++i) {
        // Test per-version partitioning parameters
        int partitionCount = i + 1;
        String iString = String.valueOf(i);
        cluster.useControllerClient(controllerClient -> {
          ControllerResponse response = controllerClient.updateStore(
              storeName1,
              new UpdateStoreQueryParams().setPartitionerClass(ConstantVenicePartitioner.class.getName())
                  .setPartitionCount(partitionCount)
                  .setPartitionerParams(
                      Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, iString)));
          assertFalse(response.isError(), response.getError());
        });

        Integer expectedValue = cluster.createVersion(storeName1, KEY_COUNT);
        TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          for (int k = 0; k < KEY_COUNT; ++k) {
            Object readValue = client1.get(k).get();
            assertEquals(readValue, expectedValue);
          }
        });
      }

      // Test multiple client ingesting different stores concurrently
      DaVinciClient<Integer, Integer> client2 = factory.getAndStartGenericAvroClient(storeName2, clientConfig);
      DaVinciClient<Integer, Integer> client3 = factory.getAndStartGenericAvroClient(storeName3, clientConfig);
      CompletableFuture.allOf(client2.subscribeAll(), client3.subscribeAll()).get();
      assertEquals(client2.batchGet(keyValueMap.keySet()).get(), keyValueMap);
      assertEquals(client3.batchGet(keyValueMap.keySet()).get(), keyValueMap);

      // TODO(jlliu): Re-enable this test-case after fixing store deletion that is flaky due to
      // CLIENT_USE_SYSTEM_STORE_REPOSITORY.
      // // Test read from a store that is being deleted concurrently
      // try (ControllerClient controllerClient = cluster.getControllerClient()) {
      // ControllerResponse response = controllerClient.disableAndDeleteStore(storeName2);
      // assertFalse(response.isError(), response.getError());
      // TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      // assertThrows(VeniceClientException.class, () -> client2.get(KEY_COUNT / 3).get());
      // });
      // }
      client2.unsubscribeAll();

      // DVRT metrics shouldn't be registered if DVRT isn't enabled
      String putLatency = String.format(
          ".%s_total--%s_avg_ms.DaVinciRecordTransformerStatsGauge",
          storeName1,
          RECORD_TRANSFORMER_PUT_LATENCY);
      assertNull(metricsRepository.getMetric(putLatency));
    }

    // Test bootstrap-time junk removal
    cluster.useControllerClient(controllerClient -> {
      ControllerResponse response = controllerClient.disableAndDeleteStore(storeName3);
      assertFalse(response.isError(), response.getError());
    });

    // Test managed clients & data cleanup
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        backendConfig,
        cluster,
        Optional.of(Collections.singleton(storeName1)))) {
      assertNotEquals(FileUtils.sizeOfDirectory(new File(baseDataPath)), 0);

      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(storeName1, clientConfig);
      client1.subscribeAll().get();
      client1.unsubscribeAll();
      // client2 was removed explicitly above via disableAndDeleteStore()
      // client3 is expected to be removed by the factory during bootstrap
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        assertEquals(FileUtils.sizeOfDirectory(new File(baseDataPath)), 0);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testBootstrap(DaVinciConfig daVinciConfig) throws Exception {
    String storeName = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    try (DaVinciClient<Integer, Object> client =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath)) {
      client.subscribeAll().get();
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client.get(k).get(), 1);
      }
    }

    // Since the previous DaVinci client is closed, the static default Gauge metric measurement thread pool is also
    // shutdown. In order to continue calculating Gauge metrics values in the new client, create a new thread pool
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository(10000, 50);
    Map<String, Object> extraProps = new HashMap<>();
    extraProps.put(DATA_BASE_PATH, baseDataPath);
    DaVinciTestContext<Integer, Object> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            metricsRepository,
            Optional.empty(),
            cluster,
            storeName,
            daVinciConfig,
            extraProps);
    try (DaVinciClient<Integer, Object> client = daVinciTestContext.getDaVinciClient()) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        try {
          Map<Integer, Integer> keyValueMap = new HashMap<>();
          for (int k = 0; k < KEY_COUNT; ++k) {
            assertEquals(client.get(k).get(), 1);
            keyValueMap.put(k, 1);
          }
          assertEquals(client.batchGet(keyValueMap.keySet()).get(), keyValueMap);
        } catch (VeniceException e) {
          throw new AssertionError("", e);
        }
      });
      // After restart, Da Vinci client will still get correct metrics for ingested stores.
      String metricName = "." + storeName + "_current--disk_usage_in_bytes.Gauge";
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        Metric storeDiskUsageMetric = metricsRepository.getMetric(metricName);
        Assert.assertNotNull(storeDiskUsageMetric);
        Assert.assertTrue(storeDiskUsageMetric.value() > 0);
      });
    }

    daVinciConfig.setStorageClass(StorageClass.DISK);
    // Try to open the Da Vinci client with different storage class.
    try (DaVinciClient<Integer, Integer> client =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath, daVinciConfig)) {
      client.subscribeAll().get();
    }

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory =
        IntegrationTestPushUtils.getVeniceWriterFactory(cluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory);
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(DEFAULT_VALUE_SCHEMA);
    int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    try (VeniceWriter<Object, Object, byte[]> batchProducer = vwFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(topic).setKeyPayloadSerializer(keySerializer)
            .setValuePayloadSerializer(valueSerializer)
            .build())) {
      batchProducer.broadcastStartOfPush(Collections.emptyMap());
      Future[] writerFutures = new Future[KEY_COUNT];
      for (int i = 0; i < KEY_COUNT; i++) {
        writerFutures[i] = batchProducer.put(i, i, valueSchemaId);
      }
      for (int i = 0; i < KEY_COUNT; i++) {
        writerFutures[i].get();
      }
      /**
       * Creating a stuck VPJ here so the new version will not be fully ingested. Da Vinci bootstrap should continue to
       * subscribe to existing CURRENT VERSION pushed before.
       */
      try (DaVinciClient<Integer, Integer> client =
          ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath, daVinciConfig)) {
        TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, false, true, () -> {
          for (int i = 0; i < KEY_COUNT; i++) {
            int value = client.get(i).get();
            assertEquals(value, 1);
          }
        });
      }
      batchProducer.broadcastEndOfPush(Collections.emptyMap());
    }

    /**
     * Push a new version as the CURRENT VERSION, so that old local version is removed during bootstrap and the access will fail.
     */
    cluster.createVersion(storeName, KEY_COUNT);
    try (DaVinciClient<Integer, Integer> client =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath, daVinciConfig)) {
      assertThrows(VeniceException.class, () -> client.get(0).get());
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testBootstrapSubscription(DaVinciConfig daVinciConfig) throws Exception {
    String storeName1 = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, true)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, false)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    // Test multiple clients sharing the same ClientConfig/MetricsRepository & base data path
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(storeName1, daVinciConfig);

      // Test non-existent key access
      client1.subscribeAll().get();
      assertNull(client1.get(KEY_COUNT + 1).get());

      // Test single-get access
      Map<Integer, Integer> keyValueMap = new HashMap<>();
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client1.get(k).get(), 1);
        keyValueMap.put(k, 1);
      }

      // Test batch-get access
      assertEquals(client1.batchGet(keyValueMap.keySet()).get(), keyValueMap);
    }

    // Test managed clients
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster,
        Optional.of(Collections.singleton(storeName1)))) {

      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(storeName1, daVinciConfig);

      Set<Integer> partitions = new HashSet<>();

      for (int i = 0; i < 2; i++) {
        partitions.add(i);
      }

      client1.subscribe(partitions);
      assertEquals(client1.getPartitionCount(), 3);

      DaVinciBackend daVinciBackend = AvroGenericDaVinciClient.getBackend();
      if (daVinciBackend != null) {
        StoreBackend storeBackend = daVinciBackend.getStoreOrThrow(storeName1);
        ComplementSet<Integer> subscription = storeBackend.getSubscription();
        assertTrue(subscription.contains(0));
        assertTrue(subscription.contains(1));
        assertFalse(subscription.contains(2));
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testPartialSubscription(DaVinciConfig daVinciConfig) throws Exception {
    String storeName = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    VeniceProperties backendConfig =
        new PropertyBuilder().put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L).build();

    Set<Integer> keySet = new HashSet<>();
    for (int i = 0; i < KEY_COUNT; ++i) {
      keySet.add(i);
    }

    try (DaVinciClient<Integer, Object> client =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      // We only subscribe to 1/3 of the partitions so some data will not be present locally.
      client.subscribe(Collections.singleton(0)).get();
      assertThrows(() -> client.batchGet(keySet).get());
    }

    // Update the store to use non-default partitioner
    cluster.useControllerClient(
        controllerClient -> TestUtils.assertCommand(
            controllerClient.updateStore(
                storeName,
                new UpdateStoreQueryParams().setPartitionerClass(ConstantVenicePartitioner.class.getName())
                    .setPartitionerParams(
                        Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(2))))));
    cluster.createVersion(storeName, KEY_COUNT);
    try (DaVinciClient<Integer, Object> client =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      // Only subscribe a subset of the partitions
      client.subscribe(Collections.singleton(0)).get();
      assertThrows(() -> client.batchGet(keySet).get());
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSubscribeAndUnsubscribe() throws Exception {
    // Verify DaVinci client doesn't hang in a deadlock when calling unsubscribe right after subscribing.
    // Enable ingestion isolation since it's more likely for the race condition to occur and make sure the future is
    // only completed when the main process's ingestion task is subscribed to avoid deadlock.
    String storeName = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    DaVinciConfig daVinciConfig = new DaVinciConfig();

    Map<String, Object> extraConfigMap = TestUtils.getIngestionIsolationPropertyMap();
    DaVinciTestContext<String, GenericRecord> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster,
            storeName,
            daVinciConfig,
            extraConfigMap);

    try (CachingDaVinciClientFactory ignored = daVinciTestContext.getDaVinciClientFactory()) {
      DaVinciClient<String, GenericRecord> client = daVinciTestContext.getDaVinciClient();
      client.subscribeAll().get();
      client.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUnsubscribeBeforeFutureGet() throws Exception {
    // Verify DaVinci client doesn't hang in a deadlock when calling unsubscribe right after subscribing and before the
    // future is complete. The future should also return exceptionally.
    String storeName = createStoreWithMetaSystemStoreAndPushStatusSystemStore(10000); // A large amount of keys to give
    // window for potential
    // race conditions
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    Map<String, Object> extraConfigMap = TestUtils.getIngestionIsolationPropertyMap();
    DaVinciTestContext<String, GenericRecord> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster,
            storeName,
            daVinciConfig,
            extraConfigMap);

    try (CachingDaVinciClientFactory ignored = daVinciTestContext.getDaVinciClientFactory()) {
      DaVinciClient<String, GenericRecord> client = daVinciTestContext.getDaVinciClient();
      CompletableFuture<Void> future = client.subscribeAll();
      client.unsubscribeAll();
      future.get(); // Expecting exception here if we unsubscribed before subscribe was completed.
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof CancellationException);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDavinciSubscribeFailureWithFullDisk() throws Exception {
    String storeName = Utils.getUniqueString("test-davinci-store");
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> {};
    setUpStore(storeName, paramsConsumer, properties -> {});

    Map<String, Object> backendConfigMap = new HashMap<>(cluster.getPubSubClientProperties());
    backendConfigMap.put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true);
    backendConfigMap.put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 10);
    backendConfigMap.put(SERVER_DISK_FULL_THRESHOLD, 0.01); // force it to fail

    try (DaVinciClient<Integer, Integer> daVinciClient = ServiceFactory.getGenericAvroDaVinciClientWithRetries(
        storeName,
        cluster.getZk().getAddress(),
        new DaVinciConfig(),
        backendConfigMap)) {
      daVinciClient.subscribeAll().get();
      fail("should fail with disk full exception");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof DiskLimitExhaustedException);
    }
  }

  /*
   * Batch data schema:
   * Key: Integer
   * Value: String
   */
  private void setUpStore(
      String storeName,
      Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<Properties> propertiesConsumer) throws Exception {
    setUpStore(storeName, paramsConsumer, propertiesConsumer, false);
  }

  /*
   * Batch data schema:
   * Key: Integer
   * Value: String
   */
  private void setUpStore(
      String storeName,
      Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<Properties> propertiesConsumer,
      boolean useDVCPushStatusStore) {
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

    File inputDir = getTempDataDirectory();

    Runnable writeAvroFileRunnable = () -> {
      try {
        writeSimpleAvroFileWithIntToStringSchema(inputDir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
    String valueSchema = "\"string\"";
    setUpStore(
        storeName,
        paramsConsumer,
        propertiesConsumer,
        useDVCPushStatusStore,
        chunkingEnabled,
        compressionStrategy,
        writeAvroFileRunnable,
        valueSchema,
        inputDir);
  }

  private void setUpStore(
      String storeName,
      Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<Properties> propertiesConsumer,
      boolean useDVCPushStatusStore,
      boolean chunkingEnabled,
      CompressionStrategy compressionStrategy,
      Runnable writeAvroFileRunnable,
      String valueSchema,
      File inputDir) {
    // Produce input data.
    writeAvroFileRunnable.run();

    // Setup VPJ job properties.
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties = defaultVPJProps(cluster, inputDirPath, storeName);
    propertiesConsumer.accept(vpjProperties);
    // Create & update store for test.
    final int numPartitions = 3;
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setPartitionCount(numPartitions)
        .setChunkingEnabled(chunkingEnabled)
        .setCompressionStrategy(compressionStrategy);

    paramsConsumer.accept(params);

    try (ControllerClient controllerClient =
        createStoreForJob(cluster, DEFAULT_KEY_SCHEMA, valueSchema, vpjProperties)) {
      cluster.createMetaSystemStore(storeName);
      if (useDVCPushStatusStore) {
        cluster.createPushStatusSystemStore(storeName);
      }
      TestUtils.assertCommand(controllerClient.updateStore(storeName, params));
      runVPJ(vpjProperties, 1, cluster);
    }
  }

  private static void runVPJ(Properties vpjProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long vpjStart = System.currentTimeMillis();
    IntegrationTestPushUtils.runVPJ(vpjProperties);
    String storeName = (String) vpjProperties.get(VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
  }

  private String createStoreWithMetaSystemStoreAndPushStatusSystemStore(int keyCount) throws Exception {
    String storeName = cluster.createStore(keyCount);
    cluster.createMetaSystemStore(storeName);
    cluster.createPushStatusSystemStore(storeName);
    return storeName;
  }
}
