package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_HEARTBEAT_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DaVinciClientTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientTest.class);
  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 120_000;
  private static final String TEST_RECORD_VALUE_SCHEMA =
      "{\"type\":\"record\", \"name\":\"ValueRecord\", \"fields\": [{\"name\":\"number\", " + "\"type\":\"int\"}]}";
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testConcurrentGetAndStart() throws Exception {
    String s1 = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    String s2 = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);

    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    int totalIterations = 10;
    for (int i = 0; i < totalIterations; ++i) {
      MetricsRepository metricsRepository = new MetricsRepository();
      final int iteration = i + 1;
      try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
          d2Client,
          VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
          metricsRepository,
          backendConfig,
          cluster)) {
        DaVinciConfig c1 = new DaVinciConfig();
        DaVinciConfig c2 = new DaVinciConfig().setIsolated(true);
        BiFunction<String, DaVinciConfig, CompletableFuture<Void>> starter =
            (storeName, daVinciConfig) -> CompletableFuture.runAsync(() -> {
              try {
                factory.getGenericAvroClient(storeName, daVinciConfig).start();
                LOGGER.info(
                    "Successfully started DVC in iteration {}/{} for store '{}' with config: {}",
                    iteration,
                    totalIterations,
                    storeName,
                    daVinciConfig);
              } catch (Exception e) {
                LOGGER.warn(
                    "Caught exception while trying to start DVC in iteration {}/{} for store '{}' with config: {}",
                    iteration,
                    totalIterations,
                    storeName,
                    daVinciConfig);
                throw e;
              }
            });
        CompletableFuture
            .allOf(starter.apply(s1, c1), starter.apply(s2, c1), starter.apply(s1, c2), starter.apply(s2, c2))
            .get();
      } catch (Exception e) {
        throw new VeniceException("Failed to instantiate DVCs in iteration " + iteration + "/" + totalIterations, e);
      }
      assertThrows(NullPointerException.class, AvroGenericDaVinciClient::getBackend);
    }

    // Verify that multiple isolated clients to the same store can be started
    // successfully.
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setIsolated(true);
    MetricsRepository metricsRepository = new MetricsRepository();
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      factory.getAndStartGenericAvroClient(s1, daVinciConfig);
      factory.getAndStartGenericAvroClient(s1, daVinciConfig);
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testObjectReuse(DaVinciConfig clientConfig) throws Exception {
    final Schema schema = Schema.parse(TEST_RECORD_VALUE_SCHEMA);
    final GenericRecord value = new GenericData.Record(schema);
    value.put("number", 10);
    String storeName = cluster.createStore(KEY_COUNT, value);
    cluster.createMetaSystemStore(storeName);

    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        // TODO: Looks like cache = null does not work with fast meta store repository refresh interval
        // .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      DaVinciClient<Integer, Object> client = factory.getAndStartGenericAvroClient(storeName, clientConfig);

      GenericRecord reusableObject = new GenericData.Record(client.getLatestValueSchema());
      reusableObject.put("number", -1);
      // Test non-existent key access with a reusable object
      client.subscribeAll().get();
      assertNull(client.get(KEY_COUNT + 1, reusableObject).get());
      // A non-existing value should not get stored in the passed-in object
      assertEquals(reusableObject.get(0), -1);

      // Test single-get access
      for (int k = 0; k < KEY_COUNT; ++k) {
        // Verify returned value from the client
        assertEquals(((GenericRecord) client.get(k, reusableObject).get()).get(0), 10);
        // Verify value stores in the reused object
        if (clientConfig.isCacheEnabled()) {
          // object reuse doesn't work with object cache, so make sure it didn't try or it'll get weird
          assertEquals(reusableObject.get(0), -1);
        } else {
          assertEquals(reusableObject.get(0), 10);
          // reset the value
          reusableObject.put(0, -1);
        }
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "Isolated-Ingestion", dataProviderClass = DataProviderUtils.class)
  public void testStatusReportDuringBoostrap(IngestionMode ingestionMode) throws Exception {
    int keyCnt = 1000;
    String storeName = createStoreWithMetaSystemStoreAndPushStatusSystemStore(keyCnt);
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    Map<String, Object> extraBackendProp = new HashMap<>();
    extraBackendProp.put(DATA_BASE_PATH, baseDataPath);
    extraBackendProp.put(PUSH_STATUS_STORE_HEARTBEAT_INTERVAL_IN_SECONDS, "5");
    extraBackendProp.put(KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND, "5");
    extraBackendProp.put(PUSH_STATUS_STORE_ENABLED, "true");
    DaVinciTestContext<Integer, Object> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster,
            storeName,
            new DaVinciConfig().setIsolated(ingestionMode.equals(IngestionMode.ISOLATED)),
            extraBackendProp);
    try (DaVinciClient<Integer, Object> client = daVinciTestContext.getDaVinciClient()) {
      CompletableFuture<Void> subscribeFuture = client.subscribeAll();

      /**
       * Create a new version while bootstrapping.
       */
      VersionCreationResponse newVersion = cluster.getNewVersion(storeName);
      String topic = newVersion.getKafkaTopic();
      VeniceWriterFactory vwFactory = IntegrationTestPushUtils
          .getVeniceWriterFactory(cluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory);
      VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
      VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(DEFAULT_VALUE_SCHEMA);
      int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

      try (VeniceWriter<Object, Object, byte[]> batchProducer = vwFactory.createVeniceWriter(
          new VeniceWriterOptions.Builder(topic).setKeyPayloadSerializer(keySerializer)
              .setValuePayloadSerializer(valueSerializer)
              .build())) {
        batchProducer.broadcastStartOfPush(Collections.emptyMap());
        int keyCntForSecondVersion = 100;
        Future[] writerFutures = new Future[keyCntForSecondVersion];
        for (int i = 0; i < keyCntForSecondVersion; i++) {
          writerFutures[i] = batchProducer.put(i, i, valueSchemaId);
        }
        for (int i = 0; i < keyCntForSecondVersion; i++) {
          writerFutures[i].get();
        }
        batchProducer.broadcastEndOfPush(Collections.emptyMap());
      }
      subscribeFuture.get();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "CompressionStrategy")
  public void testReadCompressedData(CompressionStrategy compressionStrategy) throws Exception {
    String storeName = Utils.getUniqueString("batch-store");
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> params.setCompressionStrategy(compressionStrategy);
    setUpStore(storeName, paramsConsumer, properties -> {});
    try (DaVinciClient<Object, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
      client.subscribeAll().get();
      for (int i = 1; i <= 100; ++i) {
        Object value = client.get(i).get();
        Assert.assertEquals(value.toString(), "name " + i);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIsDavinciHeartbeatReported() throws Exception {
    // Setup store and create version 1
    String storeName = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000)
        .build();

    // Create dvc client and subscribe
    DaVinciClient<Object, Object> client =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig);
    client.subscribeAll().get();
    for (int k = 0; k < KEY_COUNT; ++k) {
      assertEquals(client.get(k).get(), 1);
    }

    // Check that dvc heartbeat is false as there was no dvc client during version 1's creation
    try (ControllerClient controllerClient = cluster.getControllerClient()) {
      StoreInfo store = controllerClient.getStore(storeName).getStore();
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MILLISECONDS, () -> {
        Assert.assertFalse(store.getIsDavinciHeartbeatReported());
        Assert.assertFalse(store.getVersion(1).get().getIsDavinciHeartbeatReported());
      });
    }

    // Create version 2
    Integer versionTwo = cluster.createVersion(storeName, KEY_COUNT);
    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client.get(k).get(), versionTwo);
      }
    });

    // Check that dvc heartbeat is true as there is a dvc client subscribed during version 2's creation
    try (ControllerClient controllerClient = cluster.getControllerClient()) {
      StoreInfo store = controllerClient.getStore(storeName).getStore();
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MILLISECONDS, () -> {
        Assert.assertTrue(store.getIsDavinciHeartbeatReported());
        Assert.assertTrue(store.getVersion(versionTwo).get().getIsDavinciHeartbeatReported());
      });
    }

    // Close the dvc client
    client.close();

    // Create version 3 and check that dvc heartbeat is false as the dvc client was closed
    Integer versionThree = cluster.createVersion(storeName, KEY_COUNT);
    try (ControllerClient controllerClient = cluster.getControllerClient()) {
      StoreInfo store = controllerClient.getStore(storeName).getStore();
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MILLISECONDS, () -> {
        Assert.assertFalse(store.getIsDavinciHeartbeatReported());
        Assert.assertFalse(store.getVersion(versionThree).get().getIsDavinciHeartbeatReported());
      });
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

  @DataProvider(name = "CompressionStrategy")
  public static Object[][] compressionStrategy() {
    return DataProviderUtils.allPermutationGenerator(DataProviderUtils.COMPRESSION_STRATEGIES);
  }
}
