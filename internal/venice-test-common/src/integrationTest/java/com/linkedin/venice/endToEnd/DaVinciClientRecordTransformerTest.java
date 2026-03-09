package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_DELETE_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_DELETE_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_LATENCY;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToIntSchema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.producer.online.OnlineProducerFactory;
import com.linkedin.venice.producer.online.OnlineVeniceProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientRecordTransformerTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientRecordTransformerTest.class);
  private static final int TEST_TIMEOUT = 120_000;
  private VeniceClusterWrapper cluster;
  private D2Client d2Client;
  private static final String BASE_STORE_NAME = "test-store";
  private DaVinciClusterFixture fixture;

  @BeforeClass
  public void setUp() {
    fixture = new DaVinciClusterFixture();
    cluster = fixture.getCluster();
    d2Client = fixture.getD2Client();
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(fixture);
  }

  /*
   * Lower priority ensures that this test runs first. This is needed because this test fails if it doesn't go first.
   * It fails even when this class is set to single threaded, and we recreate the resources before every test.
   * It fails because the DVRT metrics get emitted, but they're not queryable from the metrics repository. The root
   * cause is unknown, but to mitigate for now we need this test to run first.
   */
  @Test(timeOut = TEST_TIMEOUT, priority = -1)
  public void testRecordTransformer() throws Exception {
    DaVinciConfig clientConfig = new DaVinciConfig();
    String storeName1 = Utils.getUniqueString(BASE_STORE_NAME);
    String storeName2 = Utils.getUniqueString(BASE_STORE_NAME);
    String recordTransformerStoreName = Utils.getUniqueString(BASE_STORE_NAME);
    boolean pushStatusStoreEnabled = false;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    String customValue = "a";
    int numKeys = 10;

    setUpStore(storeName1, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys, 3);
    setUpStore(storeName2, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys, 3);
    setUpStore(
        recordTransformerStoreName,
        pushStatusStoreEnabled,
        chunkingEnabled,
        compressionStrategy,
        customValue,
        numKeys,
        3);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = DaVinciTestContext.getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {

      DaVinciRecordTransformerConfig recordTransformerConfig =
          new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
              .build();
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(storeName1, new DaVinciConfig());
      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(recordTransformerStoreName, clientConfig);
      DaVinciClient<Integer, Object> client2 = factory.getAndStartGenericAvroClient(storeName2, new DaVinciConfig());

      CompletableFuture
          .allOf(client1.subscribeAll(), clientWithRecordTransformer.subscribeAll(), client2.subscribeAll())
          .join();

      // Test non-existent key access
      assertNull(clientWithRecordTransformer.get(numKeys + 1).get());

      // Test single-get access
      for (int k = 1; k <= numKeys; ++k) {
        String value1 = client1.get(k).get().toString();
        String value2 = client2.get(k).get().toString();
        String transformedValue = clientWithRecordTransformer.get(k).get().toString();

        String expectedValue = "a" + k;
        String expectedTransformedValue = expectedValue + "Transformed";

        assertEquals(value1, expectedValue);
        assertEquals(value2, expectedValue);
        assertEquals(transformedValue, expectedTransformedValue);
      }

      try (OnlineVeniceProducer producer = OnlineProducerFactory.createProducer(
          ClientConfig.defaultGenericClientConfig(recordTransformerStoreName)
              .setD2Client(d2Client)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME),
          VeniceProperties.empty(),
          null)) {
        producer.asyncDelete(1).get();

        // Validate metrics
        String metricPrefix = "." + recordTransformerStoreName + "_total--";
        String metricPostfix = "_avg_ms.DaVinciRecordTransformerStatsGauge";

        String deleteLatency = metricPrefix + RECORD_TRANSFORMER_DELETE_LATENCY + metricPostfix;

        TestUtils.waitForNonDeterministicAssertion(
            10,
            TimeUnit.SECONDS,
            true,
            () -> assertTrue(metricsRepository.getMetric(deleteLatency).value() > 0));

        // Exception should be thrown in the DVRT implementation when key doesn't exist
        // to test RECORD_TRANSFORMER_DELETE_ERROR_COUNT
        producer.asyncDelete(1).get();
        String deleteErrorCount =
            metricPrefix + RECORD_TRANSFORMER_DELETE_ERROR_COUNT + ".DaVinciRecordTransformerStatsGauge";
        TestUtils.waitForNonDeterministicAssertion(
            10,
            TimeUnit.SECONDS,
            true,
            () -> assertTrue(metricsRepository.getMetric(deleteErrorCount).value() == 1.0));

        clientWithRecordTransformer.unsubscribeAll();

        String putLatency = metricPrefix + RECORD_TRANSFORMER_PUT_LATENCY + metricPostfix;
        assertTrue(metricsRepository.getMetric(putLatency).value() > 0);

        String putErrorCount =
            metricPrefix + RECORD_TRANSFORMER_PUT_ERROR_COUNT + ".DaVinciRecordTransformerStatsGauge";
        assertEquals(metricsRepository.getMetric(putErrorCount).value(), 0.0);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testTypeChangeRecordTransformer() throws Exception {
    DaVinciConfig clientConfig = new DaVinciConfig();
    String storeName = Utils.getUniqueString(BASE_STORE_NAME);
    boolean pushStatusStoreEnabled = false;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    int numKeys = 10;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, numKeys);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = DaVinciTestContext.getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {

      DaVinciRecordTransformerConfig dummyRecordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
          .setRecordTransformerFunction(TestIntToStringRecordTransformer::new)
          .build();

      Schema myKeySchema = Schema.create(Schema.Type.INT);
      Schema myInputValueSchema = Schema.create(Schema.Type.INT);
      Schema myOutputValueSchema = Schema.create(Schema.Type.STRING);
      TestIntToStringRecordTransformer recordTransformer = new TestIntToStringRecordTransformer(
          storeName,
          1,
          myKeySchema,
          myInputValueSchema,
          myOutputValueSchema,
          dummyRecordTransformerConfig);

      DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
          .setRecordTransformerFunction(
              (
                  storeNameParam,
                  storeVersion,
                  keySchema,
                  inputValueSchema,
                  outputValueSchema,
                  config) -> recordTransformer)
          .setOutputValueClass(String.class)
          .setOutputValueSchema(Schema.create(Schema.Type.STRING))
          .build();
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      DaVinciClient<Integer, String> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig, String.class);

      // Test non-existent key access
      clientWithRecordTransformer.subscribeAll().get();
      assertNull(clientWithRecordTransformer.get(numKeys + 1).get());

      // Test single-get access
      for (int k = 1; k <= numKeys; ++k) {
        Object valueObj = clientWithRecordTransformer.get(k).get();
        String expectedValue = k + "Transformed";
        assertEquals(valueObj.toString(), expectedValue);
        assertEquals(recordTransformer.get(k), expectedValue);
      }

      /*
       * Simulates a client restart. During this process, the DVRT will use the on-disk state
       * to repopulate the inMemoryDB, avoiding the need for re-ingestion after clearing.
       */
      clientWithRecordTransformer.close();
      recordTransformer.clearInMemoryDB();
      assertTrue(recordTransformer.isInMemoryDBEmpty());

      clientWithRecordTransformer.start();
      clientWithRecordTransformer.subscribeAll().get();

      for (int k = 1; k <= numKeys; ++k) {
        Object valueObj = clientWithRecordTransformer.get(k).get();
        String expectedValue = k + "Transformed";
        assertEquals(valueObj.toString(), expectedValue);
        assertEquals(recordTransformer.get(k), expectedValue);
      }

      clientWithRecordTransformer.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testRecordTransformerOnRecovery() throws Exception {
    DaVinciConfig clientConfig = new DaVinciConfig();
    String storeName1 = Utils.getUniqueString(BASE_STORE_NAME);
    String storeName2 = Utils.getUniqueString(BASE_STORE_NAME);
    String recordTransformerStoreName = Utils.getUniqueString(BASE_STORE_NAME);
    boolean pushStatusStoreEnabled = true;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.GZIP;
    String customValue = "a";
    int numKeys = 10;

    setUpStore(storeName1, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys, 3);
    setUpStore(storeName2, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys, 3);
    setUpStore(
        recordTransformerStoreName,
        pushStatusStoreEnabled,
        chunkingEnabled,
        compressionStrategy,
        customValue,
        numKeys,
        3);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();
    clientConfig.setStorageClass(StorageClass.DISK);

    try (CachingDaVinciClientFactory factory = DaVinciTestContext.getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {

      Schema myKeySchema = Schema.create(Schema.Type.INT);
      Schema myValueSchema = Schema.create(Schema.Type.STRING);

      DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
          new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
              .build();

      TestStringRecordTransformer recordTransformer = new TestStringRecordTransformer(
          recordTransformerStoreName,
          1,
          myKeySchema,
          myValueSchema,
          myValueSchema,
          dummyRecordTransformerConfig);

      DaVinciRecordTransformerConfig recordTransformerConfig =
          new DaVinciRecordTransformerConfig.Builder()
              .setRecordTransformerFunction(
                  (
                      storeNameParam,
                      storeVersion,
                      keySchema,
                      inputValueSchema,
                      outputValueSchema,
                      config) -> recordTransformer)
              .build();
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(storeName1, new DaVinciConfig());
      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(recordTransformerStoreName, clientConfig);
      DaVinciClient<Integer, Object> client2 =
          factory.getAndStartGenericAvroClient(storeName2, new DaVinciConfig().setStorageClass(StorageClass.DISK));

      CompletableFuture
          .allOf(client1.subscribeAll(), clientWithRecordTransformer.subscribeAll(), client2.subscribeAll())
          .join();

      // Test non-existent key access
      assertNull(clientWithRecordTransformer.get(numKeys + 1).get());
      assertNull(recordTransformer.get(numKeys + 1));

      // Test single-get access
      for (int k = 1; k <= numKeys; ++k) {
        String value1 = client1.get(k).get().toString();
        String value2 = client2.get(k).get().toString();
        String transformedValue = clientWithRecordTransformer.get(k).get().toString();

        String expectedValue = "a" + k;
        String expectedTransformedValue = expectedValue + "Transformed";

        assertEquals(value1, expectedValue);
        assertEquals(value2, expectedValue);
        assertEquals(transformedValue, expectedTransformedValue);
      }

      /*
       * Simulates a client restart. During this process, the DVRT will use the on-disk state
       * to repopulate the inMemoryDB, avoiding the need for re-ingestion after clearing.
       */
      clientWithRecordTransformer.close();
      recordTransformer.clearInMemoryDB();
      assertTrue(recordTransformer.isInMemoryDBEmpty());

      client1.close();
      client2.close();

      // Submit online write to ensure writes are still possible after iterating over RocksDB
      try (OnlineVeniceProducer producer = OnlineProducerFactory.createProducer(
          ClientConfig.defaultGenericClientConfig(recordTransformerStoreName)
              .setD2Client(d2Client)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME),
          VeniceProperties.empty(),
          null)) {
        int key = numKeys + 1;
        String value = "a" + key;
        producer.asyncPut(key, value).get();
      }

      client1.start();
      clientWithRecordTransformer.start();
      client2.start();

      CompletableFuture
          .allOf(client1.subscribeAll(), clientWithRecordTransformer.subscribeAll(), client2.subscribeAll())
          .join();

      for (int k = 1; k <= numKeys + 1; ++k) {
        final int key = k;
        final String expectedValue = "a" + key;
        final String expectedTransformedValue = expectedValue + "Transformed";

        TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          Object transformedResult = clientWithRecordTransformer.get(key).get();
          assertNotNull(transformedResult, "Expected transformed value for key " + key + " to be non-null");
          assertEquals(transformedResult.toString(), expectedTransformedValue);

          if (key <= numKeys) {
            Object result1 = client1.get(key).get();
            Object result2 = client2.get(key).get();
            assertNotNull(result1, "Expected value1 for key " + key + " to be non-null");
            assertNotNull(result2, "Expected value2 for key " + key + " to be non-null");

            assertEquals(result1.toString(), expectedValue);
            assertEquals(result2.toString(), expectedValue);
          }
        });
      }

      client1.unsubscribeAll();
      clientWithRecordTransformer.unsubscribeAll();
      client2.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRecordTransformerChunking() throws Exception {
    DaVinciConfig clientConfig = new DaVinciConfig();
    // Construct a large string to trigger chunking
    int sizeInBytes = 2 * BYTES_PER_MB;
    StringBuilder stringBuilder = new StringBuilder(sizeInBytes);
    while (stringBuilder.length() < sizeInBytes) {
      stringBuilder.append("a");
    }
    String largeString = stringBuilder.toString();

    String storeName = Utils.getUniqueString(BASE_STORE_NAME);
    boolean pushStatusStoreEnabled = true;
    boolean chunkingEnabled = true;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    int numKeys = 10;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, largeString, numKeys, 3);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();
    clientConfig.setStorageClass(StorageClass.DISK);

    try (CachingDaVinciClientFactory factory = DaVinciTestContext.getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {

      Schema myKeySchema = Schema.create(Schema.Type.INT);
      Schema myValueSchema = Schema.create(Schema.Type.STRING);

      DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
          new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
              .build();

      TestStringRecordTransformer recordTransformer = new TestStringRecordTransformer(
          storeName,
          1,
          myKeySchema,
          myValueSchema,
          myValueSchema,
          dummyRecordTransformerConfig);

      DaVinciRecordTransformerConfig recordTransformerConfig =
          new DaVinciRecordTransformerConfig.Builder()
              .setRecordTransformerFunction(
                  (
                      storeNameParam,
                      storeVersion,
                      keySchema,
                      inputValueSchema,
                      outputValueSchema,
                      config) -> recordTransformer)
              .build();
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      DaVinciClient<Integer, String> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig);

      // Test non-existent key access
      clientWithRecordTransformer.subscribeAll().get();
      assertNull(clientWithRecordTransformer.get(numKeys + 1).get());
      assertNull(recordTransformer.get(numKeys + 1));

      // Test single-get access
      for (int k = 1; k <= numKeys; ++k) {
        Object valueObj = clientWithRecordTransformer.get(k).get();
        String expectedValue = largeString + k + "Transformed";
        assertEquals(valueObj.toString(), expectedValue);
        assertEquals(recordTransformer.get(k), expectedValue);
      }

      /*
       * Simulates a client restart. During this process, the DVRT will use the on-disk state
       * to repopulate the inMemoryDB, avoiding the need for re-ingestion after clearing.
       */
      clientWithRecordTransformer.close();
      recordTransformer.clearInMemoryDB();
      assertTrue(recordTransformer.isInMemoryDBEmpty());

      clientWithRecordTransformer.start();
      clientWithRecordTransformer.subscribeAll().get();

      for (int k = 1; k <= numKeys; ++k) {
        Object valueObj = clientWithRecordTransformer.get(k).get();
        String expectedValue = largeString + k + "Transformed";
        assertEquals(valueObj.toString(), expectedValue);
        assertEquals(recordTransformer.get(k), expectedValue);
      }

      clientWithRecordTransformer.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRecordTransformerWithEmptyDaVinci() throws Exception {
    DaVinciConfig clientConfig = new DaVinciConfig();
    String storeName = Utils.getUniqueString(BASE_STORE_NAME);
    boolean pushStatusStoreEnabled = false;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    String customValue = "a";
    int numKeys = 10;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys, 3);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();

    Schema myKeySchema = Schema.create(Schema.Type.INT);
    Schema myValueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setStoreRecordsInDaVinci(false)
            .build();

    TestStringRecordTransformer recordTransformer = new TestStringRecordTransformer(
        storeName,
        1,
        myKeySchema,
        myValueSchema,
        myValueSchema,
        dummyRecordTransformerConfig);

    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(
            (storeNameParam, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> recordTransformer)
        .setStoreRecordsInDaVinci(false)
        .build();
    clientConfig.setRecordTransformerConfig(recordTransformerConfig);

    try (CachingDaVinciClientFactory factory = DaVinciTestContext.getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig);

      clientWithRecordTransformer.subscribeAll().get();
      for (int k = 1; k <= numKeys; ++k) {
        // Record shouldn't be stored in Da Vinci
        assertNull(clientWithRecordTransformer.get(k).get());

        // Record should be stored in inMemoryDB
        String expectedValue = "a" + k + "Transformed";
        assertEquals(recordTransformer.get(k), expectedValue);
      }
      clientWithRecordTransformer.unsubscribeAll();
    }
  }

  /*
   * Batch data schema:
   * Key: Integer
   * Value: String
   */
  protected void setUpStore(
      String storeName,
      boolean useDVCPushStatusStore,
      boolean chunkingEnabled,
      CompressionStrategy compressionStrategy,
      String customValue,
      int numKeys,
      int numPartitions) {
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> {};
    Consumer<Properties> propertiesConsumer = properties -> {};

    File inputDir = getTempDataDirectory();

    Runnable writeAvroFileRunnable = () -> {
      try {
        writeSimpleAvroFileWithIntToStringSchema(inputDir, customValue, numKeys);
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
        inputDir,
        numPartitions,
        numKeys == 0);
  }

  protected void setUpStore(
      String storeName,
      Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<Properties> propertiesConsumer,
      boolean useDVCPushStatusStore,
      boolean chunkingEnabled,
      CompressionStrategy compressionStrategy,
      Runnable writeAvroFileRunnable,
      String valueSchema,
      File inputDir,
      int numPartitions,
      boolean emptyPush) {
    // Produce input data.
    writeAvroFileRunnable.run();

    // Setup VPJ job properties.
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties = defaultVPJProps(cluster, inputDirPath, storeName);
    propertiesConsumer.accept(vpjProperties);
    // Create & update store for test.
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setPartitionCount(numPartitions)
        .setChunkingEnabled(chunkingEnabled)
        .setCompressionStrategy(compressionStrategy)
        .setHybridOffsetLagThreshold(10)
        .setHybridRewindSeconds(1);

    paramsConsumer.accept(params);

    try (ControllerClient controllerClient =
        createStoreForJob(cluster, DEFAULT_KEY_SCHEMA, valueSchema, vpjProperties)) {
      cluster.createMetaSystemStore(storeName);
      if (useDVCPushStatusStore) {
        cluster.createPushStatusSystemStore(storeName);
      }
      TestUtils.assertCommand(controllerClient.updateStore(storeName, params));
      if (emptyPush) {
        controllerClient.sendEmptyPushAndWait(storeName, "test-push", 1, 30 * Time.MS_PER_SECOND);
      } else {
        runVPJ(vpjProperties, 1, cluster);
      }
    }
  }

  /*
   * Batch data schema:
   * Key: Integer
   * Value: Integer
   */
  protected void setUpStore(
      String storeName,
      boolean useDVCPushStatusStore,
      boolean chunkingEnabled,
      CompressionStrategy compressionStrategy,
      int numKeys) {
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> {};
    Consumer<Properties> propertiesConsumer = properties -> {};

    File inputDir = getTempDataDirectory();
    Runnable writeAvroFileRunnable = () -> {
      try {
        writeSimpleAvroFileWithIntToIntSchema(inputDir, numKeys);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
    String valueSchema = "\"int\"";
    setUpStore(
        storeName,
        paramsConsumer,
        propertiesConsumer,
        useDVCPushStatusStore,
        chunkingEnabled,
        compressionStrategy,
        writeAvroFileRunnable,
        valueSchema,
        inputDir,
        3,
        false);
  }

  private static void runVPJ(Properties vpjProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long vpjStart = System.currentTimeMillis();
    IntegrationTestPushUtils.runVPJ(vpjProperties);
    String storeName = (String) vpjProperties.get(VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
  }

  static VeniceProperties buildRecordTransformerBackendConfig(boolean pushStatusStoreEnabled) {
    return DaVinciClusterFixture.buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
  }
}
