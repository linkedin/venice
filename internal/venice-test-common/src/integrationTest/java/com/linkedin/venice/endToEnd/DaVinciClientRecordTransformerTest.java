package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToIntSchema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerFunctionalInterface;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DaVinciClientRecordTransformerTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientRecordTransformerTest.class);
  private static final int TEST_TIMEOUT = 120_000;
  private VeniceClusterWrapper cluster;
  private D2Client d2Client;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    clusterConfig.put(PUSH_STATUS_STORE_ENABLED, true);
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 3);
    cluster = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 100, false, false, clusterConfig);
    d2Client = new D2ClientBuilder().setZkHosts(cluster.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
  }

  @AfterClass
  public void cleanUp() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @BeforeMethod
  @AfterClass
  public void deleteClassHash() {
    int storeVersion = 1;
    File file = new File(String.format("./classHash-%d.txt", storeVersion));
    if (file.exists()) {
      assertTrue(file.delete());
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testRecordTransformer(DaVinciConfig clientConfig) throws Exception {
    String storeName = Utils.getUniqueString("test-store");
    boolean pushStatusStoreEnabled = false;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    String customValue = "a";
    int numKeys = 10;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {

      DaVinciRecordTransformerFunctionalInterface recordTransformerFunctionalInterface =
          (storeVersion, transformerConfig) -> new TestStringRecordTransformer(storeVersion, transformerConfig, true);
      DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
          recordTransformerFunctionalInterface,
          String.class,
          Schema.create(Schema.Type.STRING));
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig);

      // Test non-existent key access
      clientWithRecordTransformer.subscribeAll().get();
      assertNull(clientWithRecordTransformer.get(numKeys + 1).get());

      // Test single-get access
      for (int k = 1; k <= numKeys; ++k) {
        Object valueObj = clientWithRecordTransformer.get(k).get();
        String expectedValue = "a" + k + "Transformed";
        assertEquals(valueObj.toString(), expectedValue);
      }
      clientWithRecordTransformer.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testTypeChangeRecordTransformer(DaVinciConfig clientConfig) throws Exception {
    String storeName = Utils.getUniqueString("test-store");
    boolean pushStatusStoreEnabled = false;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    int numKeys = 10;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, numKeys);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
      DaVinciRecordTransformerFunctionalInterface recordTransformerFunctionalInterface = (
          storeVersion,
          transformerConfig) -> new TestIntToStringRecordTransformer(storeVersion, transformerConfig, true);
      DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
          recordTransformerFunctionalInterface,
          String.class,
          Schema.create(Schema.Type.STRING));
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
      }
      clientWithRecordTransformer.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testRecordTransformerOnRecovery(DaVinciConfig clientConfig) throws Exception {
    String storeName = Utils.getUniqueString("test-store");
    boolean pushStatusStoreEnabled = true;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.GZIP;
    String customValue = "a";
    int numKeys = 10;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();
    clientConfig.setStorageClass(StorageClass.DISK);

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {

      DaVinciRecordTransformerFunctionalInterface recordTransformerFunctionalInterface =
          (storeVersion, transformerConfig) -> new TestStringRecordTransformer(storeVersion, transformerConfig, true);
      DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
          recordTransformerFunctionalInterface,
          String.class,
          Schema.create(Schema.Type.STRING));
      recordTransformerConfig.setKeySchema(Schema.create(Schema.Type.INT));
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      TestStringRecordTransformer recordTransformer =
          (TestStringRecordTransformer) recordTransformerConfig.getRecordTransformer(1);

      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig);

      // Test non-existent key access
      clientWithRecordTransformer.subscribeAll().get();
      assertNull(clientWithRecordTransformer.get(numKeys + 1).get());
      assertNull(recordTransformer.get(numKeys + 1));

      // Test single-get access
      for (int k = 1; k <= numKeys; ++k) {
        Object valueObj = clientWithRecordTransformer.get(k).get();
        String expectedValue = "a" + k + "Transformed";
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
        String expectedValue = "a" + k + "Transformed";
        assertEquals(valueObj.toString(), expectedValue);
        assertEquals(recordTransformer.get(k), expectedValue);
      }

      clientWithRecordTransformer.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testRecordTransformerChunking(DaVinciConfig clientConfig) throws Exception {
    // Construct a large string to trigger chunking
    // (2MB = 2 * 1024 * 1024 bytes)
    int sizeInBytes = 2 * 1024 * 1024;
    StringBuilder stringBuilder = new StringBuilder(sizeInBytes);
    while (stringBuilder.length() < sizeInBytes) {
      stringBuilder.append("a");
    }
    String largeString = stringBuilder.toString();

    String storeName = Utils.getUniqueString("test-store");
    boolean pushStatusStoreEnabled = true;
    boolean chunkingEnabled = true;
    CompressionStrategy compressionStrategy = CompressionStrategy.GZIP;
    int numKeys = 10;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, largeString, numKeys);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();
    clientConfig.setStorageClass(StorageClass.DISK);

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {

      DaVinciRecordTransformerFunctionalInterface recordTransformerFunctionalInterface =
          (storeVersion, transformerConfig) -> new TestStringRecordTransformer(storeVersion, transformerConfig, true);
      DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
          recordTransformerFunctionalInterface,
          String.class,
          Schema.create(Schema.Type.STRING));
      recordTransformerConfig.setKeySchema(Schema.create(Schema.Type.INT));
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      TestStringRecordTransformer recordTransformer =
          (TestStringRecordTransformer) recordTransformerConfig.getRecordTransformer(1);

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

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testRecordTransformerWithEmptyDaVinci(DaVinciConfig clientConfig) throws Exception {
    String storeName = Utils.getUniqueString("test-store");
    boolean pushStatusStoreEnabled = false;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    String customValue = "a";
    int numKeys = 10;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();

    DaVinciRecordTransformerFunctionalInterface recordTransformerFunctionalInterface =
        (storeVersion, transformerConfig) -> new TestStringRecordTransformer(storeVersion, transformerConfig, false);
    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
        recordTransformerFunctionalInterface,
        String.class,
        Schema.create(Schema.Type.STRING));
    recordTransformerConfig.setKeySchema(Schema.create(Schema.Type.INT));
    clientConfig.setRecordTransformerConfig(recordTransformerConfig);

    TestStringRecordTransformer recordTransformer =
        (TestStringRecordTransformer) recordTransformerConfig.getRecordTransformer(1);

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
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

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testSkipResultRecordTransformer(DaVinciConfig clientConfig) throws Exception {
    String storeName = Utils.getUniqueString("test-store");
    boolean pushStatusStoreEnabled = false;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    String customValue = "a";
    int numKeys = 10;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();

    DaVinciRecordTransformerFunctionalInterface recordTransformerFunctionalInterface =
        (storeVersion, transformerConfig) -> new TestSkipResultRecordTransformer(storeVersion, transformerConfig, true);
    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
        recordTransformerFunctionalInterface,
        String.class,
        Schema.create(Schema.Type.STRING));
    recordTransformerConfig.setKeySchema(Schema.create(Schema.Type.INT));
    clientConfig.setRecordTransformerConfig(recordTransformerConfig);

    TestSkipResultRecordTransformer recordTransformer =
        (TestSkipResultRecordTransformer) recordTransformerConfig.getRecordTransformer(1);

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig);
      clientWithRecordTransformer.subscribeAll().get();

      /*
       * Since the record transformer is skipping over every record,
       * nothing should exist in Da Vinci or in the inMemoryDB.
       */
      assertTrue(recordTransformer.isInMemoryDBEmpty());
      for (int k = 1; k <= numKeys; ++k) {
        assertNull(clientWithRecordTransformer.get(k).get());
      }
      clientWithRecordTransformer.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testUnchangedResultRecordTransformer(DaVinciConfig clientConfig) throws Exception {
    String storeName = Utils.getUniqueString("test-store");
    boolean pushStatusStoreEnabled = false;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    String customValue = "a";
    int numKeys = 10;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();

    DaVinciRecordTransformerFunctionalInterface recordTransformerFunctionalInterface = (
        storeVersion,
        transformerConfig) -> new TestUnchangedResultRecordTransformer(storeVersion, transformerConfig, true);
    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
        recordTransformerFunctionalInterface,
        String.class,
        Schema.create(Schema.Type.STRING));
    clientConfig.setRecordTransformerConfig(recordTransformerConfig);

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig);

      // Test non-existent key access
      clientWithRecordTransformer.subscribeAll().get();
      assertNull(clientWithRecordTransformer.get(numKeys + 1).get());

      // Records shouldn't be transformed
      for (int k = 1; k <= numKeys; ++k) {
        Object valueObj = clientWithRecordTransformer.get(k).get();
        String expectedValue = "a" + k;
        assertEquals(valueObj.toString(), expectedValue);
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
      int numKeys) {
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
        inputDir);
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
        inputDir);
  }

  private static void runVPJ(Properties vpjProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long vpjStart = System.currentTimeMillis();
    String jobName = Utils.getUniqueString("batch-job-" + expectedVersionNumber);
    TestWriteUtils.runPushJob(jobName, vpjProperties);
    String storeName = (String) vpjProperties.get(VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
  }

  public VeniceProperties buildRecordTransformerBackendConfig(boolean pushStatusStoreEnabled) {
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    PropertyBuilder backendPropertyBuilder = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, true)
        .put(PUSH_STATUS_STORE_ENABLED, pushStatusStoreEnabled)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000);

    if (pushStatusStoreEnabled) {
      backendPropertyBuilder.put(PUSH_STATUS_STORE_ENABLED, true).put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000);
    }

    return backendPropertyBuilder.build();
  }
}
