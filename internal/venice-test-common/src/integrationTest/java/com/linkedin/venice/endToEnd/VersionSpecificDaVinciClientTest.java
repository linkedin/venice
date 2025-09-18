package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.producer.online.OnlineProducerFactory;
import com.linkedin.venice.producer.online.OnlineVeniceProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class VersionSpecificDaVinciClientTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientRecordTransformerTest.class);
  private static final int TEST_TIMEOUT = 240_000;
  private VeniceClusterWrapper cluster;
  private D2Client d2Client;
  private static final String BASE_STORE_NAME = "test-store";

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
        .extraProperties(clusterConfig)
        .build();
    cluster = ServiceFactory.getVeniceCluster(options);
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testVersionSpecificDaVinciClient() throws Exception {
    DaVinciConfig clientConfig = new DaVinciConfig();
    String storeName = Utils.getUniqueString(BASE_STORE_NAME);
    boolean pushStatusStoreEnabled = true;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    String customValue = "1";
    int numKeys = 10;

    VeniceProperties backendConfig = buildBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();
    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy, customValue, numKeys);

    try (CachingDaVinciClientFactory factory = DaVinciTestContext.getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {

      DaVinciClient<Integer, Object> client =
          factory.getAndStartVersionSpecificGenericAvroClient(storeName, 1, clientConfig);
      client.subscribeAll().get();

      // Test single-get access for version 1
      validateKeys(client, customValue, numKeys);

      int streamingKey1 = numKeys + 1;
      try (OnlineVeniceProducer producer = OnlineProducerFactory.createProducer(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setD2Client(d2Client)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME),
          VeniceProperties.empty(),
          null)) {
        producer.asyncPut(streamingKey1, customValue).get();

        // Validate streaming key exists
        TestUtils.waitForNonDeterministicAssertion(
            10,
            TimeUnit.SECONDS,
            true,
            () -> assertEquals(client.get(streamingKey1).get(), Integer.getInteger(customValue)));

        int nextVersion = 2;
        runVenicePushJob(storeName, nextVersion, Integer.toString(nextVersion), numKeys, false);

        // Restart client to verify it can subscribe to the backup version on startup
        client.close();
        client.start();
        client.subscribeAll().get();

        // DaVinci should still see data for version 1
        validateKeys(client, customValue, numKeys);

        int streamingKey2 = streamingKey1 + 1;
        producer.asyncPut(streamingKey2, customValue).get();

        // Validate streaming key exists
        TestUtils.waitForNonDeterministicAssertion(
            10,
            TimeUnit.SECONDS,
            true,
            () -> assertEquals(client.get(streamingKey2).get(), Integer.getInteger(customValue)));

        // Run push job with deferred version swap, so it stays on future version
        nextVersion++;
        runVenicePushJob(storeName, nextVersion - 1, Integer.toString(nextVersion), numKeys, true);

        // DaVinci should still see data for version 1
        validateKeys(client, customValue, numKeys);

        int streamingKey3 = streamingKey2 + 1;
        producer.asyncPut(streamingKey3, customValue).get();

        // Streaming key should not exist because the topic is deleted
        // Wait 10 seconds to ensure the key doesn't appear
        Thread.sleep(10000);
        assertNull(client.get(streamingKey3).get());

        // Restarting client should cause an exception, because version is deleted
        client.close();
        client.start();
        assertThrows(VeniceClientException.class, () -> client.subscribeAll().get());
        client.close();

        // Have client subscribe to future version instead
        DaVinciClient<Integer, Object> client2 =
            factory.getAndStartVersionSpecificGenericAvroClient(storeName, nextVersion, clientConfig);
        client2.subscribeAll().get();

        // DaVinci should see data for future version
        validateKeys(client2, Integer.toString(nextVersion), numKeys);
      }
    }
  }

  private void validateKeys(DaVinciClient<Integer, Object> client, String customValue, int numKeys)
      throws ExecutionException, InterruptedException {
    for (int k = 1; k <= numKeys; ++k) {
      String value = client.get(k).get().toString();
      String expectedValue = customValue + k;

      assertEquals(value, expectedValue);
    }
  }

  private void runVenicePushJob(
      String storeName,
      int expectedVersion,
      String customValue,
      int numKeys,
      boolean deferVersionSwap) {
    File inputDir = getTempDataDirectory();
    try {
      writeSimpleAvroFileWithIntToStringSchema(inputDir, customValue, numKeys);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Setup VPJ job properties.
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties = defaultVPJProps(cluster, inputDirPath, storeName);
    vpjProperties.put(DEFER_VERSION_SWAP, deferVersionSwap);
    runVPJ(vpjProperties, expectedVersion, cluster);
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

  public VeniceProperties buildBackendConfig(boolean pushStatusStoreEnabled) {
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    PropertyBuilder backendPropertyBuilder = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, true)
        .put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, true);

    if (pushStatusStoreEnabled) {
      backendPropertyBuilder.put(PUSH_STATUS_STORE_ENABLED, true).put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000);
    }

    return backendPropertyBuilder.build();
  }
}
