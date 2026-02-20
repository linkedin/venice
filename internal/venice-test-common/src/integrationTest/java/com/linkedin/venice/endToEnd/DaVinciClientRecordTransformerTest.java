package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_DELETE_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_DELETE_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_LATENCY;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_SECURE_RANDOM_IMPLEMENTATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_TYPE;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_ACL_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.SslUtils.LOCAL_KEYSTORE_JKS;
import static com.linkedin.venice.utils.SslUtils.LOCAL_PASSWORD;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToIntSchema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.DaVinciUserApp;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.SeekableDaVinciClient;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.producer.online.OnlineProducerFactory;
import com.linkedin.venice.producer.online.OnlineVeniceProducer;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientRecordTransformerTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientRecordTransformerTest.class);
  private static final int TEST_TIMEOUT = 120_000;
  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testSkipResultRecordTransformer() throws Exception {
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
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestSkipResultRecordTransformer::new)
            .build();

    TestSkipResultRecordTransformer recordTransformer = new TestSkipResultRecordTransformer(
        storeName,
        1,
        myKeySchema,
        myValueSchema,
        myValueSchema,
        dummyRecordTransformerConfig);

    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(
            (storeNameParam, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> recordTransformer)
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testUnchangedResultRecordTransformer() throws Exception {
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

    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(TestUnchangedResultRecordTransformer::new)
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

  @Test(timeOut = 2 * TEST_TIMEOUT)
  public void testBlobTransferRecordTransformer() throws Exception {
    String dvcPath1 = Utils.getTempDataDirectory().getAbsolutePath();
    boolean pushStatusStoreEnabled = true;
    String zkHosts = cluster.getZk().getAddress();
    int port1 = TestUtils.getFreePort();
    int port2 = TestUtils.getFreePort();
    while (port1 == port2) {
      port2 = TestUtils.getFreePort();
    }
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> params.setBlobTransferEnabled(true);
    String storeName = Utils.getUniqueString(BASE_STORE_NAME);
    DaVinciConfig clientConfig = new DaVinciConfig();

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

    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(
            (storeNameParam, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> recordTransformer)
        .build();
    clientConfig.setRecordTransformerConfig(recordTransformerConfig);

    setUpStore(storeName, paramsConsumer, properties -> {}, pushStatusStoreEnabled, 3);
    LOGGER.info("Port1 is {}, Port2 is {}", port1, port2);
    LOGGER.info("zkHosts is {}", zkHosts);

    // Start the first DaVinci Client using DaVinciUserApp for regular ingestion
    File configDir = Utils.getTempDataDirectory();
    File configFile = new File(configDir, "dvc-config.properties");
    Properties props = new Properties();
    props.setProperty("zk.hosts", zkHosts);
    props.setProperty("base.data.path", dvcPath1);
    props.setProperty("store.name", storeName);
    props.setProperty("sleep.seconds", "100");
    props.setProperty("heartbeat.timeout.seconds", "10");
    props.setProperty("ingestion.isolation", "false");
    props.setProperty("blob.transfer.server.port", Integer.toString(port1));
    props.setProperty("blob.transfer.client.port", Integer.toString(port2));
    props.setProperty("storage.class", StorageClass.DISK.toString());
    props.setProperty("record.transformer.enabled", "true");
    props.setProperty("blob.transfer.manager.enabled", "true");
    props.setProperty("batch.push.report.enabled", "false");

    // Write properties to file
    try (FileWriter writer = new FileWriter(configFile)) {
      props.store(writer, null);
    }

    ForkedJavaProcess.exec(DaVinciUserApp.class, configFile.getAbsolutePath());

    // Wait for the first DaVinci Client to complete ingestion
    Thread.sleep(60000);

    // Start the second DaVinci Client using settings for blob transfer
    String dvcPath2 = Utils.getTempDataDirectory().getAbsolutePath();

    PropertyBuilder configBuilder = new PropertyBuilder().put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false")
        .put(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "3000")
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, dvcPath2)
        .put(DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, port2)
        .put(DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, port1)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 1)
        .put(BLOB_TRANSFER_MANAGER_ENABLED, true)
        .put(BLOB_TRANSFER_SSL_ENABLED, true)
        .put(BLOB_TRANSFER_ACL_ENABLED, true)
        .put(SSL_KEYSTORE_TYPE, "JKS")
        .put(SSL_KEYSTORE_LOCATION, SslUtils.getPathForResource(LOCAL_KEYSTORE_JKS))
        .put(SSL_KEYSTORE_PASSWORD, LOCAL_PASSWORD)
        .put(SSL_TRUSTSTORE_TYPE, "JKS")
        .put(SSL_TRUSTSTORE_LOCATION, SslUtils.getPathForResource(LOCAL_KEYSTORE_JKS))
        .put(SSL_TRUSTSTORE_PASSWORD, LOCAL_PASSWORD)
        .put(SSL_KEY_PASSWORD, LOCAL_PASSWORD)
        .put(SSL_KEYMANAGER_ALGORITHM, "SunX509")
        .put(SSL_TRUSTMANAGER_ALGORITHM, "SunX509")
        .put(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG");
    VeniceProperties backendConfig2 = configBuilder.build();

    try (CachingDaVinciClientFactory factory2 = DaVinciTestContext.getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        backendConfig2,
        cluster)) {
      DaVinciClient<Integer, Object> client2 = factory2.getAndStartGenericAvroClient(storeName, clientConfig);
      client2.subscribeAll().get();

      // Verify snapshots exists
      for (int i = 0; i < 3; i++) {
        String snapshotPath = RocksDBUtils.composeSnapshotDir(dvcPath1 + "/rocksdb", storeName + "_v1", i);
        Assert.assertTrue(Files.exists(Paths.get(snapshotPath)));
      }

      // All the records should have already been transformed due to blob transfer
      assertEquals(recordTransformer.getTransformInvocationCount(), 0);

      // Test single-get access
      for (int k = 1; k <= DEFAULT_USER_DATA_RECORD_COUNT; ++k) {
        Object valueObj = client2.get(k).get();
        String expectedValue = "name " + k + "Transformed";
        assertEquals(valueObj.toString(), expectedValue);
        assertEquals(recordTransformer.get(k), expectedValue);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRecordTransformerDaVinciWithSeeking() throws Exception {
    DaVinciConfig clientConfig = new DaVinciConfig();
    String storeName = Utils.getUniqueString(BASE_STORE_NAME);
    boolean pushStatusStoreEnabled = false;
    String customValue = "a";
    int numKeys = 10;

    setUpStore(storeName, false, false, CompressionStrategy.NO_OP, customValue, 0, 1);
    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM)) {
      for (int i = 0; i < numKeys; ++i) {
        String value = "a" + i;
        sendStreamingRecord(veniceProducer, storeName, i, value);
        Thread.sleep(500);
      }
    }
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
    List<DefaultPubSubMessage> messages = getDataMessages(storeName, numKeys);
    int messageIndex = 4;
    DefaultPubSubMessage pubSubMessage = messages.get(messageIndex);
    VeniceChangeCoordinate changeCoordinate = new VeniceChangeCoordinate(
        pubSubMessage.getTopic().getName(),
        pubSubMessage.getPosition(),
        pubSubMessage.getPartition());

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

      SeekableDaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericSeekableAvroClient(storeName, clientConfig);

      // test seek by change coordinate
      clientWithRecordTransformer.seekToCheckpoint(Collections.singleton(changeCoordinate)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (int k = 0; k < numKeys; ++k) {
          // Record shouldn't be stored in Da Vinci
          assertNull(clientWithRecordTransformer.get(k).get());
          // Record should be stored in inMemoryDB
          String expectedValue = "a" + k + "Transformed";
          assertEquals(recordTransformer.get(k), k < messageIndex ? null : expectedValue);
        }
      });
      clientWithRecordTransformer.unsubscribeAll();
      clientWithRecordTransformer.close();
      recordTransformer.clearInMemoryDB();
      clientWithRecordTransformer.start();

      // test seek by timestamp
      clientWithRecordTransformer.seekToTimestamp(pubSubMessage.getValue().producerMetadata.messageTimestamp).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (int k = 0; k < numKeys; ++k) {
          // Record shouldn't be stored in Da Vinci
          assertNull(clientWithRecordTransformer.get(k).get());
          // Record should be stored in inMemoryDB
          String expectedValue = "a" + k + "Transformed";
          assertEquals(recordTransformer.get(k), k < messageIndex ? null : expectedValue);
        }
      });
      clientWithRecordTransformer.unsubscribeAll();
      clientWithRecordTransformer.close();
      recordTransformer.clearInMemoryDB();
      clientWithRecordTransformer.start();
      // test seek to tail
      clientWithRecordTransformer.seekToTail().get();

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (int k = 1; k <= numKeys; ++k) {
          // Record shouldn't be stored in Da Vinci nor inMemoryDB
          assertNull(clientWithRecordTransformer.get(k).get());
          assertNull(recordTransformer.get(k));
        }
      });
    }
  }

  private List<DefaultPubSubMessage> getDataMessages(String storeName, int keyCount) {
    // Consume all the RT messages and validated how many data records were produced.
    PubSubBrokerWrapper pubSubBrokerWrapper = cluster.getPubSubBrokerWrapper();
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    List<DefaultPubSubMessage> messages = new ArrayList<>();

    try (PubSubConsumerAdapter pubSubConsumer = pubSubBrokerWrapper.getPubSubClientsFactory()
        .getConsumerAdapterFactory()
        .create(
            new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(properties))
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                .setConsumerName("testConsumer")
                .build())) {

      pubSubConsumer.subscribe(
          new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 1)), 0),
          PubSubSymbolicPosition.EARLIEST,
          false);
      while (messages.size() < keyCount) {
        Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polledResult =
            pubSubConsumer.poll(5000 * Time.MS_PER_SECOND);
        polledResult.get(
            new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 1)), 0))
            .stream()
            .filter(m -> !m.getKey().isControlMessage())
            .forEach(messages::add);
      }
    }
    return messages;
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

  /*
   * Batch data schema:
   * Key: Integer
   * Value: String
   */
  private void setUpStore(
      String storeName,
      Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<Properties> propertiesConsumer,
      boolean useDVCPushStatusStore,
      int numPartitions) {
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
        inputDir,
        numPartitions,
        false);
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

  public VeniceProperties buildRecordTransformerBackendConfig(boolean pushStatusStoreEnabled) {
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
