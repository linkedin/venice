package com.linkedin.venice.endToEnd;

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
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.SslUtils.LOCAL_KEYSTORE_JKS;
import static com.linkedin.venice.utils.SslUtils.LOCAL_PASSWORD;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.DaVinciUserApp;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.SeekableDaVinciClient;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientRecordTransformerFilterTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientRecordTransformerFilterTest.class);
  private static final int TEST_TIMEOUT = 120_000;
  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final String BASE_STORE_NAME = "test-store";
  private DaVinciClusterFixture fixture;
  private VeniceClusterWrapper cluster;
  private D2Client d2Client;

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
    props.setProperty("blob.transfer.server.port", Integer.toString(port1));
    props.setProperty("blob.transfer.client.port", Integer.toString(port2));
    props.setProperty("storage.class", StorageClass.DISK.toString());
    props.setProperty("record.transformer.enabled", "true");
    props.setProperty("blob.transfer.manager.enabled", "true");
    props.setProperty("batch.push.report.enabled", "false");
    File readyMarker = new File(configDir, "ready.marker");
    props.setProperty("ready.marker.path", readyMarker.getAbsolutePath());

    // Write properties to file
    try (FileWriter writer = new FileWriter(configFile)) {
      props.store(writer, null);
    }

    ForkedJavaProcess.exec(DaVinciUserApp.class, configFile.getAbsolutePath());

    // Poll until the forked DaVinci Client signals it is fully initialized
    // (ingestion complete + blob transfer server ready).
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, () -> {
      Assert.assertTrue(readyMarker.exists(), "DaVinciUserApp not yet ready");
    });

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

      // Test single-get access. The processPut() callbacks that populate the transformer's
      // in-memory map may still be in-flight after subscribeAll() returns, so retry until
      // all keys are available.
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        for (int k = 1; k <= DEFAULT_USER_DATA_RECORD_COUNT; ++k) {
          Object valueObj = client2.get(k).get();
          String expectedValue = "name " + k + "Transformed";
          assertEquals(valueObj.toString(), expectedValue);
          assertNotNull(recordTransformer.get(k), "processPut callback not yet fired for key " + k);
          assertEquals(recordTransformer.get(k), expectedValue);
        }
      });
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
  private void setUpStore(
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

  private void setUpStore(
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

  private static VeniceProperties buildRecordTransformerBackendConfig(boolean pushStatusStoreEnabled) {
    return DaVinciClientRecordTransformerTest.buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
  }

  private static void runVPJ(Properties vpjProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long vpjStart = System.currentTimeMillis();
    IntegrationTestPushUtils.runVPJ(vpjProperties);
    String storeName = (String) vpjProperties.get(VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
  }
}
