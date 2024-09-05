package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.D2_ZK_HOSTS_ADDRESS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_FULL_THRESHOLD;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SERVICE_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.DaVinciUserApp;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.NonLocalAccessException;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.DiskLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.IngestionMetadataUpdateType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
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
  private File inputDir;
  private String inputDirPath;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    inputDir = getTempDataDirectory();
    inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties clusterConfig = new Properties();
    clusterConfig.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    cluster = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 100, false, false, clusterConfig);
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
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    int totalIterations = 10;
    for (int i = 0; i < totalIterations; ++i) {
      MetricsRepository metricsRepository = new MetricsRepository();
      final int iteration = i + 1;
      try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
          d2Client,
          VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
          metricsRepository,
          backendConfig)) {
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
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
      factory.getAndStartGenericAvroClient(s1, daVinciConfig);
      factory.getAndStartGenericAvroClient(s1, daVinciConfig);
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testBatchStore(DaVinciConfig clientConfig) throws Exception {
    String storeName1 =
        createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT, CompressionStrategy.GZIP, s -> null);
    String storeName2 = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    String storeName3 = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, true)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    // Test record transformation
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName1, clientConfig.setRecordTransformerFunction((storeVersion) -> {
            TestRecordTransformer recordTransformer = new TestRecordTransformer(storeVersion);
            recordTransformer.setOriginalSchema(Schema.parse(DEFAULT_VALUE_SCHEMA));
            return recordTransformer;
          }));

      // Test non-existent key access
      clientWithRecordTransformer.subscribeAll().get();
      assertNull(clientWithRecordTransformer.get(KEY_COUNT + 1).get());

      // Test single-get access
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(clientWithRecordTransformer.get(k).get(), 100);
      }
      clientWithRecordTransformer.unsubscribeAll();
    }

    if (clientConfig.isRecordTransformerEnabled()) {
      clientConfig.setRecordTransformerFunction(null);
    }

    // Test multiple clients sharing the same ClientConfig/MetricsRepository & base data path
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
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
    }

    // Test bootstrap-time junk removal
    cluster.useControllerClient(controllerClient -> {
      ControllerResponse response = controllerClient.disableAndDeleteStore(storeName3);
      assertFalse(response.isError(), response.getError());
    });

    // Test managed clients & data cleanup
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        backendConfig,
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testDavinciSubscribeFailureWithFullDisk() throws Exception {
    String storeName = Utils.getUniqueString("test-davinci-store");
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> {};
    setUpStore(storeName, paramsConsumer, properties -> {});

    Map<String, Object> backendConfigMap = new HashMap<>();
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
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
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

  @Test(groups = { "flaky" }, timeOut = TEST_TIMEOUT * 2)
  public void testUnstableIngestionIsolation() throws Exception {
    final String storeName = Utils.getUniqueString("store");
    // TODO: I have no idea how this happens, BeforeClass should have run setup, but it seems to not do that sometimes?
    if (cluster == null) {
      setUp();
    }
    cluster.useControllerClient(client -> {
      NewStoreResponse response =
          client.createNewStore(storeName, getClass().getName(), DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
    });
    VersionCreationResponse newVersion = cluster.getNewVersion(storeName);
    final int pushVersion = newVersion.getVersion();
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory =
        IntegrationTestPushUtils.getVeniceWriterFactory(cluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory);
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(DEFAULT_VALUE_SCHEMA);

    Map<String, Object> extraBackendConfigMap = TestUtils.getIngestionIsolationPropertyMap();
    extraBackendConfigMap.put(SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS, 5);

    DaVinciTestContext<Integer, Integer> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            new DaVinciConfig(),
            extraBackendConfigMap);

    try (VeniceWriter<Object, Object, byte[]> writer = vwFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
            .setValueSerializer(valueSerializer)
            .build());
        CachingDaVinciClientFactory factory = daVinciTestContext.getDaVinciClientFactory()) {
      int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
      writer.broadcastStartOfPush(Collections.emptyMap());
      Future[] writerFutures = new Future[KEY_COUNT];
      for (int i = 0; i < KEY_COUNT; i++) {
        writerFutures[i] = writer.put(i, pushVersion, valueSchemaId);
      }
      for (int i = 0; i < KEY_COUNT; i++) {
        writerFutures[i].get();
      }
      DaVinciClient<Integer, Integer> client = daVinciTestContext.getDaVinciClient();
      CompletableFuture<Void> future = client.subscribeAll();
      // Kill the ingestion process.
      int isolatedIngestionServicePort = factory.getBackendConfig().getInt(SERVER_INGESTION_ISOLATION_SERVICE_PORT);
      IsolatedIngestionUtils.releaseTargetPortBinding(isolatedIngestionServicePort);
      // Make sure ingestion will end and future can complete
      writer.broadcastEndOfPush(Collections.emptyMap());
      future.get();
      for (int i = 0; i < KEY_COUNT; i++) {
        int result = client.get(i).get();
        assertEquals(result, pushVersion);
      }

      // Kill the ingestion process again.
      IsolatedIngestionUtils.releaseTargetPortBinding(isolatedIngestionServicePort);
      IngestionStorageMetadata dummyOffsetMetadata = new IngestionStorageMetadata();
      dummyOffsetMetadata.metadataUpdateType = IngestionMetadataUpdateType.PUT_OFFSET_RECORD.getValue();
      dummyOffsetMetadata.topicName = Version.composeKafkaTopic(storeName, 1);
      dummyOffsetMetadata.partitionId = 0;
      dummyOffsetMetadata.payload =
          ByteBuffer.wrap(new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer()).toBytes());
      VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
      when(serverConfig.getIngestionServicePort()).thenReturn(12345);
      VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
      when(configLoader.getVeniceServerConfig()).thenReturn(serverConfig);
      VeniceProperties combinedProperties = mock(VeniceProperties.class);
      when(configLoader.getCombinedProperties()).thenReturn(combinedProperties);
      MainIngestionRequestClient requestClient = new MainIngestionRequestClient(configLoader);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertTrue(requestClient.updateMetadata(dummyOffsetMetadata));
      });
      client.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 5)
  public void testIngestionIsolation() throws Exception {
    final int partitionCount = 3;
    final int dataPartition = 1;
    int emptyPartition1 = 2;
    int emptyPartition2 = 0;
    String storeName = Utils.getUniqueString("store");
    String storeName2 = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> params.setPartitionCount(partitionCount)
        .setPartitionerClass(ConstantVenicePartitioner.class.getName())
        .setPartitionerParams(
            Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(dataPartition)));
    setupHybridStore(storeName, paramsConsumer, 1000);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    Map<String, Object> extraBackendConfigMap = TestUtils.getIngestionIsolationPropertyMap();
    extraBackendConfigMap.put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true);
    extraBackendConfigMap.put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1);
    extraBackendConfigMap.put(DATA_BASE_PATH, baseDataPath);
    extraBackendConfigMap.put(
        SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER,
        VeniceServerConfig.MINIMUM_CONSUMER_NUM_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER);

    DaVinciTestContext<Integer, Integer> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            metricsRepository,
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            new DaVinciConfig(),
            extraBackendConfigMap);

    try (CachingDaVinciClientFactory ignored = daVinciTestContext.getDaVinciClientFactory()) {
      DaVinciClient<Integer, Integer> client = daVinciTestContext.getDaVinciClient();
      // subscribe to a partition without data
      client.subscribe(Collections.singleton(emptyPartition1)).get();
      for (int i = 0; i < KEY_COUNT; i++) {
        final int key = i;
        assertThrows(VeniceException.class, () -> client.get(key).get());
      }
      client.unsubscribe(Collections.singleton(emptyPartition1));

      /**
       * Subscribe to the data partition.
       * We perform a subscribe->unsubscribe->subscribe here because we want to test that previous subscription state is
       * cleaned up.
       */
      client.subscribe(Collections.singleton(dataPartition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
        }
      });
      client.unsubscribe(Collections.singleton(dataPartition));
      assertThrows(() -> client.get(0).get());

      // Subscribe to data partition.
      client.subscribe(Collections.singleton(dataPartition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
        }
      });
      // We subscribe and unsubscribe to different partitions to make sure forked process can work successfully.
      client.subscribe(Collections.singleton(emptyPartition1)).get();
      client.unsubscribe(Collections.singleton(emptyPartition1));
      client.subscribe(Collections.singleton(emptyPartition2)).get();
      client.unsubscribe(Collections.singleton(emptyPartition2));
    }

    // Restart Da Vinci client to test bootstrap logic.
    metricsRepository = new MetricsRepository();
    daVinciTestContext = ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
        d2Client,
        metricsRepository,
        Optional.empty(),
        cluster.getZk().getAddress(),
        storeName,
        new DaVinciConfig(),
        extraBackendConfigMap);
    try (CachingDaVinciClientFactory factory = daVinciTestContext.getDaVinciClientFactory()) {
      DaVinciClient<Integer, Integer> client = daVinciTestContext.getDaVinciClient();
      // Subscribe to data partition.
      client.subscribe(Collections.singleton(dataPartition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, true, true, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
        }
      });

      // Make sure multiple clients can share same isolated ingestion service.
      DaVinciClient<Integer, Integer> client2 = factory.getAndStartGenericAvroClient(storeName2, new DaVinciConfig());
      client2.subscribeAll().get();
      for (int k = 0; k < KEY_COUNT; ++k) {
        int result = client2.get(k).get();
        assertEquals(result, 1);
      }
      MetricsRepository finalMetricsRepository = metricsRepository;
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertTrue(
              finalMetricsRepository.metrics().keySet().stream().anyMatch(k -> k.contains("ingestion_isolation"))));
      LOGGER.info(
          "Successfully finished all assertions! All that's left is closing the {}",
          factory.getClass().getSimpleName());
    }
  }

  @Test(dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT
      * 2)
  public void testHybridStoreWithoutIngestionIsolation(DaVinciConfig daVinciConfig) throws Exception {
    // Create store
    final int partitionCount = 2;
    final int emptyPartition = 0;
    final int dataPartition = 1;
    String storeName = Utils.getUniqueString("store");

    // Convert it to hybrid
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionCount(partitionCount)
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(dataPartition)));
    setupHybridStore(storeName, paramsConsumer);

    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
      // subscribe to a partition without data
      client.subscribe(Collections.singleton(emptyPartition)).get();
      for (int i = 0; i < KEY_COUNT; i++) {
        int key = i;
        assertThrows(NonLocalAccessException.class, () -> client.get(key).get());
      }
      client.unsubscribe(Collections.singleton(emptyPartition));

      // subscribe to a partition with data
      client.subscribe(Collections.singleton(dataPartition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {

        Map<Integer, Integer> keyValueMap = new HashMap<>();
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
          keyValueMap.put(i, i);
        }

        Map<Integer, Integer> batchGetResult = client.batchGet(keyValueMap.keySet()).get();
        assertNotNull(batchGetResult);
        assertEquals(batchGetResult, keyValueMap);
      });

      // Write some fresh records to override the old value. Make sure we can read the new value.
      List<Pair<Object, Object>> dataToPublish = new ArrayList<>();
      dataToPublish.add(new Pair<>(0, 1));
      dataToPublish.add(new Pair<>(1, 2));
      dataToPublish.add(new Pair<>(3, 4));

      generateHybridData(storeName, dataToPublish);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Pair<Object, Object> entry: dataToPublish) {
          assertEquals(client.get((Integer) entry.getFirst()).get(), entry.getSecond());
        }
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testHybridStore() throws Exception {
    final int partition = 1;
    final int partitionCount = 2;
    String storeName = Utils.getUniqueString("store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionCount(partitionCount)
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition)));
    setupHybridStore(storeName, paramsConsumer);

    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
      // subscribe to a partition without data
      int emptyPartition = (partition + 1) % partitionCount;
      client.subscribe(Collections.singleton(emptyPartition)).get();
      for (int i = 0; i < KEY_COUNT; i++) {
        int key = i;
        assertThrows(NonLocalAccessException.class, () -> client.get(key).get());
      }
      client.unsubscribe(Collections.singleton(emptyPartition));

      // subscribe to a partition with data
      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        Map<Integer, Integer> keyValueMap = new HashMap<>();
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
          keyValueMap.put(i, i);
        }

        Map<Integer, Integer> batchGetResult = client.batchGet(keyValueMap.keySet()).get();
        assertNotNull(batchGetResult);
        assertEquals(batchGetResult, keyValueMap);
      });

      DaVinciConfig daVinciConfig = new DaVinciConfig().setIsolated(true);
      try (DaVinciClient<Integer, Integer> client2 = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster)) {
        DaVinciClient<Integer, Integer> client3 = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
        DaVinciClient<Integer, Integer> client4 = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);

        // Verify that closed cached client can be restarted.
        client.close();
        DaVinciClient<Integer, Integer> client1 = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
        assertEquals((int) client1.get(1).get(), 1);

        // Isolated clients are not supposed to be cached by the factory.
        assertNotSame(client, client2);
        assertNotSame(client, client3);
        assertNotSame(client, client4);

        // Isolated clients should not be able to unsubscribe partitions of other clients.
        client3.unsubscribeAll();

        client3.subscribe(Collections.singleton(partition)).get(0, TimeUnit.SECONDS);
        for (int i = 0; i < KEY_COUNT; i++) {
          final int key = i;
          // Both client2 & client4 are not subscribed to any partition. But client2 is not-isolated so it can
          // access partitions of other clients, when client4 cannot.
          assertEquals((int) client2.get(i).get(), i);
          assertEquals((int) client3.get(i).get(), i);
          assertThrows(NonLocalAccessException.class, () -> client4.get(key).get());
        }
      }
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
    DaVinciTestContext<Integer, Object> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            metricsRepository,
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            daVinciConfig,
            Collections.singletonMap(DATA_BASE_PATH, baseDataPath));
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
        new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
            .setValueSerializer(valueSerializer)
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
  public void testPartialSubscription(DaVinciConfig daVinciConfig) throws Exception {
    String storeName = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    VeniceProperties backendConfig = new PropertyBuilder().build();

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
            cluster.getZk().getAddress(),
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
            cluster.getZk().getAddress(),
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

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testCrashedDaVinciWithIngestionIsolation() throws Exception {
    String storeName = createStoreWithMetaSystemStoreAndPushStatusSystemStore(KEY_COUNT);
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    String zkHosts = cluster.getZk().getAddress();
    int port1 = TestUtils.getFreePort();
    int port2 = TestUtils.getFreePort();
    while (port1 == port2) {
      port2 = TestUtils.getFreePort();
    }
    ForkedJavaProcess forkedDaVinciUserApp = ForkedJavaProcess.exec(
        DaVinciUserApp.class,
        zkHosts,
        baseDataPath,
        storeName,
        "100",
        "10",
        "true",
        Integer.toString(port1),
        Integer.toString(port2));
    // Sleep long enough so the forked Da Vinci app process can finish ingestion.
    Thread.sleep(60000);
    IsolatedIngestionUtils.executeShellCommand("kill " + forkedDaVinciUserApp.pid());
    // Sleep long enough so the heartbeat timeout is detected by IsolatedIngestionServer.
    Thread.sleep(15000);
    D2Client d2Client = new D2ClientBuilder().setZkHosts(zkHosts)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(D2_ZK_HOSTS_ADDRESS, zkHosts)
        .build();

    // Re-open the same store's database to verify RocksDB metadata partition's lock has been released.
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
      client.subscribeAll().get();
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

  /**
   * TODO: Add asserts to accurately validate if the blob transfer was performed correctly.
   * For the local P2P testing, need to setup two different directories and ports for the two Da Vinci clients in order
   * to avoid conflicts.
   */
  @Test(timeOut = 2 * TEST_TIMEOUT)
  public void testBlobP2PTransferAmongDVC() throws Exception {
    String dvcPath1 = Utils.getTempDataDirectory().getAbsolutePath();
    String zkHosts = cluster.getZk().getAddress();
    int port1 = TestUtils.getFreePort();
    int port2 = TestUtils.getFreePort();
    while (port1 == port2) {
      port2 = TestUtils.getFreePort();
    }
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> params.setBlobTransferEnabled(true);
    String storeName = Utils.getUniqueString("test-store");
    setUpStore(storeName, paramsConsumer, properties -> {}, true);
    LOGGER.info("Port1 is {}, Port2 is {}", port1, port2);
    LOGGER.info("zkHosts is {}", zkHosts);

    // Start the first DaVinci Client using DaVinciUserApp for regular ingestion
    ForkedJavaProcess.exec(
        DaVinciUserApp.class,
        zkHosts,
        dvcPath1,
        storeName,
        "100",
        "10",
        "false",
        Integer.toString(port1),
        Integer.toString(port2));

    // Wait for the first DaVinci Client to complete ingestion
    Thread.sleep(60000);

    // Start the second DaVinci Client using settings for blob transfer
    String dvcPath2 = Utils.getTempDataDirectory().getAbsolutePath();

    PropertyBuilder configBuilder = new PropertyBuilder().put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false")
        .put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true")
        .put(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "3000")
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, dvcPath2)
        .put(DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, port2)
        .put(DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, port1)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 1)
        .put(BLOB_TRANSFER_MANAGER_ENABLED, true);
    VeniceProperties backendConfig2 = configBuilder.build();
    DaVinciConfig dvcConfig = new DaVinciConfig().setIsolated(true);

    try (CachingDaVinciClientFactory factory2 = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        backendConfig2)) {
      DaVinciClient<Integer, Object> client2 = factory2.getAndStartGenericAvroClient(storeName, dvcConfig);
      client2.subscribeAll().get();

      for (int i = 0; i < 3; i++) {
        String snapshotPath = RocksDBUtils.composeSnapshotDir(dvcPath1 + "/rocksdb", storeName + "_v1", i);
        Assert.assertTrue(Files.exists(Paths.get(snapshotPath)));
      }
    }
  }

  private void setupHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer) throws Exception {
    setupHybridStore(storeName, paramsConsumer, KEY_COUNT);
  }

  private void setupHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer, int keyCount)
      throws Exception {
    UpdateStoreQueryParams params =
        new UpdateStoreQueryParams().setHybridRewindSeconds(10).setHybridOffsetLagThreshold(10);
    paramsConsumer.accept(params);
    cluster.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      cluster.createMetaSystemStore(storeName);
      client.updateStore(storeName, params);
      cluster.createVersion(storeName, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, Stream.of());
      SystemProducer producer = IntegrationTestPushUtils.getSamzaProducer(
          cluster,
          storeName,
          Version.PushType.STREAM,
          Pair.create(VENICE_PARTITIONERS, ConstantVenicePartitioner.class.getName()));
      try {
        for (int i = 0; i < keyCount; i++) {
          IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, i, i);
        }
      } finally {
        producer.stop();
      }
    });
  }

  private void generateHybridData(String storeName, List<Pair<Object, Object>> dataToWrite) {
    SystemProducer producer = IntegrationTestPushUtils.getSamzaProducer(
        cluster,
        storeName,
        Version.PushType.STREAM,
        Pair.create(VENICE_PARTITIONERS, ConstantVenicePartitioner.class.getName()));
    try {
      for (Pair<Object, Object> record: dataToWrite) {
        IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, record.getFirst(), record.getSecond());
      }
    } finally {
      producer.stop();
    }
  }

  private void setUpStore(
      String storeName,
      Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<Properties> propertiesConsumer) throws Exception {
    setUpStore(storeName, paramsConsumer, propertiesConsumer, false);
  }

  private void setUpStore(
      String storeName,
      Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<Properties> propertiesConsumer,
      boolean useDVCPushStatusStore) throws Exception {
    // Produce input data.
    writeSimpleAvroFileWithIntToStringSchema(inputDir);

    // Setup VPJ job properties.
    Properties vpjProperties = defaultVPJProps(cluster, inputDirPath, storeName);
    propertiesConsumer.accept(vpjProperties);
    // Create & update store for test.
    final int numPartitions = 3;
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setPartitionCount(numPartitions); // Update the
    // partition count.
    paramsConsumer.accept(params);

    try (ControllerClient controllerClient =
        createStoreForJob(cluster, DEFAULT_KEY_SCHEMA, "\"string\"", vpjProperties)) {
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
    String jobName = Utils.getUniqueString("batch-job-" + expectedVersionNumber);
    TestWriteUtils.runPushJob(jobName, vpjProperties);
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

  private String createStoreWithMetaSystemStoreAndPushStatusSystemStore(
      int keyCount,
      CompressionStrategy compressionStrategy,
      Function<String, ByteBuffer> compressionDictionaryGenerator) throws Exception {
    String storeName = cluster.createStore(keyCount, compressionStrategy, compressionDictionaryGenerator);
    cluster.createMetaSystemStore(storeName);
    cluster.createPushStatusSystemStore(storeName);
    return storeName;
  }

  @DataProvider(name = "CompressionStrategy")
  public static Object[][] compressionStrategy() {
    return DataProviderUtils.allPermutationGenerator(DataProviderUtils.COMPRESSION_STRATEGIES);
  }
}
