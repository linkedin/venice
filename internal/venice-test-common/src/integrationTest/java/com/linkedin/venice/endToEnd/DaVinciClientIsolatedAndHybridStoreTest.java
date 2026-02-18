package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.D2_ZK_HOSTS_ADDRESS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_D2_CLIENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SERVICE_PORT;
import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.DaVinciUserApp;
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
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.IngestionMetadataUpdateType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DaVinciClientIsolatedAndHybridStoreTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientIsolatedAndHybridStoreTest.class);
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
            cluster,
            storeName,
            new DaVinciConfig(),
            extraBackendConfigMap);

    try (VeniceWriter<Object, Object, byte[]> writer = vwFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(topic).setKeyPayloadSerializer(keySerializer)
            .setValuePayloadSerializer(valueSerializer)
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
      dummyOffsetMetadata.payload = ByteBuffer.wrap(
          new OffsetRecord(
              AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
              DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING).toBytes());
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

    // Start the first DaVinci Client using DaVinciUserApp for regular ingestion
    File configDir = Utils.getTempDataDirectory();
    File configFile = new File(configDir, "dvc-config.properties");

    Properties props = new Properties();
    props.setProperty("zk.hosts", zkHosts);
    props.setProperty("base.data.path", baseDataPath);
    props.setProperty("store.name", storeName);
    props.setProperty("sleep.seconds", "100");
    props.setProperty("heartbeat.timeout.seconds", "10");
    props.setProperty("ingestion.isolation", "true");
    props.setProperty("blob.transfer.server.port", Integer.toString(port1));
    props.setProperty("blob.transfer.client.port", Integer.toString(port2));
    props.setProperty("storage.class", StorageClass.DISK.toString());
    props.setProperty("record.transformer.enabled", "false");
    props.setProperty("blob.transfer.manager.enabled", "false");
    props.setProperty("batch.push.report.enabled", "false");

    // Write properties to file
    try (FileWriter writer = new FileWriter(configFile)) {
      props.store(writer, null);
    }

    ForkedJavaProcess forkedDaVinciUserApp = ForkedJavaProcess.exec(DaVinciUserApp.class, configFile.getAbsolutePath());

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
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(D2_ZK_HOSTS_ADDRESS, zkHosts)
        .build();

    // Re-open the same store's database to verify RocksDB metadata partition's lock has been released.
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
      client.subscribeAll().get();
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
            cluster,
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
        cluster,
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

  @Test(timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testHybridStore(Boolean isD2ClientEnabled) throws Exception {
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
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(SERVER_INGESTION_ISOLATION_D2_CLIENT_ENABLED, isD2ClientEnabled)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
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
        // Verify that 2nd close call on the same store won't throw exception.
        client.close();
        DaVinciClient<Integer, Integer> client1 = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
        assertEquals((int) client1.get(1).get(), 1);

        // Isolated clients are not supposed to be cached by the factory.
        assertNotSame(client, client2);
        assertNotSame(client, client3);
        assertNotSame(client, client4);

        // Isolated clients should not be able to unsubscribe partitions of other clients.
        client3.unsubscribeAll();

        client3.subscribe(Collections.singleton(partition)).get(10, TimeUnit.SECONDS);
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

  @Test(timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class, dataProvider = "Two-True-and-False")
  public void testIncrementalPushStatusBatching(boolean isIngestionIsolated, Boolean isD2ClientEnabled)
      throws Exception {
    final int partition = 0;
    final int partitionCount = 1;
    String storeName = Utils.getUniqueString("store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionCount(partitionCount)
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition)));
    // Create an empty hybrid store first
    setupHybridStore(storeName, paramsConsumer, 0);

    String incrementalPushVersion = System.currentTimeMillis() + "_test_1";
    runIncrementalPush(storeName, incrementalPushVersion, 100);

    // Build the da-vinci client
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000)
        .put(SERVER_INGESTION_ISOLATION_D2_CLIENT_ENABLED, isD2ClientEnabled)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      DaVinciConfig daVinciConfig = new DaVinciConfig().setIsolated(isIngestionIsolated);
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);

      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        Map<Integer, Integer> keyValueMap = new HashMap<>();
        for (Integer i = 0; i < 100; i++) {
          assertEquals(client.get(i).get(), i);
          keyValueMap.put(i, i);
        }

        Map<Integer, Integer> batchGetResult = client.batchGet(keyValueMap.keySet()).get();
        assertNotNull(batchGetResult);
        assertEquals(batchGetResult, keyValueMap);
      });

      // Verify the incremental push status is END_OF_INCREMENTAL_PUSH_RECEIVED
      String versionTopic = Version.composeKafkaTopic(storeName, 1);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        cluster.useControllerClient(controllerClient -> {
          JobStatusQueryResponse statusQueryResponse =
              controllerClient.queryJobStatus(versionTopic, Optional.of(incrementalPushVersion));
          if (statusQueryResponse.isError()) {
            throw new VeniceException(statusQueryResponse.getError());
          }
          assertEquals(
              ExecutionStatus.valueOf(statusQueryResponse.getStatus()),
              ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
        });
      });
    }
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

  private void setupHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer) throws Exception {
    setupHybridStore(storeName, paramsConsumer, KEY_COUNT);
  }

  private void setupHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer, int keyCount)
      throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setHybridRewindSeconds(10)
        .setHybridOffsetLagThreshold(10)
        .setIncrementalPushEnabled(true);
    paramsConsumer.accept(params);
    cluster.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      cluster.createMetaSystemStore(storeName);
      client.updateStore(storeName, params);
      cluster.createVersion(storeName, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, Stream.of());
      if (keyCount > 0) {
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
      }
    });
  }

  private void runIncrementalPush(String storeName, String incrementalPushVersion, int keyCount) throws Exception {
    String realTimeTopicName =
        Boolean.parseBoolean(VeniceClusterWrapper.CONTROLLER_ENABLE_REAL_TIME_TOPIC_VERSIONING_IN_TESTS)
            ? Utils.composeRealTimeTopic(storeName, 1)
            : Utils.composeRealTimeTopic(storeName);
    VeniceWriterFactory vwFactory =
        IntegrationTestPushUtils.getVeniceWriterFactory(cluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory);
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(DEFAULT_VALUE_SCHEMA);
    int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    try (VeniceWriter<Object, Object, byte[]> batchProducer = vwFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(realTimeTopicName).setKeyPayloadSerializer(keySerializer)
            .setValuePayloadSerializer(valueSerializer)
            .build())) {
      batchProducer.broadcastStartOfIncrementalPush(incrementalPushVersion, new HashMap<>());

      Future[] writerFutures = new Future[keyCount];
      for (int i = 0; i < keyCount; i++) {
        writerFutures[i] = batchProducer.put(i, i, valueSchemaId);
      }
      for (int i = 0; i < keyCount; i++) {
        writerFutures[i].get();
      }
      batchProducer.broadcastEndOfIncrementalPush(incrementalPushVersion, Collections.emptyMap());
    }
  }
}
