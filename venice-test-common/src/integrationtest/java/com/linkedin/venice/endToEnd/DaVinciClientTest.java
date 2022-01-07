package com.linkedin.venice.endToEnd;

import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.ComputeRequestBuilder;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compute.ComputeOperationUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.IngestionMetadataUpdateType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.DaVinciUserApp;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.NonLocalAccessException;
import com.linkedin.davinci.client.NonLocalAccessPolicy;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;

import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.client.store.predicate.PredicateBuilder.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static com.linkedin.venice.meta.IngestionMode.*;
import static com.linkedin.venice.meta.PersistenceType.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class DaVinciClientTest {
  @DataProvider(name = "LF-And-CompressionStrategy")
  public static Object[][] lfAndCompressionStrategy() {
    return DataProviderUtils.allPermutationGenerator(DataProviderUtils.BOOLEAN, DataProviderUtils.COMPRESSION_STRATEGIES);
  }

  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 60_000; // ms

  private static final String TEST_RECORD_VALUE_SCHEMA = "{\"type\":\"record\", \"name\":\"ValueRecord\", \"fields\": [{\"name\":\"number\", "
          + "\"type\":\"int\"}]}";


  private VeniceClusterWrapper cluster;
  private D2Client d2Client;

  private final List<Float> mfEmbedding = generateRandomFloatList(100);
  private final List<Float> companiesEmbedding = generateRandomFloatList(100);
  private final List<Float> pymkCosineSimilarityEmbedding = generateRandomFloatList(100);

  private static final String KEY_PREFIX = "key_";
  private static final String VALUE_PREFIX = "id_";
  private static final int MAX_KEY_LIMIT = 1000;
  private static final String NON_EXISTING_KEY1 = "a_unknown_key";
  private static final String NON_EXISTING_KEY2 = "z_unknown_key";
  private static final int NON_EXISTING_KEY_NUM = 2;

  private static final String VALUE_SCHEMA_FOR_COMPUTE = "{" +
      "  \"namespace\": \"example.compute\",    " +
      "  \"type\": \"record\",        " +
      "  \"name\": \"MemberFeature\",       " +
      "  \"fields\": [        " +
      "         { \"name\": \"id\", \"type\": \"string\" },             " +
      "         { \"name\": \"name\", \"type\": \"string\" },           " +
      "         {   \"default\": [], \n  \"name\": \"companiesEmbedding\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  }, " +
      "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } }        " +
      "  ]       " +
      " }       ";

  private static final String VALUE_SCHEMA_FOR_COMPUTE_MISSING_FIELD = "{" +
      "  \"namespace\": \"example.compute\",    " +
      "  \"type\": \"record\",        " +
      "  \"name\": \"MemberFeature\",       " +
      "  \"fields\": [        " +
      "         { \"name\": \"id\", \"type\": \"string\" },             " +
      "         { \"name\": \"name\", \"type\": \"string\" },           " +
      "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } }        " +
      "  ]       " +
      " }       ";

  private static final String VALUE_SCHEMA_FOR_COMPUTE_NULLABLE_LIST_FIELD = "{" +
      "  \"namespace\": \"example.compute\",    " +
      "  \"type\": \"record\",        " +
      "  \"name\": \"MemberFeature\",       " +
      "  \"fields\": [        " +
      "   {\"name\": \"id\", \"type\": \"string\" },             " +
      "   {\"name\": \"name\", \"type\": \"string\" },           " +
      "   {\"name\": \"member_feature\", \"type\": [\"null\",{\"type\":\"array\",\"items\":\"float\"}],\"default\": null}" + // nullable field
      "  ] " +
      " }  ";

  private static final String VALUE_SCHEMA_FOR_COMPUTE_SWAPPED = "{" +
      "  \"namespace\": \"example.compute\",    " +
      "  \"type\": \"record\",        " +
      "  \"name\": \"MemberFeature\",       " +
      "  \"fields\": [        " +
      "         { \"name\": \"id\", \"type\": \"string\" },             " +
      "         { \"name\": \"name\", \"type\": \"string\" },           " +
      "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } },        " +
      "         {   \"default\": [], \n  \"name\": \"companiesEmbedding\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  } " +
      "  ]       " +
      " }       ";

  private static final String KEY_SCHEMA_STEAMING_COMPUTE = "\"string\"";

  private static final String VALUE_SCHEMA_STREAMING_COMPUTE = "{\n" +
      "\"type\": \"record\",\n" +
      "\"name\": \"test_value_schema\",\n" +
      "\"fields\": [\n" +
      "  {\"name\": \"int_field\", \"type\": \"int\"},\n" +
      "  {\"name\": \"float_field\", \"type\": \"float\"}\n" +
      "]\n" +
      "}";

  private static final String KEY_SCHEMA_PARTIAL_KEY_LOOKUP = "{" +
      "\"type\":\"record\"," +
      "\"name\":\"KeyRecord\"," +
      "\"namespace\":\"example.partialKeyLookup\"," +
      "\"fields\":[ " +
      "   {\"name\":\"id\",\"type\":\"string\"}," +
      "   {\"name\":\"companyId\",\"type\":\"int\"}, " +
      "   {\"name\":\"name\",\"type\":\"string\"} " +
      " ]" +
      "}";

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    cluster = ServiceFactory.getVeniceCluster(1, 2, 1, 1,
        100, false, false, clusterConfig);
    d2Client = new D2ClientBuilder()
        .setZkHosts(cluster.getZk().getAddress())
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

  @AfterMethod
  public void verifyPostConditions(Method method) {
    try {
      assertThrows(NullPointerException.class, AvroGenericDaVinciClient::getBackend);
    } catch (AssertionError e) {
      throw new AssertionError(method.getName() + " leaked DaVinciBackend.", e);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testConcurrentGetAndStart() throws Exception {
    String storeName1 = cluster.createStore(KEY_COUNT);
    String storeName2 = cluster.createStore(KEY_COUNT);

    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder()
                                         .put(DATA_BASE_PATH, baseDataPath)
                                         .put(PERSISTENCE_TYPE, ROCKS_DB)
                                         .build();

    for (int i = 0; i < 10; ++i) {
      MetricsRepository metricsRepository = new MetricsRepository();
      try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
        CompletableFuture.allOf(
            CompletableFuture.runAsync(() -> factory.getGenericAvroClient(storeName1, new DaVinciConfig()).start()),
            CompletableFuture.runAsync(() -> factory.getGenericAvroClient(storeName2, new DaVinciConfig()).start()),
            CompletableFuture.runAsync(() -> factory.getGenericAvroClient(storeName1, new DaVinciConfig().setIsolated(true)).start()),
            CompletableFuture.runAsync(() -> factory.getGenericAvroClient(storeName2, new DaVinciConfig().setIsolated(true)).start())
        ).get();
      }
      assertThrows(NullPointerException.class, AvroGenericDaVinciClient::getBackend);
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testBatchStore(DaVinciConfig clientConfig) throws Exception {
    String storeName1 = cluster.createStore(KEY_COUNT);
    String storeName2 = cluster.createStore(KEY_COUNT);
    String storeName3 = cluster.createStore(KEY_COUNT);

    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder()
            .put(DATA_BASE_PATH, baseDataPath)
            .put(PERSISTENCE_TYPE, ROCKS_DB)
            .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    // Test multiple clients sharing the same ClientConfig/MetricsRepository & base data path
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {

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

      // Test read from a store that is being deleted concurrently
      try (ControllerClient controllerClient = cluster.getControllerClient()) {
        ControllerResponse response = controllerClient.disableAndDeleteStore(storeName2);
        assertFalse(response.isError(), response.getError());

        TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          assertThrows(VeniceClientException.class, () -> client2.get(KEY_COUNT / 3).get());
        });
      }
    }

    // Test bootstrap-time junk removal
    try (ControllerClient controllerClient = cluster.getControllerClient()) {
      ControllerResponse response = controllerClient.disableAndDeleteStore(storeName3);
      assertFalse(response.isError(), response.getError());
    }

    // Test managed clients & data cleanup
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
        d2Client, new MetricsRepository(), backendConfig, Optional.of(Collections.singleton(storeName1)))) {
      assertNotEquals(FileUtils.sizeOfDirectory(new File(baseDataPath)), 0);

      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(storeName1, clientConfig);
      client1.subscribeAll().get();
      client1.unsubscribeAll();
      // client2 was removed explicitly above via disableAndDeleteStore()
      // client3 is expected to be removed by the factory during bootstrap
      assertEquals(FileUtils.sizeOfDirectory(new File(baseDataPath)), 0);
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testObjectReuse(DaVinciConfig clientConfig) throws Exception {
    final Schema schema = Schema.parse(TEST_RECORD_VALUE_SCHEMA);
    final GenericRecord value = new GenericData.Record(schema);
    value.put("number", 10);
    String storeName = cluster.createStore(KEY_COUNT, value);

    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder()
            .put(DATA_BASE_PATH, baseDataPath)
            .put(PERSISTENCE_TYPE, ROCKS_DB)
            .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {

      DaVinciClient<Integer, Object> client = factory.getAndStartGenericAvroClient(storeName, clientConfig);

      GenericRecord reusableObject = new GenericData.Record(client.getLatestValueSchema());
      reusableObject.put("number", -1);

      // Test non-existent key access with a reusable object
      client.subscribeAll().get();
      assertNull(client.get(KEY_COUNT + 1, reusableObject).get());

      // A Non existing value should not get stored in the passed in object
      assertEquals(reusableObject.get(0), -1);

      // Test single-get access
      Map<Integer, Integer> keyValueMap = new HashMap<>();
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

  @Test(groups = {"flaky"}, timeOut = TEST_TIMEOUT * 2)
  public void testUnstableIngestionIsolation() throws Exception {
    final String storeName = Utils.getUniqueString( "store");
    cluster.useControllerClient(client -> {
      NewStoreResponse response = client.createNewStore(storeName, getClass().getName(), DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
    });
    VersionCreationResponse newVersion = cluster.getNewVersion(storeName, 1024);
    final int pushVersion = newVersion.getVersion();
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(DEFAULT_VALUE_SCHEMA);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(SERVER_INGESTION_MODE, ISOLATED)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .put(SERVER_INGESTION_ISOLATION_HEARTBEAT_TIMEOUT_MS, 5 * Time.MS_PER_SECOND)
        .build();

    try (
        VeniceWriter<Object, Object, byte[]> writer = vwFactory.createVeniceWriter(topic, keySerializer, valueSerializer, false);
        CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
      writer.broadcastStartOfPush(Collections.emptyMap());
      Future[] writerFutures = new Future[KEY_COUNT];
      for (int i = 0; i < KEY_COUNT; i++) {
        writerFutures[i] = writer.put(i, pushVersion, valueSchemaId);
      }
      for (int i = 0; i < KEY_COUNT; i++) {
        writerFutures[i].get();
      }
      DaVinciConfig daVinciConfig = new DaVinciConfig();
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
      CompletableFuture<Void> future = client.subscribeAll();
      // Kill the ingestion process.
      IsolatedIngestionUtils.releaseTargetPortBinding(servicePort);
      // Make sure ingestion will end and future can complete
      writer.broadcastEndOfPush(Collections.emptyMap());
      future.get();
      for (int i = 0; i < KEY_COUNT; i++) {
        int result = client.get(i).get();
        assertEquals(result, pushVersion);
      }

      // Kill the ingestion process again.
      IsolatedIngestionUtils.releaseTargetPortBinding(servicePort);
      IngestionStorageMetadata dummyOffsetMetadata = new IngestionStorageMetadata();
      dummyOffsetMetadata.metadataUpdateType = IngestionMetadataUpdateType.PUT_OFFSET_RECORD.getValue();
      dummyOffsetMetadata.topicName = Version.composeKafkaTopic(storeName, 1);
      dummyOffsetMetadata.partitionId = 0;
      dummyOffsetMetadata.payload = ByteBuffer.wrap(new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer()).toBytes());
      MainIngestionRequestClient requestClient = new MainIngestionRequestClient(Optional.empty(), servicePort);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertTrue(requestClient.updateMetadata(dummyOffsetMetadata));
      });
      client.unsubscribeAll();
    }
  }

  @Test(dataProvider = "L/F-and-AmplificationFactor", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT * 2)
  public void testIngestionIsolation(boolean isLeaderFollowerModelEnabled, boolean isAmplificationFactorEnabled) throws Exception {
    final int partition = 1;
    final int partitionCount = 2;
    final int amplificationFactor = isAmplificationFactorEnabled ? 3 : 1;
    String storeName = Utils.getUniqueString("store");
    String storeName2 = cluster.createStore(KEY_COUNT);
    Consumer<UpdateStoreQueryParams> paramsConsumer =
            params -> params.setAmplificationFactor(amplificationFactor)
                .setLeaderFollowerModel(isLeaderFollowerModelEnabled)
                .setPartitionCount(partitionCount)
                .setPartitionerClass(ConstantVenicePartitioner.class.getName())
                .setPartitionerParams(
                    Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition))
                );
    setupHybridStore(storeName, paramsConsumer, 1000);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();
    VeniceProperties backendConfig = new PropertyBuilder()
            .put(DATA_BASE_PATH, baseDataPath)
            .put(PERSISTENCE_TYPE, ROCKS_DB)
            .put(SERVER_INGESTION_MODE, ISOLATED)
            .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
            .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
            .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
            .build();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
      // subscribe to a partition without data
      int emptyPartition = (partition + 1) % partitionCount;
      client.subscribe(Collections.singleton(emptyPartition)).get();
      for (int i = 0; i < KEY_COUNT; i++) {
        final int key = i;
        assertThrows(VeniceException.class, () -> client.get(key).get());
      }
      client.unsubscribe(Collections.singleton(emptyPartition));

      /**
       * Subscribe to the data partition.
       * We perform a subscribe->unsubscribe->subscribe here because we want to test that previous subscription state is
       * cleaned up.
       */
      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
        }
      });
      client.unsubscribe(Collections.singleton(partition));
      assertThrows(() -> client.get(0).get());

      client.subscribe(Collections.singleton(partition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
        }
      });
    }
    // Restart Da Vinci client to test bootstrap logic.
    metricsRepository = new MetricsRepository();
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, false, true, () -> {
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
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS,
          () -> assertTrue(finalMetricsRepository.metrics().keySet().stream().anyMatch(k -> k.contains("ingestion_isolation")))
      );
    }
  }

  @Test(dataProvider = "L/F-and-AmplificationFactor-and-ObjectCache", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testHybridStoreWithoutIngestionIsolation(boolean isLeaderFollowerModelEnabled, boolean isAmplificationFactorEnabled, DaVinciConfig daVinciConfig) throws Exception {
    // Create store
    final int partition = 1;
    final int partitionCount = 2;
    final int amplificationFactor = isAmplificationFactorEnabled ? 3 : 1;
    String storeName = cluster.createStore(KEY_COUNT);

    // Convert it to hybrid
    Consumer<UpdateStoreQueryParams> paramsConsumer =
            params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
                    .setLeaderFollowerModel(isLeaderFollowerModelEnabled)
                    .setPartitionCount(partitionCount)
                    .setAmplificationFactor(amplificationFactor)
                    .setPartitionerParams(
                            Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition))
                    );
    setupHybridStore(storeName, paramsConsumer);

    VeniceProperties backendConfig = new PropertyBuilder()
            .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, ROCKS_DB)
            .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
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

      // Write some fresh records to override the old value.  Make sure we can read the new value.
      List<Pair<Object,Object>> dataToPublish = new ArrayList<>();
      dataToPublish.add(new Pair<>(0, 1));
      dataToPublish.add(new Pair<>(1,2));
      dataToPublish.add(new Pair<>(3, 4));

      generateHybridData(storeName, dataToPublish);

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Pair<Object, Object> entry : dataToPublish) {
          assertEquals(client.get((Integer) entry.getFirst()).get(), entry.getSecond());
        }
      });
    }
  }

  @Test(dataProvider = "L/F-and-AmplificationFactor", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testHybridStore(boolean isLeaderFollowerModelEnabled, boolean isAmplificationFactorEnabled) throws Exception {
    final int partition = 1;
    final int partitionCount = 2;
    final int amplificationFactor = isAmplificationFactorEnabled ? 3 : 1;
    String storeName = Utils.getUniqueString("store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setLeaderFollowerModel(isLeaderFollowerModelEnabled)
            .setPartitionCount(partitionCount)
            .setAmplificationFactor(amplificationFactor)
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition))
            );
    setupHybridStore(storeName, paramsConsumer);

    VeniceProperties backendConfig = new PropertyBuilder()
            .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, ROCKS_DB)
            .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
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
        for (Integer i = 0; i < KEY_COUNT; i++) {
          final int key = i;
          // Both client2 & client4 are not subscribed to any partition. But client2 is not-isolated so it can
          // access partitions of other clients, when client4 cannot.
          assertEquals(client2.get(i).get(), i);
          assertEquals(client3.get(i).get(), i);
          assertThrows(NonLocalAccessException.class, () -> client4.get(key).get());
        }
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testBootstrap(DaVinciConfig daVinciConfig) throws Exception {
    String storeName = cluster.createStore(KEY_COUNT);
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    try (DaVinciClient<Integer, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath)) {
      client.subscribeAll().get();
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client.get(k).get(), 1);
      }
    }

    try (DaVinciClient<Integer, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath)) {
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
    }

    daVinciConfig.setStorageClass(StorageClass.DISK);
    // Try to open the Da Vinci client with different storage class.
    try (DaVinciClient<Integer, Integer> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath, daVinciConfig)) {
      client.subscribeAll().get();
    }

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName, 1024);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(DEFAULT_VALUE_SCHEMA);

    VeniceWriter<Object, Object, byte[]> batchProducer = vwFactory.createVeniceWriter(topic, keySerializer, valueSerializer, false);
    int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
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
    try (DaVinciClient<Integer, Integer> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath, daVinciConfig)) {
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, false, true, () -> {
        for (int i = 0; i < KEY_COUNT; i++) {
          int value = client.get(i).get();
          assertEquals(value, 1);
        }
      });
    }
    batchProducer.broadcastEndOfPush(Collections.emptyMap());

    /**
     * Push a new version as the CURRENT VERSION, so that old local version is removed during bootstrap and the access will fail.
     */
    cluster.createVersion(storeName, KEY_COUNT);
    try (DaVinciClient<Integer, Integer> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, baseDataPath, daVinciConfig)) {
      assertThrows(VeniceException.class, () -> client.get(0).get());
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testNonLocalAccessPolicy(DaVinciConfig daVinciConfig) throws Exception {
    String storeName = cluster.createStore(KEY_COUNT);
    VeniceProperties backendConfig = new PropertyBuilder()
            .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, ROCKS_DB)
            .build();

    Map<Integer, Integer> keyValueMap = new HashMap<>();
    daVinciConfig.setNonLocalAccessPolicy(NonLocalAccessPolicy.QUERY_VENICE);
    try (DaVinciClient<Integer, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      client.subscribe(Collections.singleton(0)).get();
      // With QUERY_VENICE enabled, all key-value pairs should be found.
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client.get(k).get(), 1);
        keyValueMap.put(k, 1);
      }
      assertEquals(client.batchGet(keyValueMap.keySet()).get(), keyValueMap);
    }

    daVinciConfig.setNonLocalAccessPolicy(NonLocalAccessPolicy.FAIL_FAST);
    try (DaVinciClient<Integer, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      // We only subscribe to 1/3 of the partitions so some data will not be present locally.
      client.subscribe(Collections.singleton(0)).get();
      assertThrows(() -> client.batchGet(keyValueMap.keySet()).get());
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
    daVinciConfig.setNonLocalAccessPolicy(NonLocalAccessPolicy.QUERY_VENICE);
    try (DaVinciClient<Integer, Object> client = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      client.subscribe(Collections.singleton(0)).get();
      // With QUERY_VENICE enabled, all key-value pairs should be found.
      assertEquals(client.batchGet(keyValueMap.keySet()).get().size(), keyValueMap.size());
    }
  }

  // TODO: add comprehensive tests for memory limit feature
  @Test(timeOut = TEST_TIMEOUT)
  public void testMemoryLimit() throws Exception {
    VeniceProperties backendConfig = new PropertyBuilder()
            .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, ROCKS_DB)
            .build();
    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      String storeName = cluster.createStore(KEY_COUNT);
      DaVinciConfig daVinciConfig = new DaVinciConfig().setMemoryLimit(KEY_COUNT / 2);
      DaVinciClient<Integer, Object> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
      assertThrows(() -> client.subscribeAll().get(5, TimeUnit.SECONDS));
      Metric memoryUsageMetric = metricsRepository.getMetric(".RocksDBMemoryStats--" + storeName + ".rocksdb.memory-usage.Gauge");
      assertNotNull(memoryUsageMetric);
      double memoryUsage = memoryUsageMetric.value();
      assertTrue(memoryUsage > 0);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSubscribeAndUnsubscribe() throws Exception {
    // Verify DaVinci client doesn't hang in a deadlock when calling unsubscribe right after subscribing.
    // Enable ingestion isolation since it's more likely for the race condition to occur and make sure the future is
    // only completed when the main process's ingestion task is subscribed to avoid deadlock.
    String storeName = cluster.createStore(KEY_COUNT);
    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(SERVER_INGESTION_MODE, ISOLATED)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .build();
    daVinciConfig.setMemoryLimit(1024 * 1024 * 1024); // 1GB
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, new MetricsRepository(), backendConfig)) {
      DaVinciClient<String, GenericRecord> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
      client.subscribeAll().get();
      client.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUnsubscribeBeforeFutureGet() throws Exception {
    // Verify DaVinci client doesn't hang in a deadlock when calling unsubscribe right after subscribing and before the
    // the future is complete. The future should also return exceptionally.
    String storeName = cluster.createStore(10000); // A large amount of keys to give window for potential race conditions
    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(SERVER_INGESTION_MODE, ISOLATED)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .build();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    daVinciConfig.setMemoryLimit(1024 * 1024 * 1024); // 1GB
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, new MetricsRepository(), backendConfig)) {
      DaVinciClient<String, GenericRecord> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
      CompletableFuture future = client.subscribeAll();
      client.unsubscribeAll();
      future.get(); // Expecting exception here if we unsubscribed before subscribe was completed.
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof CancellationException);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT * 2)
  public void testLiveUpdateSuppression(boolean enableIngestionIsolation) throws Exception {
    final String storeName = Utils.getUniqueString("store");
    cluster.useControllerClient(client -> {
      NewStoreResponse response = client.createNewStore(storeName, getClass().getName(), DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
      // Update to hybrid store
      client.updateStore(storeName, new UpdateStoreQueryParams().setHybridRewindSeconds(10).setHybridOffsetLagThreshold(10));
    });

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName, 1024);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(DEFAULT_VALUE_SCHEMA);

    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();
    // Enable live update suppression
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS, "true")
        .put(SERVER_INGESTION_MODE, enableIngestionIsolation ? ISOLATED : BUILT_IN)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .build();

    VeniceWriter<Object, Object, byte[]> batchProducer = vwFactory.createVeniceWriter(topic, keySerializer, valueSerializer, false);
    int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
    batchProducer.broadcastStartOfPush(Collections.emptyMap());
    Future[] writerFutures = new Future[KEY_COUNT];
    for (int i = 0; i < KEY_COUNT; i++) {
      writerFutures[i] = batchProducer.put(i, i, valueSchemaId);
    }
    for (int i = 0; i < KEY_COUNT; i++) {
      writerFutures[i].get();
    }
    batchProducer.broadcastEndOfPush(Collections.emptyMap());

    CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, new MetricsRepository(), backendConfig);
    DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
    client.subscribe(Collections.singleton(0)).get();

    VeniceWriter<Object, Object, byte[]> realTimeProducer = vwFactory.createVeniceWriter(Version.composeRealTimeTopic(storeName),
        keySerializer, valueSerializer, false);
    writerFutures = new Future[KEY_COUNT];
    for (int i = 0; i < KEY_COUNT; i++) {
      writerFutures[i] = realTimeProducer.put(i, i * 1000, valueSchemaId);
    }
    for (int i = 0; i < KEY_COUNT; i++) {
      writerFutures[i].get();
    }

    /**
     * Since live update suppression is enabled, once the partition is ready to serve, da vinci client will stop ingesting
     * new messages and also ignore any new message
     */
    try {
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, false, true,
          () -> {
            /**
             * Try to read the new value from real-time producer; assertion should fail
             */
            for (int i = 0; i < KEY_COUNT; i++) {
              int result = client.get(i).get();
              assertEquals(result, i * 1000);
            }
          });
      // It's wrong if new value can be read from da-vinci client
      throw new VeniceException("Should not be able to read live updates.");
    } catch (AssertionError e) {
      // expected
    }
    client.close();
    factory.close();

    /**
     * After restarting da-vinci client, since live update suppression is enabled and there is local data, ingestion
     * will not start.
     *
     * da-vinci client restart is done by building a new factory and a new client
     */
    CachingDaVinciClientFactory factory2 = new CachingDaVinciClientFactory(d2Client, new MetricsRepository(), backendConfig);
    DaVinciClient<Integer, Integer> client2 = factory2.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
    client2.subscribeAll().get();
    for (int i = 0; i < KEY_COUNT; i++) {
      int result = client2.get(i).get();
      assertEquals(result, i);
    }
    try {
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, false, true,
          () -> {
            /**
             * Try to read the new value from real-time producer; assertion should fail
             */
            for (int i = 0; i < KEY_COUNT; i++) {
              int result = client2.get(i).get();
              assertEquals(result, i * 1000);
            }
          });
      // It's wrong if new value can be read from da-vinci client
      throw new VeniceException("Should not be able to read live updates.");
    } catch (AssertionError e) {
      // expected
    }
    /**
     * The Da Vinci client must be closed in order to release the {@link com.linkedin.davinci.client.AvroGenericDaVinciClient#daVinciBackend}
     * reference because it's a singleton; if we don't do this, other test cases will reuse the same singleton and have
     * live updates suppressed.
     */
    client2.close();
    factory2.close();

    batchProducer.close();
    realTimeProducer.close();
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testCrashedDaVinciWithIngestionIsolation() throws Exception {
    String storeName = cluster.createStore(KEY_COUNT);
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    String zkHosts = cluster.getZk().getAddress();
    ForkedJavaProcess forkedDaVinciUserApp = ForkedJavaProcess.exec(
        DaVinciUserApp.class,
        Arrays.asList(zkHosts, baseDataPath, storeName, "100", "10"),
        new ArrayList<>(),
        Optional.empty()
    );
    // Sleep long enough so the forked Da Vinci app process can finish ingestion.
    Thread.sleep(60000);
    IsolatedIngestionUtils.executeShellCommand("kill " + forkedDaVinciUserApp.pid());
    // Sleep long enough so the heartbeat timeout is detected by IsolatedIngestionServer.
    Thread.sleep(15000);
    D2Client d2Client = new D2ClientBuilder()
        .setZkHosts(zkHosts)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, zkHosts)
        .build();

    // Re-open the same store's database to verify RocksDB metadata partition's lock has been released.
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
      client.subscribeAll().get();
    }
  }

  @Test
  public void testComputeOnStoreWithQTFDScompliantSchema() throws Exception {
    final String storeName = Utils.getUniqueString( "store");
    cluster.useControllerClient(client -> TestUtils.assertCommand(
        client.createNewStore(storeName, getClass().getName(), DEFAULT_KEY_SCHEMA, VALUE_SCHEMA_FOR_COMPUTE_NULLABLE_LIST_FIELD)));

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName, 1024);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE_NULLABLE_LIST_FIELD);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();

    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(SERVER_INGESTION_MODE, ISOLATED)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .build();

    Map<Integer, GenericRecord> computeResult;
    final int key1 = 1;
    final int key2 = 2;
    Set<Integer> keySetForCompute = new HashSet<Integer>(){{
      add(key1);
      add(key2);
    }};

    try (
        VeniceWriter<Object, Object, byte[]> veniceWriter = vwFactory.createVeniceWriter(topic, keySerializer, valueSerializer, false);
        CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig);
        DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig())) {

      //Write data to DaVinci Store and subscribe client
      Schema valueSchema = Schema.parse(VALUE_SCHEMA_FOR_COMPUTE_NULLABLE_LIST_FIELD);
      List<Float> memberFeatureEmbedding = Arrays.asList(1.0f, 2.0f, 3.0f);
      GenericRecord value1 = new GenericData.Record(valueSchema);
      GenericRecord value2 = new GenericData.Record(valueSchema);
      value1.put("id", "1");
      value1.put("name", "companiesEmbedding");
      value1.put("member_feature", memberFeatureEmbedding);
      value2.put("id", "2");
      value2.put("name", "companiesEmbedding");
      value2.put("member_feature", null); // Null value instead of a list
      Map<Integer, GenericRecord> valuesByKey = new HashMap<>(2);
      valuesByKey.put(key1, value1);
      valuesByKey.put(key2, value2);

      pushRecordsToStore(valuesByKey, veniceWriter, 1);
      client.subscribeAll().get();

      final Consumer<Map<Integer, GenericRecord>> assertComputeResults = (readComputeResult) -> {
        // Expect no error
        readComputeResult.forEach((key, value) -> Assert.assertEquals(
            ((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0));
        // Results for key 2 should be all null since the nullable field in the value of key 2 is null
        Assert.assertNull(readComputeResult.get(key2).get("dot_product_result"));
        Assert.assertNull(readComputeResult.get(key2).get("hadamard_product_result"));
        Assert.assertNull(readComputeResult.get(key2).get("cosine_similarity_result"));
        // Results for key 1 should be non-null since the nullable field in the value of key 1 is non-null
        Assert.assertEquals(readComputeResult.get(key1).get("dot_product_result"), ComputeOperationUtils.dotProduct(memberFeatureEmbedding, memberFeatureEmbedding));
        Assert.assertEquals(readComputeResult.get(key1).get("hadamard_product_result"), ComputeOperationUtils.hadamardProduct(memberFeatureEmbedding, memberFeatureEmbedding));
        Assert.assertEquals(readComputeResult.get(key1).get("cosine_similarity_result"), 1.0f); // Cosine similarity between a vector and itself is 1.0
      };

      // Perform compute on client
      computeResult = client.compute()
          .dotProduct("member_feature", memberFeatureEmbedding, "dot_product_result")
          .hadamardProduct("member_feature", memberFeatureEmbedding, "hadamard_product_result")
          .cosineSimilarity("member_feature", memberFeatureEmbedding, "cosine_similarity_result")
          .execute(keySetForCompute).get();

      assertComputeResults.accept(computeResult);

      computeResult = client.compute()
          .dotProduct("member_feature", memberFeatureEmbedding, "dot_product_result")
          .hadamardProduct("member_feature", memberFeatureEmbedding, "hadamard_product_result")
          .cosineSimilarity("member_feature", memberFeatureEmbedding, "cosine_similarity_result")
          .streamingExecute(keySetForCompute).get();

      assertComputeResults.accept(computeResult);
      client.unsubscribeAll();
    }
  }


  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testReadComputeMissingField() throws Exception {
    //Create DaVinci store
    final String storeName = Utils.getUniqueString( "store");
    cluster.useControllerClient(client -> TestUtils.assertCommand(
        client.createNewStore(storeName, getClass().getName(), DEFAULT_KEY_SCHEMA, VALUE_SCHEMA_FOR_COMPUTE)));

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName, 1024);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE);
    VeniceKafkaSerializer valueSerializerMissingField = new VeniceAvroKafkaSerializer(
        VALUE_SCHEMA_FOR_COMPUTE_MISSING_FIELD);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();

    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(SERVER_INGESTION_MODE, ISOLATED)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .build();

    int numRecords = 100;
    Map<Integer, GenericRecord> computeResult;
    Set<Integer> keySetForCompute = new HashSet<Integer>(){{
      add(1);
      add(2);
    }};

    try (
        VeniceWriter<Object, Object, byte[]> writer = vwFactory.createVeniceWriter(topic, keySerializer, valueSerializer, false);
        CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig);
        DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig())) {

      //Write data to DaVinci Store and subscribe client
      pushSyntheticDataToStore(writer, VALUE_SCHEMA_FOR_COMPUTE, false, 1, numRecords);
      client.subscribeAll().get();

      /**
       * Perform compute on client with all fields
       *
       * This is necessary to add the compute result schema to {@link com.linkedin.davinci.client.AvroGenericDaVinciClient#computeResultSchemaCache}
       * and {@link com.linkedin.venice.client.store.AbstractAvroComputeRequestBuilder#RESULT_SCHEMA_CACHE}
       */
       computeResult = client.compute()
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .execute(keySetForCompute).get();

      computeResult.forEach((key, value) -> Assert.assertEquals(
          ((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0));

      computeResult = client.compute()
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .streamingExecute(keySetForCompute).get();

      computeResult.forEach((key, value) -> Assert.assertEquals(
          ((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0));

      client.unsubscribeAll();
    }

    // Add value schema for compute with a missing field
    cluster.useControllerClient(clientMissingField -> TestUtils.assertCommand(
        clientMissingField.addValueSchema(storeName, VALUE_SCHEMA_FOR_COMPUTE_MISSING_FIELD)));

    VersionCreationResponse newVersionMissingField = cluster.getNewVersion(storeName, 1024);
    String topicForMissingField = newVersionMissingField.getKafkaTopic();

    try(
      VeniceWriter<Object, Object, byte[]> writerForMissingField = vwFactory.createVeniceWriter(topicForMissingField, keySerializer, valueSerializerMissingField, false);
        CachingDaVinciClientFactory factoryForMissingFieldClient = new CachingDaVinciClientFactory(d2Client, new MetricsRepository(), backendConfig);
        DaVinciClient<Integer, Integer> clientForMissingField = factoryForMissingFieldClient.getAndStartGenericAvroClient(storeName, new DaVinciConfig())){

      // Write data to DaVinci store with a missing field and subscribe client
      pushSyntheticDataToStore(writerForMissingField, VALUE_SCHEMA_FOR_COMPUTE_MISSING_FIELD, true, 2, numRecords);
      clientForMissingField.subscribeAll().get();

      // Execute read compute on client which is missing the companiesEmbedding field
      computeResult = clientForMissingField.compute()
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .execute(keySetForCompute).get();

      computeResult.forEach((key, value) -> Assert.assertEquals(
          ((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 1));

      computeResult = clientForMissingField.compute()
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .streamingExecute(keySetForCompute).get();

      computeResult.forEach((key, value) -> Assert.assertEquals(
          ((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 1));

      clientForMissingField.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testReadComputeSwappedFields() throws Exception {
    //Create DaVinci store
    final String storeName = Utils.getUniqueString( "store");
    cluster.useControllerClient(client -> TestUtils.assertCommand(
        client.createNewStore(storeName, getClass().getName(), DEFAULT_KEY_SCHEMA, VALUE_SCHEMA_FOR_COMPUTE_SWAPPED)));

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName, 1024);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE);
    VeniceKafkaSerializer valueSerializerSwapped = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE_SWAPPED);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();

    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(SERVER_INGESTION_MODE, ISOLATED)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .build();

    int numRecords = 100;
    Map<Integer, GenericRecord> computeResult;
    Set<Integer> keySetForCompute = new HashSet<Integer>(){{
      add(1);
      add(2);
    }};

    try (
        VeniceWriter<Object, Object, byte[]> writer = vwFactory.createVeniceWriter(topic, keySerializer, valueSerializer, false);
        CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig);
        DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig())) {

      //Write data to DaVinci Store and subscribe client
      pushSyntheticDataToStore(writer, VALUE_SCHEMA_FOR_COMPUTE_SWAPPED, false, 1, numRecords);
      client.subscribeAll().get();

      /**
       * Perform compute on client with all fields
       *
       * This is necessary to add the compute result schema to {@link com.linkedin.davinci.client.AvroGenericDaVinciClient#computeResultSchemaCache}
       * and {@link com.linkedin.venice.client.store.AbstractAvroComputeRequestBuilder#RESULT_SCHEMA_CACHE}
       */
      computeResult = client.compute()
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .execute(keySetForCompute).get();

      computeResult.forEach((key, value) -> Assert.assertEquals(
          ((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0));
      client.unsubscribeAll();
    }

    // Add value schema for compute
    cluster.useControllerClient(clientMissingField -> TestUtils.assertCommand(
        clientMissingField.addValueSchema(storeName, VALUE_SCHEMA_FOR_COMPUTE_MISSING_FIELD)));

    VersionCreationResponse newVersionMissingField = cluster.getNewVersion(storeName, 1024);
    String topicForMissingField = newVersionMissingField.getKafkaTopic();

    try(
      VeniceWriter<Object, Object, byte[]> writer2 = vwFactory.createVeniceWriter(topicForMissingField, keySerializer, valueSerializerSwapped, false);
      CachingDaVinciClientFactory factory2 = new CachingDaVinciClientFactory(d2Client, new MetricsRepository(), backendConfig);
      DaVinciClient<Integer, Integer> client2 = factory2.getAndStartGenericAvroClient(storeName, new DaVinciConfig())){

      // Write data to DaVinci store
      pushSyntheticDataToStore(writer2, VALUE_SCHEMA_FOR_COMPUTE_SWAPPED, false, 2, numRecords);
      client2.subscribeAll().get();

      // Execute read compute on new client
      computeResult = client2.compute()
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .execute(keySetForCompute).get();

      computeResult.forEach((key, value) -> Assert.assertEquals(
          ((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0));

      client2.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testComputeStreamingExecute() throws ExecutionException, InterruptedException {
    //Setup Store
    final String storeName = Utils.getUniqueString( "store");
    cluster.useControllerClient(client -> TestUtils.assertCommand(
        client.createNewStore(storeName, getClass().getName(), KEY_SCHEMA_STEAMING_COMPUTE,
            VALUE_SCHEMA_STREAMING_COMPUTE)));

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName, 1024);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STEAMING_COMPUTE);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STREAMING_COMPUTE);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();

    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(SERVER_INGESTION_MODE, ISOLATED)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .build();

    int numRecords = 10000;

    //setup keyset to test
    Set<String> keySet = new TreeSet<>();
    /**
     * {@link NON_EXISTING_KEY1}: "a_unknown_key" will be with key index: 0 internally, and we want to verify
     * whether the code could handle non-existing key with key index: 0
     */
    keySet.add(NON_EXISTING_KEY1);
    for (int i = 0; i < MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM; ++i) {
      keySet.add(KEY_PREFIX + i);
    }
    keySet.add(NON_EXISTING_KEY2);

    DaVinciConfig config = new DaVinciConfig();
    config.setNonLocalAccessPolicy(NonLocalAccessPolicy.QUERY_VENICE);

    try (
        VeniceWriter<Object, Object, byte[]> writer = vwFactory.createVeniceWriter(topic, keySerializer, valueSerializer, false);
        CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig);
        DaVinciClient<String, Integer> client = factory.getAndStartGenericAvroClient(storeName, config)) {

      //push data to store and subscribe client
      pushDataToStoreForStreamingCompute(writer, VALUE_SCHEMA_STREAMING_COMPUTE, HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID, numRecords);
      client.subscribeAll().get();

      // Test compute with streaming execute using custom provided callback
      AtomicInteger computeResultCnt = new AtomicInteger(0);
      Map<String, GenericRecord> finalComputeResultMap = new VeniceConcurrentHashMap<>();
      CountDownLatch computeLatch = new CountDownLatch(1);

      ComputeRequestBuilder<String> computeRequestBuilder = client.compute().project("int_field");
      computeRequestBuilder.streamingExecute(keySet, new StreamingCallback<String, GenericRecord>() {
        @Override
        public void onRecordReceived(String key, GenericRecord value) {
          computeResultCnt.incrementAndGet();
          if (null != value) {
            finalComputeResultMap.put(key, value);
          }
        }

        @Override
        public void onCompletion(Optional<Exception> exception) {
          computeLatch.countDown();
          if (exception.isPresent()) {
            Assert.fail("Exception: " + exception.get() + " is not expected");
          }
        }
      });
      computeLatch.await();

      Assert.assertEquals(computeResultCnt.get(), MAX_KEY_LIMIT);
      Assert.assertEquals(finalComputeResultMap.size(), MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM); // Without non-existing key
      verifyStreamingComputeResult(finalComputeResultMap);

      // Test compute with streaming execute using default callback
      CompletableFuture<VeniceResponseMap<String, GenericRecord>> computeFuture = computeRequestBuilder.streamingExecute(keySet);
      Map<String, GenericRecord> computeResultMap = computeFuture.get();
      Assert.assertEquals(computeResultMap.size(), MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM);
      verifyStreamingComputeResult(computeResultMap);

      client.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider="LF-And-CompressionStrategy")
  public void testReadCompressedData(boolean leaderFollowerEnabled, CompressionStrategy compressionStrategy) throws Exception {
    String storeName = Utils.getUniqueString("batch-store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setLeaderFollowerModel(leaderFollowerEnabled).setCompressionStrategy(compressionStrategy);
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
  public void testPartialKeyLookupWithRocksDBBlockBasedTable() throws ExecutionException, InterruptedException {
    final String storeName = Utils.getUniqueString( "store");
    cluster.useControllerClient(client -> TestUtils.assertCommand(
        client.createNewStore(storeName, getClass().getName(), KEY_SCHEMA_PARTIAL_KEY_LOOKUP, VALUE_SCHEMA_FOR_COMPUTE)));

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName, 1024);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_PARTIAL_KEY_LOOKUP);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    int numRecords = 100;

    try (
        VeniceWriter<GenericRecord, GenericRecord, byte[]> writer = vwFactory.createVeniceWriter(topic, keySerializer, valueSerializer, false);
        CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig);
        DaVinciClient<GenericRecord, GenericRecord> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig().setStorageClass(StorageClass.DISK))) {

      pushSyntheticDataToStoreForPartialKeyLookup(writer, KEY_SCHEMA_PARTIAL_KEY_LOOKUP, VALUE_SCHEMA_FOR_COMPUTE, false, 1, numRecords);
      client.subscribeAll().get();

      Map<GenericRecord, GenericRecord> finalComputeResultMap = new VeniceConcurrentHashMap<>();
      CountDownLatch computeLatch = new CountDownLatch(1);

      Predicate partialKey = and(
          equalTo("id", "key_abcdefgh_1"),
          equalTo("companyId", 0)
      );

      Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_PARTIAL_KEY_LOOKUP);

      GenericData.Record key = new GenericData.Record(keySchema);
      key.put("id", "key_abcdefgh_1");
      key.put("companyId", 0);
      key.put("name", "name_4");

      GenericRecord result = client.get(key).get();
      Assert.assertNotNull(result);

      client.compute()
          .project("id", "name", "companiesEmbedding", "member_feature")
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .executeWithFilter(partialKey, new StreamingCallback<GenericRecord, GenericRecord>() {
            @Override
            public void onRecordReceived(GenericRecord key, GenericRecord value) {
              if (null != value) {
                finalComputeResultMap.put(key, value);
              }
            }

            @Override
            public void onCompletion(Optional<Exception> exception) {
              computeLatch.countDown();
              if (exception.isPresent()) {
                Assert.fail("Exception: " + exception.get() + " is not expected");
              }
            }
          });

      computeLatch.await();
      Assert.assertEquals(finalComputeResultMap.size(), 16);
      finalComputeResultMap.forEach((key1, value) -> {
        Assert.assertEquals(key1.getSchema(), keySchema);
        Assert.assertEquals(key1.get("id").toString(), "key_abcdefgh_1");
        Assert.assertEquals(key1.get("companyId"), 0);
        Assert.assertEquals(value.getSchema().getFields().size(), 7);
        Assert.assertEquals(((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testPartialKeyLookupWithRocksDBPlainTable() throws ExecutionException, InterruptedException {
    final String storeName = Utils.getUniqueString( "store");
    cluster.useControllerClient(client -> TestUtils.assertCommand(
        client.createNewStore(storeName, getClass().getName(), KEY_SCHEMA_PARTIAL_KEY_LOOKUP, VALUE_SCHEMA_FOR_COMPUTE)));

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName, 1024);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_PARTIAL_KEY_LOOKUP);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    int numRecords = 100;

    try (
        VeniceWriter<GenericRecord, GenericRecord, byte[]> writer = vwFactory.createVeniceWriter(topic, keySerializer, valueSerializer, false);
        CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig);
        DaVinciClient<GenericRecord, GenericRecord> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig())) {

      pushSyntheticDataToStoreForPartialKeyLookup(writer, KEY_SCHEMA_PARTIAL_KEY_LOOKUP, VALUE_SCHEMA_FOR_COMPUTE, false, 1, numRecords);
      client.subscribeAll().get();

      Predicate partialKey = and(
          equalTo("id", "key_abcdefgh_1"),
          equalTo("companyId", 0)
      );

      final boolean[] completed = {false};

      client.compute()
          .project("id", "name", "companiesEmbedding", "member_feature")
          .executeWithFilter(partialKey, new StreamingCallback<GenericRecord, GenericRecord>() {
            @Override
            public void onRecordReceived(GenericRecord key, GenericRecord value) {
              Assert.fail("No records should have been found from the store engine.");
            }

            @Override
            public void onCompletion(Optional<Exception> exception) {
              completed[0] = true;
              Assert.assertTrue(exception.isPresent());
              Assert.assertEquals(exception.get().getMessage(), "Get by key prefix is not supported with RocksDB PlainTable Format.");
            }
          });
      Assert.assertTrue(completed[0]);
    }
  }

  private void pushSyntheticDataToStore(VeniceWriter<Object, Object, byte[]> writer, String schema, boolean skip, int valueSchemaId, int numRecords)
      throws ExecutionException, InterruptedException {
    writer.broadcastStartOfPush(Collections.emptyMap());
    Schema valueSchema = Schema.parse(schema);

    for (int i = 0; i < numRecords; ++i) {
      GenericRecord value = new GenericData.Record(valueSchema);
      value.put("id", VALUE_PREFIX + i);
      value.put("name", "companiesEmbedding");
      if (!skip) {
        value.put("companiesEmbedding", companiesEmbedding);
      }
      value.put("member_feature", mfEmbedding);
      writer.put(i, value, valueSchemaId).get();
    }
    writer.broadcastEndOfPush(Collections.emptyMap());
  }

  private void pushRecordsToStore(
      Map<Integer, GenericRecord> valuesByKey,
      VeniceWriter<Object, Object, byte[]> veniceWriter,
      int valueSchemaId
  ) throws Exception {

    veniceWriter.broadcastStartOfPush(Collections.emptyMap());
    for (Map.Entry<Integer, GenericRecord> keyValue : valuesByKey.entrySet()) {
      veniceWriter.put(keyValue.getKey(), keyValue.getValue(), valueSchemaId).get();
    }
    veniceWriter.broadcastEndOfPush(Collections.emptyMap());
  }

  private void pushDataToStoreForStreamingCompute(VeniceWriter<Object, Object, byte[]> writer, String valueSchemaString,
      int valueSchemaId, int numRecords) throws ExecutionException, InterruptedException {
    writer.broadcastStartOfPush(Collections.emptyMap());
    Schema.Parser schemaParser = new Schema.Parser();
    Schema valueSchema = schemaParser.parse(valueSchemaString);

    for (int i = 0; i < numRecords; i++){
      GenericRecord valueRecord = new GenericData.Record(valueSchema);
      valueRecord.put("int_field", i);
      valueRecord.put("float_field", i + 100.0f);
      writer.put(KEY_PREFIX + i, valueRecord,   valueSchemaId).get();
    }

    writer.broadcastEndOfPush(Collections.emptyMap());
  }

  private void verifyStreamingComputeResult(Map<String, GenericRecord> resultMap) {
    for (int i = 0; i < MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM; ++i) {
      String key = KEY_PREFIX + i;
      GenericRecord record = resultMap.get(key);
      Assert.assertEquals(record.get("int_field"), i);
      Assert.assertNull(record.get("float_field"));
    }
  }

  private void pushSyntheticDataToStoreForPartialKeyLookup(VeniceWriter<GenericRecord, GenericRecord, byte[]> writer,
      String keySchemaString, String valueSchemaString, boolean skip, int valueSchemaId, int numRecords)
      throws ExecutionException, InterruptedException {
    writer.broadcastStartOfPush(Collections.emptyMap());
    Schema keySchema = new Schema.Parser().parse(keySchemaString);
    Schema valueSchema = new Schema.Parser().parse(valueSchemaString);
    String keyIdFiller = "abcdefgh_";
    for (int i = 0; i < numRecords; ++i) {
      GenericRecord key = new GenericData.Record(keySchema);
      key.put("id", KEY_PREFIX + keyIdFiller + (i % 3));
      key.put("companyId", i % 2);
      key.put("name", "name_" + i);

      GenericRecord value = new GenericData.Record(valueSchema);
      value.put("id", VALUE_PREFIX + i);
      value.put("name", "companiesEmbedding");
      if (!skip) {
        value.put("companiesEmbedding", companiesEmbedding);
      }
      value.put("member_feature", mfEmbedding);
      writer.put(key, value, valueSchemaId).get();
    }
    writer.broadcastEndOfPush(Collections.emptyMap());
  }

  private List<Float> generateRandomFloatList(int listSize) {
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    List<Float> feature = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      feature.add(rand.nextFloat());
    }
    return feature;
  }

  private void setupHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer) throws Exception {
    setupHybridStore(storeName, paramsConsumer, KEY_COUNT);
  }

  private void setupHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer, int keyCount) throws Exception {
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
      try {
        for (int i = 0; i < keyCount; i++) {
          TestPushUtils.sendStreamingRecord(producer, storeName, i, i);
        }
      } finally {
        producer.stop();
      }
    }
  }

  private void generateHybridData(String storeName, List<Pair<Object, Object>> dataToWrite) {
    SystemProducer producer = TestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM,
            Pair.create(VeniceSystemFactory.VENICE_PARTITIONERS, ConstantVenicePartitioner.class.getName()));
    try {
      for (Pair<Object,Object> record : dataToWrite) {
        TestPushUtils.sendStreamingRecord(producer, storeName, record.getFirst(), record.getSecond());
      }
    } finally {
      producer.stop();
    }
  }

  private void setUpStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<Properties> propertiesConsumer) throws Exception {
    // Produce input data.
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    writeSimpleAvroFileWithIntToStringSchema(inputDir, true);

    // Setup H2V job properties.
    Properties h2vProperties = defaultH2VProps(cluster, inputDirPath, storeName);
    propertiesConsumer.accept(h2vProperties);
    // Create & update store for test.
    final int numPartitions = 3;
    UpdateStoreQueryParams params = new UpdateStoreQueryParams()
        .setPartitionCount(numPartitions); // Update the partition count.
    paramsConsumer.accept(params);
    try (ControllerClient controllerClient =
        createStoreForJob(cluster, DEFAULT_KEY_SCHEMA, "\"string\"", h2vProperties)) {
      ControllerResponse response = controllerClient.updateStore(storeName, params);
      Assert.assertFalse(response.isError(), response.getError());

      // Push data through H2V bridge.
      runH2V(h2vProperties, 1, cluster);
    }
  }

  private static void runH2V(Properties h2vProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long h2vStart = System.currentTimeMillis();
    String jobName = Utils.getUniqueString("batch-job-" + expectedVersionNumber);
    TestPushUtils.runPushJob(jobName, h2vProperties);
    String storeName = (String) h2vProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
  }
}
