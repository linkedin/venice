package com.linkedin.venice.fastclient;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.meta.PersistenceType.*;
import static com.linkedin.venice.system.store.MetaStoreWriter.*;
import static org.testng.Assert.*;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.schema.TestValueSchema;
import com.linkedin.venice.fastclient.stats.ClientStats;
import com.linkedin.venice.fastclient.utils.RouterBasedStoreMetadata;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;


/**
 * This is copied from the AvroClientEndToEndTest class . Will need to adapt to batchget ( scatter gather) use cases .
 * Some of the logic might not be needed and will need to be cleaned up
 * TODO :
 * Metrics . Test if metrics are what we expect
 * Error handling. How can we simulate synchronization issues
 * Da Vinci metadata. Do we need to test with that? What is the use case when we use da vinci metadata
 */
public class BatchGetAvroStoreClientTest {
  // Every test will print all stats if set to true. Should only be used locally
  private static boolean PRINT_STATS = false;
  private static final Logger LOGGER = LogManager.getLogger(BatchGetAvroStoreClientTest.class);
  protected final String keyPrefix = "key_";
  protected final int recordCnt = 100;
  protected VeniceClusterWrapper veniceCluster;
  protected String storeVersionName;
  protected int valueSchemaId;
  protected String storeName;
  protected VeniceKafkaSerializer keySerializer;
  protected VeniceKafkaSerializer valueSerializer;
  private VeniceWriter<Object, Object, Object> veniceWriter;
  private Client r2Client;
  private D2Client d2Client;
  private static final long TIME_OUT_IN_SECONDS = 60;
  protected static final String KEY_SCHEMA_STR = "\"string\"";
  protected static final String VALUE_FIELD_NAME = "int_field";
  protected static final String VALUE_SCHEMA_STR = "{\n" + "\"type\": \"record\",\n"
      + "\"name\": \"TestValueSchema\",\n" + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\"}]\n" + "}";
  protected static final Schema VALUE_SCHEMA = new Schema.Parser().parse(VALUE_SCHEMA_STR);
  private ClientConfig clientConfig;
  private VeniceProperties daVinciBackendConfig;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    Properties props = new Properties();
    props.put(SERVER_HTTP2_INBOUND_ENABLED, "true");
    veniceCluster = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 100, true, false, props);
    r2Client = ClientTestUtils.getR2Client(true);
    d2Client = D2TestUtils.getAndStartD2Client(veniceCluster.getZk().getAddress());
    prepareData();
    prepareMetaSystemStore();
  }

  /**
   * Creates a batchget request which uses scatter gather to fetch all keys from different replicas.
   */
  @Test
  @Ignore("For future use. We will enable batchget implemetation using streaming apis once streaming implementation is stabilized")
  public void testBatchGetGenericClient() throws Exception {

    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient();

    Set<String> keys = new HashSet<String>();
    for (int i = 0; i < recordCnt; i++) {
      keys.add(keyPrefix + i);
    }
    keys.add("nonExistingKey");
    Map<String, GenericRecord> results = genericFastClient.batchGet(keys).get();
    // Assertions
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      GenericRecord value = results.get(key);
      Assert.assertEquals(value.get(VALUE_FIELD_NAME), i);
    }
    ClientStats stats = clientConfig.getStats(RequestType.MULTI_GET);
    LOGGER.info("STATS: {}", stats.buildSensorStatSummary("multiget_healthy_request_latency"));
  }

  @Test
  @Ignore("For future use once streaming implementation is stabilized")
  public void testBatchGetSpecificClient() throws Exception {
    AvroSpecificStoreClient<String, TestValueSchema> specificFastClient = getSpecificFastClient();

    Set<String> keys = new HashSet<String>();
    for (int i = 0; i < recordCnt; i++) {
      keys.add(keyPrefix + i);
    }
    Map<String, TestValueSchema> results = specificFastClient.batchGet(keys).get();
    // Assertions
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      GenericRecord value = results.get(key);
      Assert.assertEquals(value.get(VALUE_FIELD_NAME), i);
    }
    specificFastClient.close();
  }

  @Test
  public void testStreamingBatchGetCallbackGenericClient() throws Exception {
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient();
    Set<String> keys = new HashSet<String>();
    for (int i = 0; i < recordCnt; i++) {
      keys.add(keyPrefix + i);
    }
    keys.add("nonExisting");
    Map<String, GenericRecord> results = new ConcurrentHashMap<>();
    AtomicBoolean isComplete = new AtomicBoolean();
    AtomicBoolean isDuplicate = new AtomicBoolean();
    CountDownLatch completeLatch = new CountDownLatch(1);
    genericFastClient.streamingBatchGet(keys, new StreamingCallback<String, GenericRecord>() {
      @Override
      public void onRecordReceived(String key, GenericRecord value) {
        LOGGER.info("Record received {}:{}", key, value);
        if ("nonExisting".equals(key)) {
          Assert.assertNull(value);
        } else {
          if (results.containsKey(key)) {
            isDuplicate.set(true);
          } else {
            results.put(key, value);
          }
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        LOGGER.info("Exception received {}", exception);
        Assert.assertEquals(exception, Optional.empty());
        isComplete.set(true);
        completeLatch.countDown();
      }
    });

    // Wait until isComplete is true or timeout
    if (!completeLatch.await(TIME_OUT_IN_SECONDS, TimeUnit.SECONDS)) {
      Assert.fail("Test did not complete within timeout");
    }

    Assert.assertFalse(isDuplicate.get(), "Duplicate records received");
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      GenericRecord value = results.get(key);
      Assert.assertNotNull(value, "Expected non null value but got null for key " + key);
      Assert.assertEquals(
          value.get(VALUE_FIELD_NAME),
          i,
          "Expected value " + i + " for key " + key + " but got " + value.get(VALUE_FIELD_NAME));
    }
    Assert.assertEquals(
        results.size(),
        recordCnt,
        "Incorrect record count . Expected " + recordCnt + " actual " + results.size());
    Assert.assertFalse(
        results.containsKey("nonExisting"),
        " Results contained nonExisting key with value " + results.get("nonExisting"));
    ClientStats stats = clientConfig.getStats(RequestType.MULTI_GET);
    List<Double> metricValues = stats.getMetricValues("multiget_request_key_count", "Avg");
    Assert.assertEquals(metricValues.get(0), 101.0);
    metricValues = stats.getMetricValues("multiget_success_request_key_count", "Avg");
    Assert.assertEquals(metricValues.get(0), 100.0);

    LOGGER.info(
        "STATS: latency -> {}",
        stats.buildSensorStatSummary("multiget_healthy_request_latency", "99thPercentile"));
    printAllStats();
  }

  /* Interpretation of available stats when printstats is enabled
  * multiget_request:OccurrenceRate=0.03329892444474044 -> no. of total requests per second
  * multiget_healthy_request:OccurrenceRate=0.03331556503198294 -> No. of healthy request per second. This is over a
    30 second window so the numbers won't make sense for a unit test
  * multiget_healthy_request_latency:99_9thPercentile=895.5951595159517,90thPercentile=895.5951595159517,
    77thPercentile=895.5951595159517,Avg=895.929393,99thPercentile=895.5951595159517,
    95thPercentile=895.5951595159517,50thPercentile=895.5951595159517
    -> latency %iles for the whole request. This measures time until onCompletion is called
  * multiget_request_key_count:Max=101.0,Rate=3.2674452460289216,Avg=101.0 -> no. of keys processed and rate per sec
  * multiget_success_request_key_count:Max=100.0,Rate=3.3315565031982945,Avg=100.0
      -> same as above but for successful requests.
  * multiget_success_request_key_ratio:SimpleRatioStat=1.0196212185184406 (number of success keys / no. of total keys)
  * multiget_success_request_ratio:SimpleRatioStat=1.0 -> no. of successful / total requests
  */

  @Test
  public void testStreamingBatchGetGenericClient() throws Exception {
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient();

    Set<String> keys = new HashSet<String>();
    for (int i = 0; i < recordCnt; i++) {
      keys.add(keyPrefix + i);
    }
    keys.add("nonExisting");

    VeniceResponseMap<String, GenericRecord> veniceResponseMap =
        genericFastClient.streamingBatchGet(keys).get(1, TimeUnit.MINUTES);
    Assert.assertEquals(veniceResponseMap.getNonExistingKeys().size(), 1);
    Assert.assertTrue(veniceResponseMap.isFullResponse());
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      GenericRecord value = veniceResponseMap.get(key);
      Assert.assertNotNull(value, "Expected non null value but got null for key " + key);
      Assert.assertEquals(
          value.get(VALUE_FIELD_NAME),
          i,
          "Expected value " + i + " for key " + key + " but got " + value.get(VALUE_FIELD_NAME));
    }
    Assert.assertEquals(
        veniceResponseMap.size(),
        recordCnt,
        "Incorrect record count . Expected " + recordCnt + " actual " + veniceResponseMap.size());
    Assert.assertFalse(
        veniceResponseMap.containsKey("nonExisting"),
        " Results contained nonExisting key with value " + veniceResponseMap.get("nonExisting"));
    Assert.assertNotNull(veniceResponseMap.getNonExistingKeys(), " Expected non existing keys to be not null");
    Assert.assertEquals(
        veniceResponseMap.getNonExistingKeys().size(),
        1,
        "Incorrect non existing key size . Expected  1 got " + veniceResponseMap.getNonExistingKeys().size());
  }

  /// Helper methods
  protected void prepareData() throws Exception {
    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(KEY_SCHEMA_STR, VALUE_SCHEMA_STR);
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // TODO: Make serializers parameterized so we test them all.
    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STR);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STR);

    veniceWriter = TestUtils.getVeniceWriterFactory(veniceCluster.getKafka().getAddress())
        .createVeniceWriter(storeVersionName, keySerializer, valueSerializer);
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < recordCnt; ++i) {
      GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
      record.put(VALUE_FIELD_NAME, i);
      veniceWriter.put(keyPrefix + i, record, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName)
          .getStore()
          .getCurrentVersion();
      return currentVersion == pushVersion;
    });
  }

  private void prepareMetaSystemStore() throws Exception {
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    veniceCluster.useControllerClient(controllerClient -> {
      VersionCreationResponse metaSystemStoreVersionCreationResponse =
          controllerClient.emptyPush(metaSystemStoreName, "test_bootstrap_meta_system_store", 10000);
      assertFalse(
          metaSystemStoreVersionCreationResponse.isError(),
          "New version creation for meta system store failed with error: "
              + metaSystemStoreVersionCreationResponse.getError());
      TestUtils.waitForNonDeterministicPushCompletion(
          metaSystemStoreVersionCreationResponse.getKafkaTopic(),
          controllerClient,
          30,
          TimeUnit.SECONDS);
      daVinciBackendConfig = new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
          .put(PERSISTENCE_TYPE, ROCKS_DB)
          .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
          .put(CLIENT_USE_DA_VINCI_BASED_SYSTEM_STORE_REPOSITORY, true)
          .build();
    });

    // Verify meta system store received the snapshot writes.
    try (AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> metaClient =
        com.linkedin.venice.client.store.ClientFactory.getAndStartSpecificAvroClient(
            com.linkedin.venice.client.store.ClientConfig
                .defaultSpecificClientConfig(metaSystemStoreName, StoreMetaValue.class)
                .setVeniceURL(veniceCluster.getRandomRouterURL())
                .setSslFactory(SslUtils.getVeniceLocalSslFactory()))) {
      StoreMetaKey replicaStatusKey =
          MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(new HashMap<String, String>() {
            {
              put(KEY_STRING_STORE_NAME, storeName);
              put(KEY_STRING_CLUSTER_NAME, veniceCluster.getClusterName());
              put(
                  KEY_STRING_VERSION_NUMBER,
                  Integer.toString(Version.parseVersionFromVersionTopicName(storeVersionName)));
              put(KEY_STRING_PARTITION_ID, "0");
            }
          });
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        assertNotNull(metaClient.get(replicaStatusKey).get());
      });
    }
  }

  private AvroGenericStoreClient<String, GenericRecord> getGenericFastClient() {
    VeniceRouterWrapper routerWrapper = veniceCluster.getRandomVeniceRouter();

    // Test generic store client
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<Object, Object, SpecificRecord>();
    clientConfigBuilder.setStoreName(storeName);
    clientConfigBuilder.setR2Client(r2Client);
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfigBuilder.setSpeculativeQueryEnabled(true);
    clientConfigBuilder.setDualReadEnabled(false);
    AvroGenericStoreClient<String, GenericRecord> genericThinClient =
        com.linkedin.venice.client.store.ClientFactory.getAndStartGenericAvroClient(
            com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig(storeName)
                .setVeniceURL(veniceCluster.getRandomRouterSslURL())
                .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
    clientConfigBuilder.setGenericThinClient(genericThinClient);

    CachingDaVinciClientFactory daVinciClientFactory =
        new CachingDaVinciClientFactory(d2Client, new MetricsRepository(), daVinciBackendConfig);
    DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore =
        daVinciClientFactory.getAndStartSpecificAvroClient(
            VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName),
            new DaVinciConfig(),
            StoreMetaValue.class);
    clientConfigBuilder.setDaVinciClientForMetaStore(daVinciClientForMetaStore);
    clientConfig = clientConfigBuilder.build();

    return ClientFactory.getAndStartGenericStoreClient(clientConfig);
  }

  private AvroSpecificStoreClient<String, TestValueSchema> getSpecificFastClient() {
    VeniceRouterWrapper routerWrapper = veniceCluster.getRandomVeniceRouter();

    // Test generic store client
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<Object, Object, SpecificRecord>();
    clientConfigBuilder.setStoreName(storeName);
    clientConfigBuilder.setR2Client(r2Client);
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfigBuilder.setSpeculativeQueryEnabled(true);
    clientConfigBuilder.setDualReadEnabled(false);
    clientConfigBuilder.setSpecificValueClass(TestValueSchema.class);

    AvroSpecificStoreClient<String, TestValueSchema> specificThinClient =
        com.linkedin.venice.client.store.ClientFactory.getAndStartSpecificAvroClient(
            com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig(storeName)
                .setSpecificValueClass(TestValueSchema.class)
                .setVeniceURL(veniceCluster.getRandomRouterSslURL())
                .setSslFactory(SslUtils.getVeniceLocalSslFactory()));

    clientConfigBuilder.setSpecificThinClient(specificThinClient);

    ClientConfig clientConfig = clientConfigBuilder.build();
    RouterBasedStoreMetadata storeMetadata = new RouterBasedStoreMetadata(
        routerWrapper.getMetaDataRepository(),
        routerWrapper.getSchemaRepository(),
        routerWrapper.getRoutingDataRepository(),
        storeName,
        clientConfig);

    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    return ClientFactory.getAndStartSpecificStoreClient(storeMetadata, clientConfigBuilder.build());
  }

  private void printAllStats() {
    /* The print_stats flag controls if all stats are printed. Usually it will be too much info but while running
     * locally this can help understand what metrics are used */
    if (!PRINT_STATS || clientConfig == null)
      return;
    // Prints all available stats. Useful for debugging
    Map<String, List<Metric>> metricsSan = new HashMap<>();
    ClientStats stats = clientConfig.getStats(RequestType.MULTI_GET);
    MetricsRepository metricsRepository = stats.getMetricsRepository();
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    for (Map.Entry<String, ? extends Metric> metricName: metrics.entrySet()) {
      String metricNameSan = StringUtils.substringBetween(metricName.getKey(), "--", ".");
      if (StringUtils.startsWith(metricNameSan, "multiget_")) {
        metricsSan.computeIfAbsent(metricNameSan, (k) -> new ArrayList<>()).add(metricName.getValue());
      }
    }
    List<String> valMetrics = new ArrayList<>();
    List<String> noValMetrics = new ArrayList<>();
    for (Map.Entry<String, List<Metric>> metricNameSan: metricsSan.entrySet()) {
      boolean hasVal = false;
      for (Metric metric: metricNameSan.getValue()) {
        if (metric != null) {
          double value = metric.value();
          if (Double.isFinite(value) && value != 0f) {
            hasVal = true;
          }
        }
      }
      if (!hasVal) {
        noValMetrics.add(
            metricNameSan + ":"
                + metricsSan.get(metricNameSan.getKey())
                    .stream()
                    .map((m) -> StringUtils.substringAfterLast(m.name(), "."))
                    .collect(Collectors.joining(",")));
      } else {
        valMetrics.add(
            metricNameSan + ":"
                + metricsSan.get(metricNameSan.getKey())
                    .stream()
                    .map((m) -> StringUtils.substringAfterLast(m.name(), ".") + "=" + m.value())
                    .collect(Collectors.joining(",")));
      }
    }
    valMetrics.sort(Comparator.naturalOrder());
    noValMetrics.sort(Comparator.naturalOrder());
    LOGGER.info("STATS: Metrics with values -> \n    {}", String.join("\n    ", valMetrics));
    LOGGER.info("STATS: Metrics with noValues -> \n    {}", String.join("\n    ", noValMetrics));
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (r2Client != null) {
      r2Client.shutdown(null);
    }
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
    Utils.closeQuietlyWithErrorLogged(veniceWriter);
  }
}
