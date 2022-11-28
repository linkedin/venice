package com.linkedin.venice.fastclient;

import static com.linkedin.venice.ConfigKeys.CLIENT_USE_DA_VINCI_BASED_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_PARTITION_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_VERSION_NUMBER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.schema.TestValueSchema;
import com.linkedin.venice.fastclient.utils.RouterBasedStoreMetadata;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class AvroStoreClientEndToEndTest {
  protected VeniceClusterWrapper veniceCluster;
  protected String storeVersionName;
  protected int valueSchemaId;
  protected String storeName;

  protected VeniceKafkaSerializer keySerializer;
  protected VeniceKafkaSerializer valueSerializer;
  private VeniceWriter<Object, Object, Object> veniceWriter;
  private Client r2Client;
  private VeniceProperties daVinciBackendConfig;
  private D2Client d2Client;
  DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore;
  CachingDaVinciClientFactory daVinciClientFactory;

  private static final long TIME_OUT = 60 * Time.MS_PER_SECOND;
  protected static final String KEY_SCHEMA_STR = "\"string\"";
  protected static final String VALUE_FIELD_NAME = "int_field";
  protected static final String VALUE_SCHEMA_STR = "{\n" + "\"type\": \"record\",\n"
      + "\"name\": \"TestValueSchema\",\n" + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\"}]\n" + "}";
  protected static final Schema VALUE_SCHEMA = new Schema.Parser().parse(VALUE_SCHEMA_STR);

  protected final String keyPrefix = "key_";
  protected final int recordCnt = 100;

  // two sizes: default 2 and max of recordCnt
  // TODO figure out where this count is checked and limited to a global or store based max value
  public final Object[] BATCH_GET_KEY_SIZE = { 2, recordCnt };

  // useDaVinciClientBasedMetadata is true/false. Testing both legacy and the current implementation
  @DataProvider(name = "FastClient-Four-Boolean-And-A-Number")
  public Object[][] fourBooleanAndANumber() {
    return DataProviderUtils.allPermutationGenerator(
        DataProviderUtils.BOOLEAN,
        DataProviderUtils.BOOLEAN,
        DataProviderUtils.BOOLEAN,
        DataProviderUtils.BOOLEAN,
        BATCH_GET_KEY_SIZE);
  }

  protected Client constructR2Client() throws Exception {
    return ClientTestUtils.getR2Client(true);
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    Properties props = new Properties();
    props.put(SERVER_HTTP2_INBOUND_ENABLED, "true");
    veniceCluster = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 100, true, false, props);

    r2Client = constructR2Client();

    d2Client = D2TestUtils.getAndStartD2Client(veniceCluster.getZk().getAddress());

    prepareData();
    prepareMetaSystemStore();
  }

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
    // Insert test record and wait synchronously for it to succeed by calling get() on the future
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
    final String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
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

  private void runTest(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      boolean useDaVinciClientBasedMetadata,
      boolean batchGet,
      int batchGetKeySize) throws Exception {
    runTest(clientConfigBuilder, useDaVinciClientBasedMetadata, batchGet, batchGetKeySize, (metricsRepository) -> {});
  }

  /**
   * Run fast client tests based on the input parameters.
   * Only RouterBasedStoreMetadata can be reused. Other StoreMetadata implementation cannot be used after close() is called.
   *
   * @param clientConfigBuilder config to build client
   * @param useDaVinciClientBasedMetadata true: use DaVinci based meta data, false: use Router based meta data
   * @param batchGet singleGet or batchGet
   * @throws Exception
   */
  private void runTest(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      boolean useDaVinciClientBasedMetadata,
      boolean batchGet,
      int batchGetKeySize,
      Consumer<MetricsRepository> statsValidation) throws Exception {
    // setting up storeMetaData for either DVC based or router based metadata
    StoreMetadata routerBasedStoreMetadata = null;
    if (useDaVinciClientBasedMetadata) {
      setupDaVinciClientForMetaStore();
      clientConfigBuilder.setDaVinciClientForMetaStore(daVinciClientForMetaStore);
    } else {
      routerBasedStoreMetadata = getRouterBasedStoreMetadata(clientConfigBuilder);
    }

    // clientConfigBuilder will be used for building multiple clients over this test flow,
    // so, always specify a new MetricsRepository to avoid conflicts.
    MetricsRepository metricsRepositoryForGenericClient = new MetricsRepository();
    clientConfigBuilder.setMetricsRepository(metricsRepositoryForGenericClient);
    // Test generic store client first
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = useDaVinciClientBasedMetadata
        ? ClientFactory.getAndStartGenericStoreClient(clientConfigBuilder.build())
        : ClientFactory.getAndStartGenericStoreClient(routerBasedStoreMetadata, clientConfigBuilder.build());

    // Construct a Vson store client
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    ClientConfig.ClientConfigBuilder clientConfigBuilderForVsonStore = clientConfigBuilder.clone().setVsonStore(true);
    AvroGenericStoreClient<String, Map> genericFastVsonClient = useDaVinciClientBasedMetadata
        ? ClientFactory.getAndStartGenericStoreClient(clientConfigBuilderForVsonStore.build())
        : ClientFactory
            .getAndStartGenericStoreClient(routerBasedStoreMetadata, clientConfigBuilderForVsonStore.build());

    try {
      if (batchGet) {
        // test batch get of size 2 (default)
        if (batchGetKeySize == 2) {
          for (int i = 0; i < recordCnt - 1; ++i) {
            String key1 = keyPrefix + i;
            String key2 = keyPrefix + (i + 1);
            Set<String> keys = new HashSet<>();
            keys.add(key1);
            keys.add(key2);
            Map<String, GenericRecord> resultMap = genericFastClient.batchGet(keys).get();
            assertEquals(resultMap.size(), 2);
            assertEquals((int) resultMap.get(key1).get(VALUE_FIELD_NAME), i);
            assertEquals((int) resultMap.get(key2).get(VALUE_FIELD_NAME), i + 1);

            // Test Vson client
            Map<String, Map> vsonResultMap = genericFastVsonClient.batchGet(keys).get();
            assertEquals(vsonResultMap.size(), 2);
            assertEquals((int) vsonResultMap.get(key1).get(VALUE_FIELD_NAME), i);
            assertEquals((int) vsonResultMap.get(key2).get(VALUE_FIELD_NAME), i + 1);
          }
        } else if (batchGetKeySize == recordCnt) {
          // test batch get of size recordCnt (configured)
          Set<String> keys = new HashSet<>();
          for (int i = 0; i < recordCnt; ++i) {
            String key = keyPrefix + i;
            keys.add(key);
          }
          Map<String, GenericRecord> resultMap = genericFastClient.batchGet(keys).get();
          assertEquals(resultMap.size(), recordCnt);

          for (int i = 0; i < recordCnt; ++i) {
            String key = keyPrefix + i;
            assertEquals((int) resultMap.get(key).get(VALUE_FIELD_NAME), i);
          }

          // vson
          Map<String, Map> vsonResultMap = genericFastVsonClient.batchGet(keys).get();
          assertEquals(vsonResultMap.size(), recordCnt);

          for (int i = 0; i < recordCnt; ++i) {
            String key = keyPrefix + i;
            assertEquals((int) vsonResultMap.get(key).get(VALUE_FIELD_NAME), i);
          }
        } else {
          throw new VeniceException("unsupported batchGetKeySize: " + batchGetKeySize);
        }
      } else {
        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          GenericRecord value = genericFastClient.get(key).get();
          assertEquals((int) value.get(VALUE_FIELD_NAME), i);

          // Test Vson client
          Object vsonResult = genericFastVsonClient.get(key).get();
          assertTrue(vsonResult instanceof Map, "VsonClient should return Map, but got" + vsonResult.getClass());
          Map vsonValue = (Map) vsonResult;
          assertEquals((int) vsonValue.get(VALUE_FIELD_NAME), i);
        }
      }
      statsValidation.accept(metricsRepositoryForGenericClient);
    } finally {
      if (genericFastClient != null) {
        genericFastClient.close();
      }
      if (genericFastVsonClient != null) {
        genericFastVsonClient.close();
      }
    }

    // Test specific store client
    MetricsRepository metricsRepositoryForSpecificClient = new MetricsRepository();
    ClientConfig.ClientConfigBuilder specificClientConfigBuilder = clientConfigBuilder.clone()
        .setSpecificValueClass(TestValueSchema.class)
        .setMetricsRepository(metricsRepositoryForSpecificClient); // To avoid metric registration conflict.
    AvroSpecificStoreClient<String, TestValueSchema> specificFastClient = useDaVinciClientBasedMetadata
        ? ClientFactory.getAndStartSpecificStoreClient(specificClientConfigBuilder.build())
        : ClientFactory.getAndStartSpecificStoreClient(routerBasedStoreMetadata, specificClientConfigBuilder.build());

    try {
      if (batchGet) {
        // test batch get of size 2 (default)
        if (batchGetKeySize == 2) {
          for (int i = 0; i < recordCnt - 1; ++i) {
            String key1 = keyPrefix + i;
            String key2 = keyPrefix + (i + 1);
            Set<String> keys = new HashSet<>();
            keys.add(key1);
            keys.add(key2);
            Map<String, TestValueSchema> resultMap = specificFastClient.batchGet(keys).get();
            assertEquals(resultMap.size(), 2);
            assertEquals(resultMap.get(key1).int_field, i);
            assertEquals(resultMap.get(key2).int_field, i + 1);
          }
        } else if (batchGetKeySize == recordCnt) {
          // test batch get of size recordCnt (configured)
          Set<String> keys = new HashSet<>();
          for (int i = 0; i < recordCnt; ++i) {
            String key = keyPrefix + i;
            keys.add(key);
          }
          Map<String, TestValueSchema> resultMap = specificFastClient.batchGet(keys).get();
          assertEquals(resultMap.size(), recordCnt);

          for (int i = 0; i < recordCnt; ++i) {
            String key = keyPrefix + i;
            assertEquals(resultMap.get(key).int_field, i);
          }
        } else {
          throw new VeniceException("unsupported batchGetKeySize: " + batchGetKeySize);
        }
      } else {
        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          TestValueSchema value = specificFastClient.get(key).get();
          assertEquals(value.int_field, i);
        }
      }
      statsValidation.accept(metricsRepositoryForSpecificClient);
    } finally {
      if (specificFastClient != null) {
        specificFastClient.close();
      }
      if (useDaVinciClientBasedMetadata)
        cleanupDaVinciClientForMetaStore();
    }
  }

  // Helper for runTest()
  void setupDaVinciClientForMetaStore() {
    daVinciClientFactory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        daVinciBackendConfig);
    daVinciClientForMetaStore = daVinciClientFactory.getAndStartSpecificAvroClient(
        VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName),
        new DaVinciConfig(),
        StoreMetaValue.class);
  }

  // Helper for runTest()
  void cleanupDaVinciClientForMetaStore() {
    if (daVinciClientForMetaStore != null) {
      daVinciClientForMetaStore.close();
    }
    if (daVinciClientFactory != null) {
      daVinciClientFactory.close();
    }
  }

  private RouterBasedStoreMetadata getRouterBasedStoreMetadata(ClientConfig.ClientConfigBuilder clientConfigBuilder) {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    ClientConfig clientConfig = clientConfigBuilder.build();
    VeniceRouterWrapper routerWrapper = veniceCluster.getRandomVeniceRouter();
    return new RouterBasedStoreMetadata(
        routerWrapper.getMetaDataRepository(),
        routerWrapper.getSchemaRepository(),
        routerWrapper.getRoutingDataRepository(),
        storeName,
        clientConfig);
  }

  private AvroGenericStoreClient<String, GenericRecord> getGenericThinClient() {
    return com.linkedin.venice.client.store.ClientFactory.getAndStartGenericAvroClient(
        com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterSslURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
  }

  private AvroSpecificStoreClient<String, TestValueSchema> getSpecificThinClient() {
    return com.linkedin.venice.client.store.ClientFactory.getAndStartSpecificAvroClient(
        com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig(storeName)
            .setSpecificValueClass(TestValueSchema.class)
            .setVeniceURL(veniceCluster.getRandomRouterSslURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
  }

  @Test(dataProvider = "FastClient-Four-Boolean", timeOut = TIME_OUT)
  public void testFastClientGet(
      boolean useDaVinciClientBasedMetadata,
      boolean multiGet,
      boolean dualRead,
      boolean speculativeQueryEnabled,
      int batchGetKeySize) throws Exception {
    if (multiGet == false && batchGetKeySize != 2) {
      // redundant case as batchGetKeySize doesn't apply for single gets, so run only once
      return;
    }
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(speculativeQueryEnabled)
            .setDualReadEnabled(dualRead)
            // default maxAllowedKeyCntInBatchGetReq is 2. configuring it to test different cases.
            .setMaxAllowedKeyCntInBatchGetReq(recordCnt);

    // dualRead also needs thinClient
    AvroGenericStoreClient<String, GenericRecord> genericThinClient = null;
    AvroSpecificStoreClient<String, TestValueSchema> specificThinClient = null;
    if (dualRead) {
      genericThinClient = getGenericThinClient();
      clientConfigBuilder.setGenericThinClient(genericThinClient);
      specificThinClient = getSpecificThinClient();
      clientConfigBuilder.setSpecificThinClient(specificThinClient);
    }

    runTest(clientConfigBuilder, useDaVinciClientBasedMetadata, multiGet, batchGetKeySize);

    if (genericThinClient != null) {
      genericThinClient.close();
    }
    if (specificThinClient != null) {
      specificThinClient.close();
    }
  }

  @Test
  public void testFastClientSingleGetLongTailRetry() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(false)
            .setMaxAllowedKeyCntInBatchGetReq(recordCnt)
            .setLongTailRetryEnabledForSingleGet(true)
            .setLongTailRetryThresholdForSingletGetInMicroSeconds(10); // Try to trigger long-tail retry as much as
                                                                       // possible.

    runTest(clientConfigBuilder, true, false, 2, metricsRepository -> {
      // Validate long-tail retry related metrics
      metricsRepository.metrics().forEach((mName, metric) -> {
        if (mName.contains("--long_tail_retry_request.OccurrenceRate")) {
          assertTrue(metric.value() > 0, "Long tail retry for single-get should be triggered");
        }
      });
    });
  }
}
