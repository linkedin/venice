package com.linkedin.venice.fastclient.utils;

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
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

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
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.schema.TestValueSchema;
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
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;


/**
 * TODO: Add desc
 */

public abstract class AbstractClientEndToEndSetup {
  protected VeniceClusterWrapper veniceCluster;
  protected String storeVersionName;
  protected int valueSchemaId;
  protected String storeName;

  protected VeniceKafkaSerializer keySerializer;
  protected VeniceKafkaSerializer valueSerializer;
  private VeniceWriter<Object, Object, Object> veniceWriter;
  protected Client r2Client;
  private VeniceProperties daVinciBackendConfig;
  private D2Client d2Client;

  /**
   * daVinci based metadata
   */
  // Client factory
  CachingDaVinciClientFactory daVinciClientFactory = null;
  // daVinciClient for the daVinciClient based metastore
  protected DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore = null;

  /**
   * router based metadata
   */
  StoreMetadata routerBasedStoreMetadata = null;

  protected ClientConfig clientConfig;

  protected static final long TIME_OUT = 60 * Time.MS_PER_SECOND;
  protected static final String KEY_SCHEMA_STR = "\"string\"";
  protected static final String VALUE_FIELD_NAME = "int_field";
  protected static final String VALUE_SCHEMA_STR = "{\n" + "\"type\": \"record\",\n"
      + "\"name\": \"TestValueSchema\",\n" + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\"}]\n" + "}";
  protected static final Schema VALUE_SCHEMA = new Schema.Parser().parse(VALUE_SCHEMA_STR);

  protected final String keyPrefix = "key_";
  protected final int recordCnt = 100;

  /**
   * two sizes: default 2 (initial FC batch get implementation size) and max of recordCnt
   *
   * TODO
   * 1: figure out where this count is checked and limited to a global or store based max value
   * 2: Current implementation of batchGet() using single get() in a loop quickly fails due
   * to routingPendingRequestCounterInstanceBlockThreshold set to 50 by default and the loop
   * is faster than the counter decrement following a successful get, so some get() calls will
   * not be sent due to blocked instances. Setting this variable to be 100 from the tests for now.
   * This needs to be discussed further.
    */
  public final Object[] BATCH_GET_KEY_SIZE = { 2, /*recordCnt*/ };

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

  @DataProvider(name = "useDaVinciClientBasedMetadata")
  public static Object[][] useDaVinciClientBasedMetadata() {
    return new Object[][] { { false }, { true } };
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    Properties props = new Properties();
    props.put(SERVER_HTTP2_INBOUND_ENABLED, "true");
    veniceCluster = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 100, true, false, props);

    r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_HTTPCLIENT5);

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

  protected AvroGenericStoreClient<String, Object> getGenericFastVsonClient(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      MetricsRepository metricsRepository,
      boolean useDaVinciClientBasedMetadata) throws IOException {
    clientConfigBuilder.setVsonStore(true);
    setupStoreMetadata(clientConfigBuilder, useDaVinciClientBasedMetadata);

    // clientConfigBuilder will be used for building multiple clients over this test flow,
    // so, always specify a new MetricsRepository to avoid conflicts.
    clientConfigBuilder.setMetricsRepository(metricsRepository);

    clientConfig = clientConfigBuilder.build();

    return useDaVinciClientBasedMetadata
        ? ClientFactory.getAndStartGenericStoreClient(clientConfig)
        : ClientFactory.getAndStartGenericStoreClient(routerBasedStoreMetadata, clientConfig);
  }

  protected AvroGenericStoreClient<String, GenericRecord> getGenericFastClient(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      MetricsRepository metricsRepository,
      boolean useDaVinciClientBasedMetadata) throws IOException {
    setupStoreMetadata(clientConfigBuilder, useDaVinciClientBasedMetadata);

    // clientConfigBuilder will be used for building multiple clients over this test flow,
    // so, always specify a new MetricsRepository to avoid conflicts.
    clientConfigBuilder.setMetricsRepository(metricsRepository);

    clientConfig = clientConfigBuilder.build();

    return useDaVinciClientBasedMetadata
        ? ClientFactory.getAndStartGenericStoreClient(clientConfig)
        : ClientFactory.getAndStartGenericStoreClient(routerBasedStoreMetadata, clientConfig);
  }

  protected AvroSpecificStoreClient<String, TestValueSchema> getSpecificFastClient(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      MetricsRepository metricsRepository,
      boolean useDaVinciClientBasedMetadata,
      Class specificValueClass) throws IOException {
    setupStoreMetadata(clientConfigBuilder, useDaVinciClientBasedMetadata);

    // clientConfigBuilder will be used for building multiple clients over this test flow,
    // so, always specify a new MetricsRepository to avoid conflicts.
    clientConfigBuilder.setMetricsRepository(metricsRepository);
    clientConfigBuilder.setSpecificValueClass(specificValueClass);

    clientConfig = clientConfigBuilder.build();

    return useDaVinciClientBasedMetadata
        ? ClientFactory.getAndStartSpecificStoreClient(clientConfig)
        : ClientFactory.getAndStartSpecificStoreClient(routerBasedStoreMetadata, clientConfig);
  }

  protected void setupStoreMetadata(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      boolean useDaVinciClientBasedMetadata) throws IOException {
    if (useDaVinciClientBasedMetadata) {
      setupDaVinciClientForMetaStore();
      clientConfigBuilder.setDaVinciClientForMetaStore(daVinciClientForMetaStore);
    } else {
      setupRouterBasedStoreMetadata(clientConfigBuilder);
    }
  }

  // Helper for runTest()
  private void setupDaVinciClientForMetaStore() {
    cleanupDaVinciClientForMetaStore();
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
  /**
   * Note that both daVinciClientBasedStoreMetadata and routerBasedStoreMetaData
   * will be closed when the respective client closes. The below function
   * needs to clean up the daVinciClient and its client factory alone.
   *
   * TODO: Explore to see if we can reuse these for all the tests rather than cleaning it up everytime.
   * */
  protected void cleanupDaVinciClientForMetaStore() {
    if (daVinciClientForMetaStore != null) {
      daVinciClientForMetaStore.close();
      daVinciClientForMetaStore = null;
    }
    if (daVinciClientFactory != null) {
      daVinciClientFactory.close();
      daVinciClientFactory = null;
    }
  }

  private void setupRouterBasedStoreMetadata(ClientConfig.ClientConfigBuilder clientConfigBuilder) throws IOException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    ClientConfig clientConfig = clientConfigBuilder.build();
    VeniceRouterWrapper routerWrapper = veniceCluster.getRandomVeniceRouter();
    routerBasedStoreMetadata = new RouterBasedStoreMetadata(
        routerWrapper.getMetaDataRepository(),
        routerWrapper.getSchemaRepository(),
        routerWrapper.getRoutingDataRepository(),
        storeName,
        clientConfig);
  }

  protected AvroGenericStoreClient<String, GenericRecord> getGenericThinClient() {
    return com.linkedin.venice.client.store.ClientFactory.getAndStartGenericAvroClient(
        com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterSslURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
  }

  protected AvroSpecificStoreClient<String, TestValueSchema> getSpecificThinClient() {
    return com.linkedin.venice.client.store.ClientFactory.getAndStartSpecificAvroClient(
        com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig(storeName)
            .setSpecificValueClass(TestValueSchema.class)
            .setVeniceURL(veniceCluster.getRandomRouterSslURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    cleanupDaVinciClientForMetaStore();

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
