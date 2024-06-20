package com.linkedin.venice.fastclient.utils;

import static com.linkedin.venice.ConfigKeys.CLIENT_USE_DA_VINCI_BASED_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.fastclient.utils.ClientTestUtils.FASTCLIENT_HTTP_VARIANTS;
import static com.linkedin.venice.fastclient.utils.ClientTestUtils.REQUEST_TYPES_SMALL;
import static com.linkedin.venice.fastclient.utils.ClientTestUtils.STORE_METADATA_FETCH_MODES;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_PARTITION_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_VERSION_NUMBER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
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
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.GrpcClientConfig;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.schema.TestValueSchema;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
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
  protected String dataPath;

  protected VeniceKafkaSerializer keySerializer;
  protected VeniceKafkaSerializer valueSerializer;
  private VeniceWriter<Object, Object, Object> veniceWriter;
  protected Client r2Client;
  protected D2Client d2Client;

  // da-vinci client for the da-vinci client based metadata
  private VeniceProperties daVinciBackendConfig;
  CachingDaVinciClientFactory daVinciClientFactory = null;
  protected DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore = null;

  // thin client for the thin client based metadata
  protected AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> thinClientForMetaStore = null;

  protected ClientConfig clientConfig;

  protected static final int TIME_OUT = 60 * Time.MS_PER_SECOND;
  protected static final String KEY_SCHEMA_STR = "\"string\"";
  protected static final String VALUE_FIELD_NAME = "int_field";
  protected static final String SECOND_VALUE_FIELD_NAME = "opt_int_field";
  protected static final String VALUE_SCHEMA_STR = "{\n" + "\"type\": \"record\",\n"
      + "\"name\": \"TestValueSchema\",\n" + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\"}]\n" + "}";
  protected static final Schema VALUE_SCHEMA = new Schema.Parser().parse(VALUE_SCHEMA_STR);
  protected static final String VALUE_SCHEMA_V2_STR = "{\n" + "\"type\": \"record\",\n"
      + "\"name\": \"TestValueSchema\",\n" + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\"},\n" + "{\"name\": \""
      + SECOND_VALUE_FIELD_NAME + "\", \"type\": [\"null\", \"int\"], \"default\": null}]\n" + "}";
  protected static final Schema VALUE_SCHEMA_V2 = new Schema.Parser().parse(VALUE_SCHEMA_V2_STR);

  protected static final String keyPrefix = "key_";
  protected static final int recordCnt = 100;

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
  protected static final ImmutableList<Object> BATCH_GET_KEY_SIZE = ImmutableList.of(2, recordCnt);

  @DataProvider(name = "FastClient-Test-Permutations")
  public Object[][] fastClientTestPermutations() {
    return DataProviderUtils.allPermutationGenerator((permutation) -> {
      boolean dualRead = (boolean) permutation[0];
      boolean speculativeQueryEnabled = (boolean) permutation[1];
      boolean retryEnabled = (boolean) permutation[3];
      int batchGetKeySize = (int) permutation[4];
      RequestType requestType = (RequestType) permutation[5];
      StoreMetadataFetchMode storeMetadataFetchMode = (StoreMetadataFetchMode) permutation[6];
      if (requestType != RequestType.MULTI_GET && requestType != RequestType.MULTI_GET_STREAMING) {
        if (batchGetKeySize != (int) BATCH_GET_KEY_SIZE.get(0)) {
          // these parameters are related only to batchGet, so just allowing 1 set
          // to avoid duplicate tests
          return false;
        }
      }
      if (storeMetadataFetchMode != StoreMetadataFetchMode.SERVER_BASED_METADATA) {
        if (retryEnabled || speculativeQueryEnabled) {
          return false;
        }
      }
      if (retryEnabled && speculativeQueryEnabled) {
        return false;
      }
      if (dualRead && (requestType == RequestType.COMPUTE || requestType == RequestType.COMPUTE_STREAMING)) {
        // Compute requests don't do dual reads
        return false;
      }
      return true;
    },
        DataProviderUtils.BOOLEAN, // dualRead
        DataProviderUtils.BOOLEAN, // speculativeQueryEnabled
        DataProviderUtils.BOOLEAN, // enableGrpc
        DataProviderUtils.BOOLEAN, // retryEnabled
        BATCH_GET_KEY_SIZE.toArray(), // batchGetKeySize
        REQUEST_TYPES_SMALL, // requestType
        STORE_METADATA_FETCH_MODES); // storeMetadataFetchMode
  }

  @DataProvider(name = "fastClientHTTPVariantsAndStoreMetadataFetchModes")
  public static Object[][] httpVariantsAndStoreMetadataFetchModes() {
    return DataProviderUtils.allPermutationGenerator(FASTCLIENT_HTTP_VARIANTS, STORE_METADATA_FETCH_MODES);
  }

  @DataProvider(name = "Boolean-And-StoreMetadataFetchModes")
  public static Object[][] booleanAndStoreMetadataFetchModes() {
    return DataProviderUtils.allPermutationGenerator(DataProviderUtils.BOOLEAN, STORE_METADATA_FETCH_MODES);
  }

  @DataProvider(name = "StoreMetadataFetchModes")
  public static Object[][] storeMetadataFetchModes() {
    return DataProviderUtils.allPermutationGenerator(STORE_METADATA_FETCH_MODES);
  }

  @DataProvider(name = "FastClient-Request-Types-Small")
  public static Object[][] fastClientRequestTypesSmall() {
    return DataProviderUtils.allPermutationGenerator(REQUEST_TYPES_SMALL);
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    Properties props = new Properties();
    props.put(SERVER_HTTP2_INBOUND_ENABLED, "true");
    props.put(SERVER_QUOTA_ENFORCEMENT_ENABLED, "true");
    VeniceClusterCreateOptions createOptions = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(2)
        .enableGrpc(true)
        .numberOfRouters(1)
        .replicationFactor(2)
        .partitionSize(100)
        .sslToStorageNodes(true)
        .sslToKafka(false)
        .extraProperties(props)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(createOptions);

    r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_HTTPCLIENT5);

    d2Client = D2TestUtils.getAndStartHttpsD2Client(veniceCluster.getZk().getAddress());

    dataPath = Paths.get(System.getProperty("java.io.tmpdir"), Utils.getUniqueString("venice-server-data"))
        .toAbsolutePath()
        .toString();

    prepareData();
    prepareMetaSystemStore();
    waitForRouterD2();
  }

  /**
   * <b>Note:</b> Classes that overrides this method need to ensure the store creation enables the storage
   * node read quota.
   */
  protected void prepareData() throws Exception {
    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(KEY_SCHEMA_STR, VALUE_SCHEMA_STR);
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    veniceCluster
        .useControllerClient(
            client -> assertFalse(
                client
                    .updateStore(
                        storeName,
                        new UpdateStoreQueryParams().setStorageNodeReadQuotaEnabled(true)
                            .setReadComputationEnabled(true))
                    .isError()));
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // TODO: Make serializers parameterized so we test them all.
    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STR);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STR);

    PubSubBrokerWrapper pubSubBrokerWrapper = veniceCluster.getPubSubBrokerWrapper();
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
    veniceWriter = IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory)
        .createVeniceWriter(
            new VeniceWriterOptions.Builder(storeVersionName).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializer)
                .build());
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
    });

    daVinciBackendConfig = new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_USE_DA_VINCI_BASED_SYSTEM_STORE_REPOSITORY, true)
        .put(DATA_BASE_PATH, dataPath)
        .build();

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
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          true,
          () -> assertNotNull(metaClient.get(replicaStatusKey).get()));
    }
  }

  private void waitForRouterD2() {
    AvroGenericStoreClient<String, GenericRecord> thinClient =
        com.linkedin.venice.client.store.ClientFactory.getAndStartGenericAvroClient(
            com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig(storeName)
                .setVeniceURL(veniceCluster.getRandomRouterSslURL())
                .setSslFactory(SslUtils.getVeniceLocalSslFactory())
                .setD2Client(d2Client)
                .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME));

    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        true,
        () -> assertNotNull(thinClient.get(keyPrefix + "0")));
  }

  protected AvroGenericStoreClient<String, Object> getGenericFastVsonClient(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      MetricsRepository metricsRepository,
      Optional<AvroGenericStoreClient> vsonThinClient,
      StoreMetadataFetchMode storeMetadataFetchMode) throws IOException {
    clientConfigBuilder.setVsonStore(true);
    setupStoreMetadata(clientConfigBuilder, storeMetadataFetchMode);

    // clientConfigBuilder will be used for building multiple clients over this test flow,
    // so, always specify a new MetricsRepository to avoid conflicts.
    clientConfigBuilder.setMetricsRepository(metricsRepository);

    // Need to switch to a VSON-based thin client for dual read support
    if (vsonThinClient.isPresent()) {
      clientConfigBuilder.setGenericThinClient(vsonThinClient.get());
    }

    clientConfig = clientConfigBuilder.build();

    return ClientFactory.getAndStartGenericStoreClient(clientConfig);
  }

  protected AvroGenericStoreClient<String, GenericRecord> getGenericFastClient(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      MetricsRepository metricsRepository,
      StoreMetadataFetchMode storeMetadataFetchMode) throws IOException {
    setupStoreMetadata(clientConfigBuilder, storeMetadataFetchMode);

    // clientConfigBuilder will be used for building multiple clients over this test flow,
    // so, always specify a new MetricsRepository to avoid conflicts.
    clientConfigBuilder.setMetricsRepository(metricsRepository);

    clientConfig = clientConfigBuilder.build();

    return ClientFactory.getAndStartGenericStoreClient(clientConfig);
  }

  protected AvroSpecificStoreClient<String, TestValueSchema> getSpecificFastClient(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      MetricsRepository metricsRepository,
      Class specificValueClass,
      StoreMetadataFetchMode storeMetadataFetchMode) throws IOException {
    setupStoreMetadata(clientConfigBuilder, storeMetadataFetchMode);

    // clientConfigBuilder will be used for building multiple clients over this test flow,
    // so, always specify a new MetricsRepository to avoid conflicts.
    clientConfigBuilder.setMetricsRepository(metricsRepository);
    clientConfigBuilder.setSpecificValueClass(specificValueClass);

    clientConfig = clientConfigBuilder.build();

    return ClientFactory.getAndStartSpecificStoreClient(clientConfig);
  }

  protected void setUpGrpcFastClient(ClientConfig.ClientConfigBuilder clientConfigBuilder) {
    GrpcClientConfig grpcClientConfig = new GrpcClientConfig.Builder().setR2Client(r2Client)
        .setSSLFactory(SslUtils.getVeniceLocalSslFactory())
        .setNettyServerToGrpcAddress(veniceCluster.getNettyServerToGrpcAddress())
        .build();

    clientConfigBuilder.setGrpcClientConfig(grpcClientConfig).setUseGrpc(true);
  }

  protected void setupStoreMetadata(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      StoreMetadataFetchMode storeMetadataFetchMode) throws IOException {
    clientConfigBuilder.setStoreMetadataFetchMode(storeMetadataFetchMode);
    switch (storeMetadataFetchMode) {
      case SERVER_BASED_METADATA:
        clientConfigBuilder.setD2Client(d2Client);
        clientConfigBuilder.setClusterDiscoveryD2Service(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME);
        clientConfigBuilder.setMetadataRefreshIntervalInSeconds(1);
        // Validate the metadata response schema forward compat support setup
        veniceCluster.useControllerClient(controllerClient -> {
          String schemaStoreName = AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getSystemStoreName();
          MultiSchemaResponse multiSchemaResponse = controllerClient.getAllValueSchema(schemaStoreName);
          assertFalse(multiSchemaResponse.isError());
          assertEquals(
              AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getCurrentProtocolVersion(),
              multiSchemaResponse.getSchemas().length);
        });
        break;
      case THIN_CLIENT_BASED_METADATA:
        setupThinClientBasedStoreMetadata();
        clientConfigBuilder.setThinClientForMetaStore(thinClientForMetaStore);
        break;
      case DA_VINCI_CLIENT_BASED_METADATA:
        setupDaVinciClientForMetaStore();
        clientConfigBuilder.setDaVinciClientForMetaStore(daVinciClientForMetaStore);
    }
  }

  private void setupThinClientBasedStoreMetadata() {
    if (thinClientForMetaStore == null) {
      thinClientForMetaStore = com.linkedin.venice.client.store.ClientFactory.getAndStartSpecificAvroClient(
          com.linkedin.venice.client.store.ClientConfig
              .defaultSpecificClientConfig(
                  VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName),
                  StoreMetaValue.class)
              .setVeniceURL(veniceCluster.getRandomRouterURL())
              .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
    }
  }

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
    Utils.closeQuietlyWithErrorLogged(daVinciClientForMetaStore);
    daVinciClientForMetaStore = null;

    Utils.closeQuietlyWithErrorLogged(daVinciClientFactory);
    daVinciClientFactory = null;
  }

  protected AvroGenericStoreClient<String, GenericRecord> getGenericThinClient(MetricsRepository metricsRepository) {
    return com.linkedin.venice.client.store.ClientFactory.getAndStartGenericAvroClient(
        com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterSslURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory())
            .setMetricsRepository(metricsRepository));
  }

  protected AvroGenericStoreClient<String, Object> getGenericVsonThinClient() {
    return com.linkedin.venice.client.store.ClientFactory.getAndStartGenericAvroClient(
        com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterSslURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory())
            .setVsonClient(true));
  }

  protected AvroSpecificStoreClient<String, TestValueSchema> getSpecificThinClient() {
    return com.linkedin.venice.client.store.ClientFactory.getAndStartSpecificAvroClient(
        com.linkedin.venice.client.store.ClientConfig.defaultGenericClientConfig(storeName)
            .setSpecificValueClass(TestValueSchema.class)
            .setVeniceURL(veniceCluster.getRandomRouterSslURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
  }

  protected void validateSingleGetMetrics(MetricsRepository metricsRepository, boolean retryEnabled) {
    validateMetrics(metricsRepository, RequestType.SINGLE_GET, 0, 0, retryEnabled);
  }

  protected void validateBatchGetMetrics(
      MetricsRepository metricsRepository,
      boolean streamingBatchGetApi,
      int expectedMultiKeySizeMetricsCount,
      int expectedMultiKeySizeSuccessMetricsCount,
      boolean retryEnabled) {
    validateMetrics(
        metricsRepository,
        streamingBatchGetApi ? RequestType.MULTI_GET_STREAMING : RequestType.MULTI_GET,
        expectedMultiKeySizeMetricsCount,
        expectedMultiKeySizeSuccessMetricsCount,
        retryEnabled);
  }

  protected void validateComputeMetrics(
      MetricsRepository metricsRepository,
      boolean streamingComputeApi,
      int expectedBatchGetKeySizeMetricsCount,
      int expectedBatchGetKeySizeSuccessMetricsCount,
      boolean retryEnabled) {
    validateMetrics(
        metricsRepository,
        streamingComputeApi ? RequestType.COMPUTE_STREAMING : RequestType.COMPUTE,
        expectedBatchGetKeySizeMetricsCount,
        expectedBatchGetKeySizeSuccessMetricsCount,
        retryEnabled);
  }

  private void validateMetrics(
      MetricsRepository metricsRepository,
      RequestType requestType,
      int expectedMultiKeySizeMetricsCount,
      int expectedMultiKeySizeSuccessMetricsCount,
      boolean retryEnabled) {
    final String metricPrefix = ClientTestUtils.getMetricPrefix(storeName, requestType);

    double keyCount = expectedMultiKeySizeMetricsCount;
    double successKeyCount = expectedMultiKeySizeSuccessMetricsCount;
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    // counters are incremented in an async manner, so adding non-deterministic wait
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertTrue(metrics.get(metricPrefix + "request.OccurrenceRate").value() > 0);
      assertTrue(metrics.get(metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
      assertTrue(metrics.get(metricPrefix + "healthy_request_latency.Avg").value() > 0);
      assertFalse(metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get(metricPrefix + "unhealthy_request_latency.Avg").value() > 0);
      assertTrue(
          metrics.get(metricPrefix + "request_key_count.Rate").value() > 0,
          "Respective request_key_count should have been incremented");
      assertTrue(
          metrics.get(metricPrefix + "request_key_count.Max").value() >= keyCount,
          "Respective request_key_count should have been incremented");
      assertTrue(
          metrics.get(metricPrefix + "success_request_key_count.Rate").value() > 0,
          "Respective success_request_key_count should have been incremented");
      assertTrue(
          metrics.get(metricPrefix + "success_request_key_count.Max").value() >= successKeyCount,
          "Respective success_request_key_count should have been incremented");

      Set<String> allMetricPrefixes = ClientTestUtils.getAllMetricPrefixes(storeName);
      Set<String> allIncorrectMetricPrefixes = new HashSet<>(allMetricPrefixes);
      allIncorrectMetricPrefixes.remove(metricPrefix);

      for (String incorrectMetricPrefix: allIncorrectMetricPrefixes) {
        // incorrect metric should not be incremented
        assertFalse(
            metrics.get(incorrectMetricPrefix + "request_key_count.Rate").value() > 0,
            "Incorrect request_key_count should not be incremented");
      }

      if (retryEnabled) {
        assertTrue(
            metrics.get(metricPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0,
            "Long tail retry should be triggered");
      } else {
        metrics.forEach((mName, metric) -> {
          if (mName.contains("long_tail_retry_request")) {
            assertTrue(metric.value() == 0, "Long tail retry should not be triggered");
          }
        });
      }
    });
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
