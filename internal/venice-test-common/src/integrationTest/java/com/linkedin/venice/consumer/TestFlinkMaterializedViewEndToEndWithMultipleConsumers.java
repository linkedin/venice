package com.linkedin.venice.consumer;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceAfterImageConsumerImpl;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestFlinkMaterializedViewEndToEndWithMultipleConsumers {
  private static final Logger LOGGER =
      LogManager.getLogger(TestFlinkMaterializedViewEndToEndWithMultipleConsumers.class);
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final int NUMBER_OF_REGIONS = 2;

  // Centralized test constants
  private static final String TEST_VIEW_NAME = "MaterializedViewTest";
  private static final int INITIAL_BATCH_RECORD_COUNT = 100;
  private static final int SECOND_BATCH_RECORD_COUNT = 200;
  private static final int STORE_PARTITION_COUNT = 3;
  private static final int VIEW_PARTITION_COUNT = 1;

  private final List<AutoCloseable> testCloseables = new ArrayList<>();
  private final List<String> testStoresToDelete = new ArrayList<>();

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private ControllerClient parentControllerClient;
  private ControllerClient childControllerClientRegion0;
  private D2Client d2Client;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws IOException {
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_REGIONS)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(2)
            .forkServer(false)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    clusterName = CLUSTER_NAMES[0];
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);
    parentControllerClient = new ControllerClient(clusterName, parentControllers.get(0).getControllerUrl());
    childControllerClientRegion0 =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    d2Client = new D2ClientBuilder()
        .setZkHosts(multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    D2ClientUtils.shutdownClient(d2Client);
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(childControllerClientRegion0);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanupAfterTest() {
    for (int i = testCloseables.size() - 1; i >= 0; i--) {
      try {
        testCloseables.get(i).close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close resource during test cleanup", e);
      }
    }
    testCloseables.clear();

    for (String storeName: testStoresToDelete) {
      try {
        parentControllerClient.disableAndDeleteStore(storeName);
      } catch (Exception e) {
        LOGGER.warn("Failed to delete store {} during test cleanup", storeName, e);
      }
    }
    testStoresToDelete.clear();
  }

  /**
   * Test batch-only store with materialized view using stateless CDC consumer.
   * This test does the following:
   * 1. Create a batch-only store with materialized view
   * 2. Run the initial push - v1 (100 keys)
   * 3. Consume from version topic v1, should have 100 records
   * 4. Consume from view topic v1 using stateless CDC client, should have 0 records
   * 5. Run the second push - v2 (200 keys)
   * 6. Consume from version topic v2, should have 200 records
   * 7. Consume from view topic v2 using stateless CDC client, should have 0 records
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyStoreWithStatelessCDCClient() throws IOException, ExecutionException, InterruptedException {
    String storeName = Utils.getUniqueString("batchStore");
    testStoresToDelete.add(storeName);
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        multiRegionMultiClusterWrapper.getPubSubClientProperties());
    setupBatchOnlyStoreWithMaterializedView(storeName, props, keySchemaStr, valueSchemaStr);
    waitForMetaSystemStoreToBeReady(storeName);
    // Run the initial push - v1
    IntegrationTestPushUtils.runVPJ(props);
    waitForCurrentVersion(storeName, 1);

    // Consume from version topic
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(getGlobalChangelogClientConfig(inputDirPath), metricsRepository);
    VeniceChangelogConsumer<Integer, Utf8> versionTopicChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "1");
    versionTopicChangelogConsumer.subscribeAll().get();
    waitForMessageCount(versionTopicChangelogConsumer, INITIAL_BATCH_RECORD_COUNT);
    versionTopicChangelogConsumer.close();

    // Consume from view topic
    ChangelogClientConfig viewChangeLogClientConfig =
        getGlobalChangelogClientConfig(inputDirPath).setViewName(TEST_VIEW_NAME);
    VeniceChangelogConsumerClientFactory veniceViewChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(viewChangeLogClientConfig, metricsRepository);

    try (VeniceChangelogConsumer<Integer, Utf8> viewTopicConsumer =
        veniceViewChangelogConsumerClientFactory.getChangelogConsumer(storeName)) {
      Assert.assertTrue(viewTopicConsumer instanceof VeniceAfterImageConsumerImpl);
      viewTopicConsumer.subscribeAll().get();
      assertEquals(viewTopicConsumer.poll(1000).size(), 0); // No data in view topic for batch-only push
    }
  }

  /**
   * Test batch-only store with materialized view using DVC consumer.
   * This test does the following:
   * 1. Create a batch-only store with materialized view
   * 2. Run the initial push - v1 (100 keys)
   * 3. Consume from version topic v1, should have 100 records
   * 4. Consume from view topic v1 using DVC consumer, all keys should return null values
   * 5. Run the second push - v2 (200 keys)
   * 6. Consume from version topic v2, should have 200 records
   * 7. Consume from view topic v2 using DVC consumer, all keys should return null values
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyStoreWithDVCConsumer() throws IOException, ExecutionException, InterruptedException {
    String storeName = Utils.getUniqueString("batchStore");
    testStoresToDelete.add(storeName);
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        multiRegionMultiClusterWrapper.getPubSubClientProperties());
    setupBatchOnlyStoreWithMaterializedView(storeName, props, keySchemaStr, valueSchemaStr);
    waitForMetaSystemStoreToBeReady(storeName);
    // Run the initial push - v1
    IntegrationTestPushUtils.runVPJ(props);
    waitForCurrentVersion(storeName, 1);

    // Consume from version topic
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(getGlobalChangelogClientConfig(inputDirPath), metricsRepository);

    VeniceChangelogConsumer<Integer, Utf8> versionTopicChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "1");
    versionTopicChangelogConsumer.subscribeAll().get();
    waitForMessageCount(versionTopicChangelogConsumer, INITIAL_BATCH_RECORD_COUNT);
    versionTopicChangelogConsumer.unsubscribeAll();

    // Broadcast end of push to all regions
    broadcastEndOfPushToAllRegions(storeName);

    // Consume from view topic using DVC
    MetricsRepository dvcMetricsRepo = new MetricsRepository();

    // Verify from source fabric
    D2Client D2ClientForSourceFabric = getD2Client(true);
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        D2ClientForSourceFabric,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        dvcMetricsRepo,
        createBackendConfig(),
        multiRegionMultiClusterWrapper)) {
      DaVinciClient<String, Object> viewClient =
          factory.getAndStartGenericAvroClient(storeName, TEST_VIEW_NAME, new DaVinciConfig());
      viewClient.subscribeAll().get();
      assertVersionGauge(dvcMetricsRepo, storeName, 1);
      assertAllKeysNull(viewClient, INITIAL_BATCH_RECORD_COUNT);
    } finally {
      D2ClientUtils.shutdownClient(D2ClientForSourceFabric);
    }

    // Verify from remote fabric
    D2Client D2ClientForRemoteFabric = getD2Client(false);
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        D2ClientForRemoteFabric,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        dvcMetricsRepo,
        createBackendConfig(),
        multiRegionMultiClusterWrapper)) {
      // Verify that current version is still 1
      DaVinciClient<String, Object> viewClient =
          factory.getAndStartGenericAvroClient(storeName, TEST_VIEW_NAME, new DaVinciConfig());
      viewClient.subscribeAll().get();
      assertVersionGauge(dvcMetricsRepo, storeName, 1);

      // Run a second push - v2
      File newPushInputDir = getTempDataDirectory();
      TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(newPushInputDir, SECOND_BATCH_RECORD_COUNT);
      String newPushInputDirPath = "file:" + newPushInputDir.getAbsolutePath();
      Properties newPushProps = TestWriteUtils.defaultVPJProps(
          parentControllers.get(0).getControllerUrl(),
          newPushInputDirPath,
          storeName,
          multiRegionMultiClusterWrapper.getPubSubClientProperties());
      IntegrationTestPushUtils.runVPJ(newPushProps);

      // Wait for version to be switched
      waitForCurrentVersion(storeName, 2);

      // Consume from version topic v2
      VeniceChangelogConsumer<Integer, Utf8> versionTopicV2ChangelogConsumer =
          veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, "2");
      versionTopicV2ChangelogConsumer.subscribeAll().get();
      waitForMessageCount(versionTopicV2ChangelogConsumer, SECOND_BATCH_RECORD_COUNT);
      versionTopicV2ChangelogConsumer.close();

      // Verify DVC view client still sees version 1 (no VS) and all keys are null
      assertVersionGauge(dvcMetricsRepo, storeName, 1);
      assertAllKeysNull(viewClient, INITIAL_BATCH_RECORD_COUNT);
    } finally {
      D2ClientUtils.shutdownClient(D2ClientForRemoteFabric);
    }
  }

  /**
   * Test hybrid store with materialized view using stateless CDC consumer.
   * This test does the following:
   * 1. Create a hybrid store with materialized view
   * 2. Run the initial push - v1 (100 keys)
   * 3. Consume from version topic v1, should have 100 records
   * 4. Consume from view topic v1 using stateless CDC consumer, no records
   * 5. Send streaming records (100 more keys)
   * 6. Consume from version topic v1, should have 200 records (100 batch + 100 streaming)
   * 7. Consume from view topic v2 using stateless CDC consumer, no records
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testHybridStoreWithStatelessCDCClient() throws IOException, ExecutionException, InterruptedException {
    String storeName = Utils.getUniqueString("hybridStore");
    testStoresToDelete.add(storeName);
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        multiRegionMultiClusterWrapper.getPubSubClientProperties());
    setupHybridStoreWithMaterializedView(storeName, props, keySchemaStr, valueSchemaStr);
    waitForMetaSystemStoreToBeReady(storeName);

    // Run the initial push - v1
    IntegrationTestPushUtils.runVPJ(props);
    waitForCurrentVersion(storeName, 1);

    // Consume from version topic
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(getGlobalChangelogClientConfig(inputDirPath), metricsRepository);

    VeniceChangelogConsumer<Integer, Utf8> versionTopicChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName);
    versionTopicChangelogConsumer.subscribeAll().get();
    waitForMessageCount(versionTopicChangelogConsumer, INITIAL_BATCH_RECORD_COUNT);
    versionTopicChangelogConsumer.unsubscribeAll();

    // Consume from view topic
    ChangelogClientConfig viewChangeLogClientConfig =
        getGlobalChangelogClientConfig(inputDirPath).setViewName(TEST_VIEW_NAME);
    VeniceChangelogConsumerClientFactory veniceViewChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(viewChangeLogClientConfig, metricsRepository);

    try (VeniceChangelogConsumer<Integer, Utf8> viewTopicConsumer =
        veniceViewChangelogConsumerClientFactory.getChangelogConsumer(storeName)) {
      Assert.assertTrue(viewTopicConsumer instanceof VeniceAfterImageConsumerImpl);
      viewTopicConsumer.subscribeAll().get();
      assertEquals(viewTopicConsumer.poll(1000).size(), 0); // No data in view topic for batch push portion
      viewTopicConsumer.unsubscribeAll();
    }

    // Send streaming records
    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(clusterName);
    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      for (int i = INITIAL_BATCH_RECORD_COUNT + 1; i <= INITIAL_BATCH_RECORD_COUNT * 2; i++) {
        sendStreamingRecord(veniceProducer, storeName, i);
      }
    }

    // Consume from version topic again
    versionTopicChangelogConsumer.subscribeAll().get();
    waitForMessageCount(versionTopicChangelogConsumer, INITIAL_BATCH_RECORD_COUNT * 2);
    versionTopicChangelogConsumer.close();

    try (VeniceChangelogConsumer<Integer, Utf8> viewTopicConsumer =
        veniceViewChangelogConsumerClientFactory.getChangelogConsumer(storeName)) {
      viewTopicConsumer.subscribeAll().get();
      assertEquals(viewTopicConsumer.poll(1000).size(), 0); // No data in view topic for streaming portion
    }
  }

  // Helpers

  /**
   * Wait for the meta system store to be ready for the given store.
   * This ensures that the thin client metadata repository can successfully fetch store metadata.
   * The method waits for:
   * 1. The meta system store push to complete
   * 2. The meta store to be queryable via the router (verifies router has healthy routes)
   */
  private void waitForMetaSystemStoreToBeReady(String storeName) {
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(metaSystemStoreName, 1),
        childControllerClientRegion0,
        90,
        TimeUnit.SECONDS);
    clusterWrapper.refreshAllRouterMetaData();
    String routerUrl = clusterWrapper.getRandomRouterURL();
    try (AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> metaStoreClient =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(metaSystemStoreName, StoreMetaValue.class)
                .setVeniceURL(routerUrl))) {
      StoreMetaKey storeClusterConfigKey =
          MetaStoreDataType.STORE_CLUSTER_CONFIG.getStoreMetaKey(Collections.singletonMap("KEY_STORE_NAME", storeName));
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, false, true, () -> {
        StoreMetaValue value = metaStoreClient.get(storeClusterConfigKey).get(30, TimeUnit.SECONDS);
        Assert.assertNotNull(value, "Meta store should return non-null value for STORE_CLUSTER_CONFIG");
        Assert.assertNotNull(value.storeClusterConfig, "storeClusterConfig should not be null");
      });
    }
  }

  private void waitForMessageCount(VeniceChangelogConsumer<Integer, Utf8> consumer, int expectedCount) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> messages = consumer.poll(1000);
      assertEquals(messages.size(), expectedCount);
    });
  }

  private void assertVersionGauge(MetricsRepository repo, String storeName, int version) {
    assertEquals(
        getMetric(repo, "current_version_number.Gauge", VeniceView.getViewStoreName(storeName, TEST_VIEW_NAME)),
        (double) version);
  }

  private void assertAllKeysNull(DaVinciClient<String, Object> client, int count)
      throws ExecutionException, InterruptedException {
    for (int i = 1; i <= count; i++) {
      assertNull(client.get(Integer.toString(i)).get());
    }
  }

  private double getMetric(MetricsRepository metricsRepository, String metricName, String storeName) {
    Metric metric = metricsRepository.getMetric("." + storeName + "--" + metricName);
    assertNotNull(metric, "Expected metric " + metricName + " not found.");
    return metric.value();
  }

  private void setupBatchOnlyStoreWithMaterializedView(
      String storeName,
      Properties props,
      String keySchemaStr,
      String valueSchemaStr) throws IOException {
    UpdateStoreQueryParams storeParms = baseStoreParams().setFlinkVeniceViewsEnabled(true);
    createStoreAndView(storeName, props, keySchemaStr, valueSchemaStr, storeParms);
  }

  private void setupHybridStoreWithMaterializedView(
      String storeName,
      Properties props,
      String keySchemaStr,
      String valueSchemaStr) throws IOException {
    UpdateStoreQueryParams storeParms =
        baseStoreParams().setFlinkVeniceViewsEnabled(true).setHybridRewindSeconds(10L).setHybridOffsetLagThreshold(2L);
    createStoreAndView(storeName, props, keySchemaStr, valueSchemaStr, storeParms);
  }

  private UpdateStoreQueryParams baseStoreParams() {
    return new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(false)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setNativeReplicationSourceFabric(childDatacenters.get(0).getRegionName())
        .setPartitionCount(STORE_PARTITION_COUNT);
  }

  private void createStoreAndView(
      String storeName,
      Properties props,
      String keySchemaStr,
      String valueSchemaStr,
      UpdateStoreQueryParams storeParms) throws IOException {
    try (ControllerClient controllerClient =
        IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms)) {
      MaterializedViewParameters.Builder viewParamBuilder =
          new MaterializedViewParameters.Builder(TEST_VIEW_NAME).setPartitionCount(VIEW_PARTITION_COUNT);
      UpdateStoreQueryParams updateViewParam = new UpdateStoreQueryParams().setViewName(TEST_VIEW_NAME)
          .setViewClassName(MaterializedView.class.getCanonicalName())
          .setViewClassParams(viewParamBuilder.build());
      controllerClient
          .retryableRequest(5, controllerClient1 -> controllerClient.updateStore(storeName, updateViewParam));
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        Map<String, ViewConfig> viewConfigMap = controllerClient.getStore(storeName).getStore().getViewConfigs();
        assertEquals(viewConfigMap.size(), 1);
        assertEquals(viewConfigMap.get(TEST_VIEW_NAME).getViewClassName(), MaterializedView.class.getCanonicalName());
      });
    }
  }

  private ChangelogClientConfig getGlobalChangelogClientConfig(String inputDirPath) {
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    return new ChangelogClientConfig().setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(localZkServer.getAddress())
        .setControllerRequestRetryCount(3)
        .setVersionSwapDetectionIntervalTimeInSeconds(3)
        .setD2Client(d2Client)
        .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
  }

  private void broadcastEndOfPushToAllRegions(String storeName) {
    String viewTopicName = getMaterializedViewTopicName(storeName, 1, TEST_VIEW_NAME);
    for (int i = 0; i < NUMBER_OF_REGIONS; i++) {
      PubSubBrokerWrapper pubSubBrokerWrapper =
          multiRegionMultiClusterWrapper.getChildRegions().get(i).getPubSubBrokerWrapper();
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
          pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
      VeniceWriterFactory vwFactory =
          IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory);
      VeniceWriterOptions.Builder vwOptionsBuilder =
          new VeniceWriterOptions.Builder(viewTopicName).setUseKafkaKeySerializer(true);
      try (VeniceWriter veniceWriter = vwFactory.createVeniceWriter(vwOptionsBuilder.build())) {
        veniceWriter.broadcastEndOfPush(Collections.emptyMap());
      }
    }
  }

  private VeniceProperties createBackendConfig() {
    return new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .build();
  }

  private D2Client getD2Client(boolean isSourceFabric) {
    int regionIndex = isSourceFabric ? 0 : 1;
    return D2TestUtils.getAndStartD2Client(
        multiRegionMultiClusterWrapper.getChildRegions().get(regionIndex).getZkServerWrapper().getAddress());
  }

  private void waitForCurrentVersion(String storeName, int expectedVersion) {
    TestUtils.waitForNonDeterministicAssertion(
        90,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(
            childControllerClientRegion0.getStore(storeName).getStore().getCurrentVersion(),
            expectedVersion));
  }

  private String getMaterializedViewTopicName(String storeName, int version, String viewName) {
    return Version.composeKafkaTopic(storeName, version) + MaterializedView.VIEW_NAME_SEPARATOR + viewName
        + MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX;
  }
}
