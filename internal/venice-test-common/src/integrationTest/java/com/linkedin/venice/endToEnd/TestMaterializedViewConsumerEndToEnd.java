package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.testng.Assert.assertEquals;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.StatefulVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.consumer.ChangelogConsumerTestUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.MaterializedView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestMaterializedViewConsumerEndToEnd extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final String DEFAULT_VIEW_NAME = "MaterializedViewTest";
  private String clusterName;

  @Override
  protected Properties getExtraServerProperties() {
    Properties serverProperties = new Properties();
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    return serverProperties;
  }

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    super.setUp();
    clusterName = multiRegionMultiClusterWrapper.getClusterNames()[0];
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyMaterializedViewStatefulCDCConsumer() throws Exception {
    testBatchOnlyMaterializedViewCDCConsumer(true);
  }

  /**
   * This test has comparable setup to {@link #testBatchOnlyMaterializedViewStatefulCDCConsumer}
   * but verifies DVC consumer instead of CDC consumer.
   * The main purpose is to verify DVC can consume from materialized view topic
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyMaterializedViewDVCConsumerWithOnePush() throws Exception {
    // Create a batch only store with materialized view and run batch push job with 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("batchStore");

    // Setup store and materialized view
    setupStoreAndMaterializedView(inputDirPath, storeName, DEFAULT_VIEW_NAME, recordSchema);

    // Verify DVC consumer can consume from materialized view topic and get all the records.
    testDVCConsumer(storeName, 100);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyNoViewWithStatelessCDCConsumer() throws Exception {
    testBatchOnlyNoViewWithCDCConsumer(false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyNoViewWithStatefulCDCConsumer() throws Exception {
    testBatchOnlyNoViewWithCDCConsumer(true);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyNoViewWithDVCConsumer() throws Exception {
    // Create a batch only store with no view and run batch push job with 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("batchStore");

    setupStoreWithNoView(inputDirPath, storeName, recordSchema);

    // Verify DVC consumer when there is no view.
    // Start a DVC client that's subscribed to partition 0, 1, 3 of the store. The DVC client should
    // contain all data records.
    D2Client d2Client = D2TestUtils
        .getAndStartD2Client(multiRegionMultiClusterWrapper.getChildRegions().get(1).getZkServerWrapper().getAddress());

    VeniceProperties backendConfig =
        new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
            .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
            .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
            .build();
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        backendConfig,
        multiRegionMultiClusterWrapper)) {
      DaVinciClient<String, Object> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
      Set<Integer> partitions = new HashSet<>();
      partitions.add(0);
      partitions.add(1);
      partitions.add(2);
      client.subscribe(partitions).get();
      for (int i = 1; i <= 100; i++) {
        assertEquals(client.get(Integer.toString(i)).get().toString(), DEFAULT_USER_DATA_VALUE_PREFIX + i);
      }
    } finally {
      D2ClientUtils.shutdownClient(d2Client);
    }
  }

  private void testBatchOnlyMaterializedViewCDCConsumer(boolean useStatefulConsumer) throws Exception {
    // Create a batch only store with materialized view and run batch push job with 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("batchStore");

    // Setup store and materialized view
    setupStoreAndMaterializedView(inputDirPath, storeName, DEFAULT_VIEW_NAME, recordSchema);

    // Setup D2Client and ChangelogClientConfig
    D2Client d2Client = D2TestUtils
        .getAndStartD2Client(multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper().getAddress());

    try {
      ChangelogClientConfig changelogClientConfig =
          createChangelogClientConfig(d2Client, inputDirPath, DEFAULT_VIEW_NAME);

      // Test CDC consumer
      testCDCConsumer(storeName, changelogClientConfig, useStatefulConsumer);
    } finally {
      D2ClientUtils.shutdownClient(d2Client);
    }
  }

  private void testBatchOnlyNoViewWithCDCConsumer(boolean isStatefulClient) throws Exception {
    // Create a batch only store with no view and run batch push job with 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("batchStore");

    setupStoreWithNoView(inputDirPath, storeName, recordSchema);

    // Setup D2Client and ChangelogClientConfig
    D2Client d2Client = D2TestUtils
        .getAndStartD2Client(multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper().getAddress());

    try {
      ChangelogClientConfig changelogClientConfig = createChangelogClientConfig(d2Client, inputDirPath, "");

      // Test CDC consumer
      testCDCConsumer(storeName, changelogClientConfig, isStatefulClient);
    } finally {
      D2ClientUtils.shutdownClient(d2Client);
    }
  }

  private void testDVCConsumer(String storeName, int recordCount) throws Exception {
    // Start a DVC client that's subscribed to partition 0 of the store's materialized view. The DVC client should
    // contain all data records.
    D2Client d2Client = D2TestUtils
        .getAndStartD2Client(multiRegionMultiClusterWrapper.getChildRegions().get(1).getZkServerWrapper().getAddress());

    VeniceProperties backendConfig =
        new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
            .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
            .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
            .build();
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        backendConfig,
        multiRegionMultiClusterWrapper)) {
      DaVinciClient<String, Object> viewClient =
          factory.getAndStartGenericAvroClient(storeName, DEFAULT_VIEW_NAME, new DaVinciConfig());
      viewClient.subscribe(Collections.singleton(0)).get();
      for (int i = 1; i <= recordCount; i++) {
        assertEquals(viewClient.get(Integer.toString(i)).get().toString(), DEFAULT_USER_DATA_VALUE_PREFIX + i);
      }
    } finally {
      D2ClientUtils.shutdownClient(d2Client);
    }
  }

  private void setupStoreWithNoView(String inputDirPath, String storeName, Schema recordSchema) {
    Properties props = TestWriteUtils.defaultVPJProps(
        getParentControllerUrl(),
        inputDirPath,
        storeName,
        multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(false)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setNativeReplicationSourceFabric(childDatacenters.get(0).getRegionName())
        .setPartitionCount(3);

    IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
    IntegrationTestPushUtils.runVPJ(props);
  }

  private void setupStoreAndMaterializedView(
      String inputDirPath,
      String storeName,
      String testViewName,
      Schema recordSchema) {
    Properties props = TestWriteUtils.defaultVPJProps(
        getParentControllerUrl(),
        inputDirPath,
        storeName,
        multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(false)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setNativeReplicationSourceFabric(childDatacenters.get(0).getRegionName())
        .setPartitionCount(3);

    try (ControllerClient controllerClient =
        IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms)) {
      MaterializedViewParameters.Builder viewParamBuilder =
          new MaterializedViewParameters.Builder(testViewName).setPartitionCount(1);
      UpdateStoreQueryParams updateViewParam = new UpdateStoreQueryParams().setViewName(testViewName)
          .setViewClassName(MaterializedView.class.getCanonicalName())
          .setViewClassParams(viewParamBuilder.build());
      controllerClient
          .retryableRequest(5, controllerClient1 -> controllerClient.updateStore(storeName, updateViewParam));
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        Map<String, ViewConfig> viewConfigMap = controllerClient.getStore(storeName).getStore().getViewConfigs();
        assertEquals(viewConfigMap.size(), 1);
        assertEquals(viewConfigMap.get(testViewName).getViewClassName(), MaterializedView.class.getCanonicalName());
      });
    }
    IntegrationTestPushUtils.runVPJ(props);
  }

  private ChangelogClientConfig createChangelogClientConfig(
      D2Client d2Client,
      String inputDirPath,
      String testViewName) {
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = ChangelogConsumerTestUtils.buildConsumerProperties(
        multiRegionMultiClusterWrapper,
        localKafka,
        clusterName,
        localZkServer,
        Utils.getUniqueString(inputDirPath));
    consumerProperties.put(CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY, true);

    return new ChangelogClientConfig().setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(localZkServer.getAddress())
        .setControllerRequestRetryCount(3)
        .setVersionSwapDetectionIntervalTimeInSeconds(3)
        .setD2Client(d2Client)
        .setViewName(testViewName)
        .setIsNewStatelessClientEnabled(true);
  }

  private void testCDCConsumer(
      String storeName,
      ChangelogClientConfig changelogClientConfig,
      boolean useStatefulConsumer) throws Exception {
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceChangelogConsumerClientFactory consumerFactory =
        new VeniceChangelogConsumerClientFactory(changelogClientConfig, metricsRepository);

    final List<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages = new ArrayList<>();

    if (useStatefulConsumer) {
      try (StatefulVeniceChangelogConsumer<Integer, Utf8> viewTopicConsumer =
          consumerFactory.getStatefulChangelogConsumer(storeName)) {
        Assert.assertTrue(viewTopicConsumer instanceof VeniceChangelogConsumerDaVinciRecordTransformerImpl);
        viewTopicConsumer.start().get();
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
          Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polled =
              viewTopicConsumer.poll(1000);
          pubSubMessages.addAll(polled);
          Assert.assertEquals(pubSubMessages.size(), 100);
        });
      }
    } else {
      try (VeniceChangelogConsumer<Integer, Utf8> viewTopicConsumer = consumerFactory.getChangelogConsumer(storeName)) {
        Assert.assertTrue(viewTopicConsumer instanceof VeniceChangelogConsumerDaVinciRecordTransformerImpl);
        viewTopicConsumer.subscribeAll().get();
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
          Collection<PubSubMessage<Integer, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polled =
              viewTopicConsumer.poll(1000);
          pubSubMessages.addAll(polled);
          Assert.assertEquals(pubSubMessages.size(), 100);
        });
      }
    }
  }
}
