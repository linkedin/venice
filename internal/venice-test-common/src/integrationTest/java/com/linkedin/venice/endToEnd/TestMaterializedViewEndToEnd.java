package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.views.MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX;
import static com.linkedin.venice.views.VeniceView.VIEW_NAME_SEPARATOR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.StatefulVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceAfterImageConsumerImpl;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
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
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.view.TestValueBasedVenicePartitioner;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestMaterializedViewEndToEnd {
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final String DEFAULT_VIEW_NAME = "MaterializedViewTest";
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(2)
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
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testLFIngestionWithMaterializedView() throws IOException {
    // Create a non-A/A store with materialized view and run batch push job with 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(false)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setNativeReplicationSourceFabric(childDatacenters.get(0).getRegionName())
        .setPartitionCount(3);
    try (ControllerClient controllerClient =
        IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms)) {
      MaterializedViewParameters.Builder viewParamBuilder =
          new MaterializedViewParameters.Builder(DEFAULT_VIEW_NAME).setPartitionCount(6);
      UpdateStoreQueryParams updateViewParam = new UpdateStoreQueryParams().setViewName(DEFAULT_VIEW_NAME)
          .setViewClassName(MaterializedView.class.getCanonicalName())
          .setViewClassParams(viewParamBuilder.build());
      controllerClient
          .retryableRequest(5, controllerClient1 -> controllerClient.updateStore(storeName, updateViewParam));
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
        Map<String, ViewConfig> viewConfigMap = controllerClient.getStore(storeName).getStore().getViewConfigs();
        assertEquals(viewConfigMap.size(), 1);
        assertEquals(
            viewConfigMap.get(DEFAULT_VIEW_NAME).getViewClassName(),
            MaterializedView.class.getCanonicalName());
        assertEquals(viewConfigMap.get(DEFAULT_VIEW_NAME).getViewParameters().size(), 3);
      });

      IntegrationTestPushUtils.runVPJ(props);
      // TODO we will verify the actual content once the DVC consumption part of the view topic is completed.
      // For now just check for topic existence and that they contain some records.
      String viewTopicName = MaterializedView.composeTopicName(storeName, 1, DEFAULT_VIEW_NAME);
      String versionTopicName = Version.composeKafkaTopic(storeName, 1);
      validateViewTopicAndVersionTopic(viewTopicName, versionTopicName, 6, 3, 100);

      // A re-push should succeed
      Properties rePushProps = TestWriteUtils.defaultVPJProps(
          parentControllers.get(0).getControllerUrl(),
          inputDirPath,
          storeName,
          multiRegionMultiClusterWrapper.getPubSubClientProperties());
      rePushProps.setProperty(SOURCE_KAFKA, "true");
      rePushProps.setProperty(KAFKA_INPUT_BROKER_URL, childDatacenters.get(0).getPubSubBrokerWrapper().getAddress());
      IntegrationTestPushUtils.runVPJ(rePushProps);

      for (VeniceMultiClusterWrapper veniceClusterWrapper: childDatacenters) {
        try (ControllerClient childControllerClient =
            new ControllerClient(clusterName, veniceClusterWrapper.getRandomController().getControllerUrl())) {
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            StoreResponse storeResponse = childControllerClient.getStore(storeName);
            Assert.assertFalse(storeResponse.isError());
            Assert.assertEquals(
                storeResponse.getStore().getCurrentVersion(),
                2,
                "Version 2 should be created for the store: " + storeName);
          });
        }
      }
      String rePushViewTopicName = MaterializedView.composeTopicName(storeName, 2, DEFAULT_VIEW_NAME);
      String rePushVersionTopicName = Version.composeKafkaTopic(storeName, 2);
      validateViewTopicAndVersionTopic(rePushViewTopicName, rePushVersionTopicName, 6, 3, 100);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyMaterializedViewDVCConsumer() throws Exception {
    // Create a batch only store with materialized view and run batch push job with 100 records
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("batchStore");
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
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
    String testViewName = "MaterializedViewTest";
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

    // Start a DVC client that's subscribed to partition 0 of the store's materialized view. The DVC client should
    // contain all data records.
    VeniceProperties backendConfig =
        new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
            .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
            .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
            .build();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    // Use non-source fabric region to also verify NR for materialized view.
    D2Client daVinciD2RemoteFabric = D2TestUtils
        .getAndStartD2Client(multiRegionMultiClusterWrapper.getChildRegions().get(1).getZkServerWrapper().getAddress());
    MetricsRepository dvcMetricsRepo = new MetricsRepository();
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        daVinciD2RemoteFabric,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        dvcMetricsRepo,
        backendConfig,
        multiRegionMultiClusterWrapper)) {
      // Ensure compatibility of store and view DVC clients since it's possible for the same JVM to subscribe to a store
      // and its view(s).
      DaVinciClient<String, Object> storeClient = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
      storeClient.subscribeAll().get();
      // writeSimpleAvroFileWithStringToStringSchema writes input starting at 1 and stops with <= recordCount (100)
      for (int i = 1; i <= 100; i++) {
        assertEquals(storeClient.get(Integer.toString(i)).get().toString(), DEFAULT_USER_DATA_VALUE_PREFIX + i);
      }
      DaVinciClient<String, Object> viewClient =
          factory.getAndStartGenericAvroClient(storeName, testViewName, daVinciConfig);
      viewClient.subscribe(Collections.singleton(0)).get();
      assertEquals(
          getMetric(
              dvcMetricsRepo,
              "current_version_number.Gauge",
              VeniceView.getViewStoreName(storeName, testViewName)),
          (double) 1);
      for (int i = 1; i <= 100; i++) {
        assertEquals(viewClient.get(Integer.toString(i)).get().toString(), DEFAULT_USER_DATA_VALUE_PREFIX + i);
      }
      // Perform another push with 200 keys to verify future version ingestion and version swap
      File newPushInputDir = getTempDataDirectory();
      TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(newPushInputDir, 200);
      String newPushInputDirPath = "file:" + newPushInputDir.getAbsolutePath();
      Properties newPushProps = TestWriteUtils.defaultVPJProps(
          parentControllers.get(0).getControllerUrl(),
          newPushInputDirPath,
          storeName,
          multiRegionMultiClusterWrapper.getPubSubClientProperties());
      IntegrationTestPushUtils.runVPJ(newPushProps);
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> assertEquals(
              getMetric(
                  dvcMetricsRepo,
                  "current_version_number.Gauge",
                  VeniceView.getViewStoreName(storeName, testViewName)),
              (double) 2));
      // The materialized view DVC client should be able to read all the keys from the new push
      for (int i = 1; i <= 200; i++) {
        assertEquals(viewClient.get(Integer.toString(i)).get().toString(), DEFAULT_USER_DATA_VALUE_PREFIX + i);
      }
    } finally {
      D2ClientUtils.shutdownClient(daVinciD2RemoteFabric);
    }

    // Make sure things work in the source fabric too.
    VeniceProperties newBackendConfig =
        new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
            .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
            .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
            .build();
    D2Client daVinciD2SourceFabric = D2TestUtils
        .getAndStartD2Client(multiRegionMultiClusterWrapper.getChildRegions().get(1).getZkServerWrapper().getAddress());
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        daVinciD2SourceFabric,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        newBackendConfig,
        multiRegionMultiClusterWrapper)) {
      DaVinciClient<String, Object> viewClient =
          factory.getAndStartGenericAvroClient(storeName, testViewName, daVinciConfig);
      viewClient.subscribe(Collections.singleton(0)).get();
      for (int i = 1; i <= 200; i++) {
        assertEquals(viewClient.get(Integer.toString(i)).get().toString(), DEFAULT_USER_DATA_VALUE_PREFIX + i);
      }
    } finally {
      D2ClientUtils.shutdownClient(daVinciD2SourceFabric);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testLargeValuePushMaterializedViewDVCConsumer()
      throws IOException, ExecutionException, InterruptedException {
    // A batch push with value size spanning from 0 to 2MB to have a mix workload of chunked and non-chunked records.
    int minValueSize = 0;
    int maxValueSize = 2 * BYTES_PER_MB;
    int numberOfRecords = 100;
    File inputDir = getTempDataDirectory();
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithCustomSize(inputDir, numberOfRecords, minValueSize, maxValueSize);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("largeValueBatchStore");
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
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
    String testViewName = "MaterializedViewTest";
    try (ControllerClient controllerClient =
        IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms)) {
      MaterializedViewParameters.Builder viewParamBuilder =
          new MaterializedViewParameters.Builder(testViewName).setPartitionCount(2);
      UpdateStoreQueryParams updateViewParam = new UpdateStoreQueryParams().setViewName(testViewName)
          .setViewClassName(MaterializedView.class.getCanonicalName())
          .setViewClassParams(viewParamBuilder.build());
      controllerClient
          .retryableRequest(5, controllerClient1 -> controllerClient.updateStore(storeName, updateViewParam));
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, false, () -> {
        Map<String, ViewConfig> viewConfigMap = controllerClient.getStore(storeName).getStore().getViewConfigs();
        assertEquals(viewConfigMap.size(), 1);
        assertEquals(viewConfigMap.get(testViewName).getViewClassName(), MaterializedView.class.getCanonicalName());
      });
    }
    IntegrationTestPushUtils.runVPJ(props);
    // Start a DVC client that's subscribed to all partitions of the store's materialized view. The DVC client should
    // contain all data records.
    VeniceProperties backendConfig =
        new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
            .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
            .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
            .build();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    // Use non-source fabric region to also verify NR for materialized view and chunks forwarding.
    D2Client daVinciD2RemoteFabric = D2TestUtils
        .getAndStartD2Client(multiRegionMultiClusterWrapper.getChildRegions().get(1).getZkServerWrapper().getAddress());
    MetricsRepository dvcMetricsRepo = new MetricsRepository();
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        daVinciD2RemoteFabric,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        dvcMetricsRepo,
        backendConfig,
        multiRegionMultiClusterWrapper)) {
      DaVinciClient<String, Object> viewClient =
          factory.getAndStartGenericAvroClient(storeName, testViewName, daVinciConfig);
      viewClient.subscribeAll().get();
      for (int i = 0; i < numberOfRecords; i++) {
        Assert.assertNotNull(viewClient.get(Integer.toString(i)).get());
      }
    } finally {
      D2ClientUtils.shutdownClient(daVinciD2RemoteFabric);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testLargeValuePushMaterializedViewCCConsumer()
      throws IOException, ExecutionException, InterruptedException {
    // A batch push with value size spanning from 0 to 2MB to have a mix workload of chunked and non-chunked records.
    int minValueSize = 0;
    int maxValueSize = 2 * BYTES_PER_MB;
    int numberOfRecords = 100;
    File inputDir = getTempDataDirectory();
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithCustomSize(inputDir, numberOfRecords, minValueSize, maxValueSize);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("largeValueBatchStore");
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
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
    String testViewName = "MaterializedViewTest";
    try (ControllerClient controllerClient =
        IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms)) {
      MaterializedViewParameters.Builder viewParamBuilder =
          new MaterializedViewParameters.Builder(testViewName).setPartitionCount(2);
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
    // Start a CC consumer in remote region to make sure it can consume all the records properly.
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    consumerProperties.put(
        KAFKA_BOOTSTRAP_SERVERS,
        multiRegionMultiClusterWrapper.getChildRegions().get(1).getPubSubBrokerWrapper().getAddress());
    ChangelogClientConfig viewChangeLogClientConfig = new ChangelogClientConfig().setViewName(testViewName)
        .setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(multiRegionMultiClusterWrapper.getChildRegions().get(1).getZkServerWrapper().getAddress())
        .setD2Client(
            IntegrationTestPushUtils
                .getD2Client(multiRegionMultiClusterWrapper.getChildRegions().get(1).getZkServerWrapper().getAddress()))
        .setVersionSwapDetectionIntervalTimeInSeconds(3L)
        .setControllerRequestRetryCount(3)
        .setBootstrapFileSystemPath(getTempDataDirectory().getAbsolutePath());
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceChangelogConsumerClientFactory veniceViewChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(viewChangeLogClientConfig, metricsRepository);
    VeniceChangelogConsumer<Utf8, Utf8> viewTopicConsumer =
        veniceViewChangelogConsumerClientFactory.getChangelogConsumer(storeName);
    assertTrue(viewTopicConsumer instanceof VeniceAfterImageConsumerImpl);
    viewTopicConsumer.subscribeAll().get();
    // Verify we can get the records
    final List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages = new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
      pubSubMessages.addAll(viewTopicConsumer.poll(1000));
      Assert.assertEquals(pubSubMessages.size(), numberOfRecords);
    });
  }

  /**
   * Verification of the produced records is difficult because we don't really support complex partitioner in the
   * read path. Once CC with views is supported we should use CC to verify. Perform re-push to ensure we can deserialize
   * value properly during re-push.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testMaterializedViewWithComplexPartitioner() throws IOException {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV2Schema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("complexPartitionStore");
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    // Use an A/A W/C enabled store to verify correct partitioning after partial update is applied.
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(false)
        .setChunkingEnabled(true)
        .setCompressionStrategy(CompressionStrategy.GZIP)
        .setRmdChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setNativeReplicationSourceFabric(childDatacenters.get(0).getRegionName())
        .setPartitionCount(3)
        .setActiveActiveReplicationEnabled(true)
        .setWriteComputationEnabled(true)
        .setHybridRewindSeconds(10L)
        .setHybridOffsetLagThreshold(2L);
    String testViewName = "complexPartitionerView";
    try (ControllerClient controllerClient =
        IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms)) {
      MaterializedViewParameters.Builder viewParamBuilder =
          new MaterializedViewParameters.Builder(testViewName).setPartitionCount(2)
              .setPartitioner(TestValueBasedVenicePartitioner.class.getCanonicalName());
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
    String viewTopicName =
        Version.composeKafkaTopic(storeName, 1) + VIEW_NAME_SEPARATOR + testViewName + MATERIALIZED_VIEW_TOPIC_SUFFIX;
    // View topic partitions should be mostly empty based on the TestValueBasedPartitioner logic.
    int expectedMaxEndOffset = 6; // This may change when we introduce more CMs e.g. heartbeats
    for (VeniceMultiClusterWrapper veniceClusterWrapper: childDatacenters) {
      VeniceHelixAdmin admin = veniceClusterWrapper.getRandomController().getVeniceHelixAdmin();
      PubSubTopic viewPubSubTopic = admin.getPubSubTopicRepository().getTopic(viewTopicName);
      Int2LongMap viewTopicPerPartitionRecords =
          getNumberOfRecordsPerPartition(admin.getTopicManager(), viewPubSubTopic);
      for (Int2LongMap.Entry entry: viewTopicPerPartitionRecords.int2LongEntrySet()) {
        int partition = entry.getIntKey();
        long recordCount = entry.getLongValue();
        // p0 and p1 should have more records than p2
        assertTrue(
            recordCount <= expectedMaxEndOffset,
            "Expected partition " + partition + " to have at most " + expectedMaxEndOffset + " records, but found: "
                + recordCount);
      }
    }
    // Perform some partial updates in the non-NR source fabric
    VeniceClusterWrapper veniceCluster = childDatacenters.get(1).getClusters().get(clusterName);
    SystemProducer producer =
        IntegrationTestPushUtils.getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance()
        .convertFromValueRecordSchema(recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema());
    long newTimestamp = 100000L;
    for (int i = 1; i < 20; i++) {
      String key = Integer.toString(i);
      UpdateBuilder updateBuilder = new UpdateBuilderImpl(partialUpdateSchema);
      updateBuilder.setNewFieldValue("age", i);
      IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, key, updateBuilder.build(), newTimestamp);
    }
    // age 1-9 will be written to all partitions so +9 in p0 and p1
    // age 10-19 will be written to % numPartitions which will alternate so +5 in p0 and p1
    int newMinEndOffset = expectedMaxEndOffset + 9 + 5;
    for (VeniceMultiClusterWrapper veniceClusterWrapper: childDatacenters) {
      VeniceHelixAdmin admin = veniceClusterWrapper.getRandomController().getVeniceHelixAdmin();
      PubSubTopic viewPubSubTopic = admin.getPubSubTopicRepository().getTopic(viewTopicName);
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        Int2LongMap viewTopicPerPartitionRecords =
            getNumberOfRecordsPerPartition(admin.getTopicManager(), viewPubSubTopic);
        for (Int2LongMap.Entry entry: viewTopicPerPartitionRecords.int2LongEntrySet()) {
          int partition = entry.getIntKey();
          long recordCount = entry.getLongValue();
          // p0 and p1 should have more records than p2
          assertTrue(
              recordCount >= newMinEndOffset,
              "Expected partition " + partition + " to have at least " + newMinEndOffset + " records, but found: "
                  + recordCount);
        }
      });
    }
    // A re-push should succeed
    Properties rePushProps = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        multiRegionMultiClusterWrapper.getPubSubClientProperties());
    rePushProps.setProperty(SOURCE_KAFKA, "true");
    rePushProps.setProperty(KAFKA_INPUT_BROKER_URL, childDatacenters.get(0).getPubSubBrokerWrapper().getAddress());
    IntegrationTestPushUtils.runVPJ(rePushProps);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyMaterializedViewStatelessCDCConsumer() throws Exception {
    testBatchOnlyMaterializedViewCDCConsumer(false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyMaterializedViewStatefulCDCConsumer() throws Exception {
    testBatchOnlyMaterializedViewCDCConsumer(true);
  }

  /**
   * This test has comparable setup to {@link #testBatchOnlyMaterializedViewStatelessCDCConsumer}
   * and {@link #testBatchOnlyMaterializedViewStatefulCDCConsumer}
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

  public void testBatchOnlyNoViewWithCDCConsumer(boolean isStatefulClient) throws Exception {
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
        parentControllers.get(0).getControllerUrl(),
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
        parentControllers.get(0).getControllerUrl(),
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
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());
    consumerProperties.put(CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY, true);

    return new ChangelogClientConfig().setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(localZkServer.getAddress())
        .setControllerRequestRetryCount(3)
        .setVersionSwapDetectionIntervalTimeInSeconds(3)
        .setD2Client(d2Client)
        .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath))
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

  private double getMetric(MetricsRepository metricsRepository, String metricName, String storeName) {
    Metric metric = metricsRepository.getMetric("." + storeName + "--" + metricName);
    assertNotNull(metric, "Expected metric " + metricName + " not found.");
    return metric.value();
  }

  private void validateViewTopicAndVersionTopic(
      String viewTopicName,
      String versionTopicName,
      int viewTopicPartitionCount,
      int versionTopicPartitionCount,
      int minRecordCount) {
    for (VeniceMultiClusterWrapper veniceClusterWrapper: childDatacenters) {
      VeniceHelixAdmin admin = veniceClusterWrapper.getRandomController().getVeniceHelixAdmin();
      TopicManager topicManager = admin.getTopicManager();
      PubSubTopic viewPubSubTopic = admin.getPubSubTopicRepository().getTopic(viewTopicName);
      PubSubTopic versionPubSubTopic = admin.getPubSubTopicRepository().getTopic(versionTopicName);
      assertTrue(admin.getTopicManager().containsTopic(viewPubSubTopic));

      TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, () -> {
        long records = getNumberOfRecordsInTopic(topicManager, viewPubSubTopic);
        long versionTopicRecords = getNumberOfRecordsInTopic(topicManager, versionPubSubTopic);
        assertEquals(
            topicManager.getTopicPartitionInfo(versionPubSubTopic).size(),
            versionTopicPartitionCount,
            "Unexpected version partition count");
        assertEquals(
            topicManager.getTopicPartitionInfo(viewPubSubTopic).size(),
            viewTopicPartitionCount,
            "Unexpected view partition count");
        assertTrue(versionTopicRecords > minRecordCount, "Version topic records size: " + versionTopicRecords);
        assertTrue(records > minRecordCount, "View topic records size: " + records);
      });
    }
  }

  private static Int2LongMap getNumberOfRecordsPerPartition(TopicManager topicManager, PubSubTopic topic) {
    Map<PubSubTopicPartition, PubSubPosition> topicPositions = topicManager.getEndPositionsForTopicWithRetries(topic);
    Int2LongMap partitionRecordCount = new Int2LongOpenHashMap(topicPositions.size());
    for (Map.Entry<PubSubTopicPartition, PubSubPosition> entry: topicPositions.entrySet()) {
      PubSubTopicPartition partition = entry.getKey();
      long delta = topicManager.diffPosition(partition, entry.getValue(), PubSubSymbolicPosition.EARLIEST);
      partitionRecordCount.put(partition.getPartitionNumber(), delta);
    }
    return partitionRecordCount;
  }

  // count records across all partitions in a topic
  public static long getNumberOfRecordsInTopic(TopicManager topicManager, PubSubTopic topic) {
    Int2LongMap partitionRecordCount = getNumberOfRecordsPerPartition(topicManager, topic);
    long totalRecords = 0;
    for (Int2LongMap.Entry entry: partitionRecordCount.int2LongEntrySet()) {
      totalRecords += entry.getLongValue();
    }
    return totalRecords;
  }
}
