package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.Utils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests for stateless changelog consumer with different compression strategies.
 * Tests verify that basic reads work correctly with GZIP and ZSTD_WITH_DICT compression.
 */
public class TestStatelessChangelogConsumerWithCompression {
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private D2Client d2Client;

  protected boolean isAAWCParallelProcessingEnabled() {
    return false;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    serverProperties.put(SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, isAAWCParallelProcessingEnabled());
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    clusterName = CLUSTER_NAMES[0];
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);
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
    multiRegionMultiClusterWrapper.close();
    TestView.resetCounters();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStatelessChangelogConsumerWithGzipCompression()
      throws IOException, ExecutionException, InterruptedException {
    testStatelessChangelogConsumerWithCompression(CompressionStrategy.GZIP);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStatelessChangelogConsumerWithZstdWithDictCompression()
      throws IOException, ExecutionException, InterruptedException {
    testStatelessChangelogConsumerWithCompression(CompressionStrategy.ZSTD_WITH_DICT);
  }

  /**
   * Tests that chunked records (values exceeding the chunk size threshold) are correctly consumed
   * when the store uses GZIP compression. This verifies that the chunk assembler's decompression
   * is not followed by a redundant second decompression in the changelog consumer.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStatelessChangelogConsumerWithChunkedGzipRecords()
      throws IOException, ExecutionException, InterruptedException {
    testStatelessChangelogConsumerWithChunking(CompressionStrategy.GZIP);
  }

  private void testStatelessChangelogConsumerWithCompression(CompressionStrategy compressionStrategy)
      throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    int version = 1;
    // Use more records for ZSTD_WITH_DICT to exercise dictionary training
    int numKeys = compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT ? 1000 : 10;
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema(inputDir, Integer.toString(version), numKeys);
    consumeAndValidate(inputDir, recordSchema, numKeys, Function.identity(), 0, compressionStrategy);
  }

  private void testStatelessChangelogConsumerWithChunking(CompressionStrategy compressionStrategy)
      throws IOException, ExecutionException, InterruptedException {
    File inputDir = getTempDataDirectory();
    // Create records larger than the default chunk threshold (~950KB) to force chunking
    int numKeys = 5;
    int valueSizeBytes = 1024 * 1024; // 1MB per value, exceeds the ~950KB chunk threshold
    Schema recordSchema =
        TestWriteUtils.writeSimpleAvroFileWithCustomSize(inputDir, numKeys, valueSizeBytes, valueSizeBytes);
    consumeAndValidate(inputDir, recordSchema, numKeys, i -> Integer.toString(i), valueSizeBytes, compressionStrategy);
  }

  private <T> void consumeAndValidate(
      File inputDir,
      Schema recordSchema,
      int numKeys,
      Function<Integer, T> keyConverter,
      int valueSizeBytes,
      CompressionStrategy compressionStrategy) throws InterruptedException, ExecutionException {
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store-" + compressionStrategy.name().toLowerCase());
    Properties props = TestWriteUtils.defaultVPJProps(
        parentControllers.get(0).getControllerUrl(),
        inputDirPath,
        storeName,
        clusterWrapper.getPubSubClientProperties());
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = STRING_SCHEMA.toString();

    // Create store with the specified compression strategy
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(3)
        .setCompressionStrategy(compressionStrategy);

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms);

    // Push data to the store
    IntegrationTestPushUtils.runVPJ(props);

    MetricsRepository metricsRepository =
        getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);

    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = multiRegionMultiClusterWrapper.getChildRegions().get(0).getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(multiRegionMultiClusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, localZkServer.getAddress());

    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(localZkServer.getAddress())
            .setControllerRequestRetryCount(3)
            .setVersionSwapDetectionIntervalTimeInSeconds(3)
            .setD2Client(d2Client)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));

    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    VeniceChangelogConsumer<T, Utf8> changeLogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName);

    changeLogConsumer.subscribeAll().get();

    Map<String, PubSubMessage<T, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesMap = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Collection<PubSubMessage<T, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessagesList =
          changeLogConsumer.poll(1000);
      for (PubSubMessage<T, ChangeEvent<Utf8>, VeniceChangeCoordinate> message: pubSubMessagesList) {
        if (message.getKey() != null) {
          pubSubMessagesMap.put(message.getKey().toString(), message);
        }
      }
      assertEquals(pubSubMessagesMap.size(), numKeys, "Expected to receive all " + numKeys + " chunked messages");
    });

    // Verify the content of received chunked messages
    for (int i = 0; i < numKeys; i++) {
      T key = keyConverter.apply(i);
      assertTrue(pubSubMessagesMap.containsKey(key), "Should have received key " + key);
      PubSubMessage<T, ChangeEvent<Utf8>, VeniceChangeCoordinate> message = pubSubMessagesMap.get(key);
      assertNotNull(message.getValue(), "Value for key " + key + " should not be null");
      ChangeEvent<Utf8> changeEvent = message.getValue();
      assertNotNull(changeEvent.getCurrentValue(), "Current value for key " + key + " should not be null");
      // Each value should be at least valueSizeBytes long (large enough to have been chunked)
      assertTrue(
          changeEvent.getCurrentValue().toString().length() >= valueSizeBytes,
          "Value for key " + key + " should be at least " + valueSizeBytes + " chars");
    }

    changeLogConsumer.close();
  }
}
