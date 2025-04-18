package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEPRECATED_TOPIC_MAX_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.FATAL_DATA_VALIDATION_FAILURE_TOPIC_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.INSTANCE_ID;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.MIN_CONSUMER_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestFatalDataValidationExceptionHandling {
  public static final int NUMBER_OF_SERVERS = 1;

  private VeniceClusterWrapper veniceCluster;

  protected final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private PubSubPositionTypeRegistry pubSubPositionTypeRegistry;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    veniceCluster = setUpCluster();
    pubSubPositionTypeRegistry = veniceCluster.getPubSubBrokerWrapper().getPubSubPositionTypeRegistry();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  private VeniceClusterWrapper setUpCluster() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "5");
    /**
     * Set DEPRECATED_TOPIC_MAX_RETENTION_MS to a value < FATAL_DATA_VALIDATION_FAILURE_TOPIC_RETENTION_MS, so that
     * the topic would not be deleted before the test completes. Set MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE
     * to 0 so that for testing purpose the topic will be picked for deletion.
     */
    extraProperties.setProperty(DEPRECATED_TOPIC_MAX_RETENTION_MS, Long.toString(TimeUnit.SECONDS.toMillis(20)));
    extraProperties
        .setProperty(FATAL_DATA_VALIDATION_FAILURE_TOPIC_RETENTION_MS, Long.toString(TimeUnit.SECONDS.toMillis(30)));
    extraProperties.setProperty(MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE, "0");
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(1)
        .replicationFactor(1)
        .partitionSize(1000000)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(extraProperties)
        .build();
    VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options);

    // Add Venice Router
    Properties routerProperties = new Properties();
    cluster.addVeniceRouter(routerProperties);

    // Add Venice Server
    Properties serverProperties = new Properties();
    serverProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    serverProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");

    serverProperties.setProperty(SSL_TO_KAFKA_LEGACY, "false");

    // Limit shared consumer thread pool size to 1.
    serverProperties.setProperty(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "1");
    serverProperties.setProperty(MIN_CONSUMER_IN_CONSUMER_POOL_PER_KAFKA_CLUSTER, "1");
    serverProperties.setProperty(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, "true");

    // Set the max number of partitions to 1 so that we can test the partition-wise shared consumer assignment strategy.
    serverProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "1");
    serverProperties.setProperty(
        SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY,
        KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY.name());

    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      cluster.addVeniceServer(new Properties(), serverProperties);
    }
    return cluster;
  }

  /**
   * This test verifies that the Venice Controller handles fatal data validation exceptions correctly as expected.
   *
   * testFatalDataValidationHandling does the following:
   * 1. Create a Venice Controller with a parent controller and a Venice Server.
   * 2. Create a store and add a version to it.
   * 3. Verify that initially, the topic retention is set to MAX_LONG_VALUE when it is created.
   * 4. Create three Venice Writers with deterministic GUIDs and write data to the store.
   * 5. Create a CORRUPT data validation exception scenario by writing data with incorrect checksum value.
   * 6. Verify that the store version is errored.
   * 7. Verify that, when errored with data validation exception, the topic retention is set to
   *    FATAL_DATA_VALIDATION_FAILURE_TOPIC_RETENTION_MS.
   * 8. Mock the PushJobDetails so that the topic has NOW as the 'reportTimestamp'.
   * 9. Verify that the topic is deleted after FATAL_DATA_VALIDATION_FAILURE_TOPIC_RETENTION_MS is expired.
   */
  @Test(timeOut = 2 * 60 * Time.MS_PER_SECOND)
  public void testFatalDataValidationHandling() {
    final String storeName = Utils.getUniqueString("batch-store-test");
    final String[] storeNames = new String[] { storeName };

    try (
        ControllerClient controllerClient =
            new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());
        TopicManager topicManager = veniceCluster.getLeaderVeniceController().getVeniceAdmin().getTopicManager()) {
      createStoresAndVersions(controllerClient, storeNames);
      String versionTopicName = Version.composeKafkaTopic(storeName, 1);

      // Verify that the topic retention is set to MAX_LONG_VALUE when it is created.
      TestUtils.waitForNonDeterministicAssertion(
          20,
          TimeUnit.SECONDS,
          true,
          true,
          () -> assertEquals(
              topicManager.getTopicRetention(pubSubTopicRepository.getTopic(versionTopicName)),
              Long.MAX_VALUE,
              "The topic should have MAX_LONG_VALUE retention"));

      Properties veniceWriterProperties1 =
          getVeniceWriterPropertiesWithDeterministicGuid("writer_1_hostname", veniceCluster.getPubSubBrokerWrapper());
      Properties veniceWriterProperties2 =
          getVeniceWriterPropertiesWithDeterministicGuid("writer_2_hostname", veniceCluster.getPubSubBrokerWrapper());
      Properties veniceWriterProperties3 =
          getVeniceWriterProperties("writer_3_hostname", veniceCluster.getPubSubBrokerWrapper());

      AvroSerializer<String> stringSerializer = new AvroSerializer(STRING_SCHEMA);
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
          veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
      try (
          VeniceWriter<byte[], byte[], byte[]> veniceWriter1 = TestUtils
              .getVeniceWriterFactory(veniceWriterProperties1, pubSubProducerAdapterFactory, pubSubPositionTypeRegistry)
              .createVeniceWriter(new VeniceWriterOptions.Builder(versionTopicName).build());
          VeniceWriter<byte[], byte[], byte[]> veniceWriter2 = TestUtils
              .getVeniceWriterFactory(veniceWriterProperties2, pubSubProducerAdapterFactory, pubSubPositionTypeRegistry)
              .createVeniceWriter(new VeniceWriterOptions.Builder(versionTopicName).build());
          VeniceWriter<byte[], byte[], byte[]> veniceWriter3 = TestUtils
              .getVeniceWriterFactory(veniceWriterProperties3, pubSubProducerAdapterFactory, pubSubPositionTypeRegistry)
              .createVeniceWriter(new VeniceWriterOptions.Builder(versionTopicName).build())) {
        createCorruptDIVScenario(veniceWriter1, veniceWriter2, veniceWriter3, stringSerializer);
      }

      // Verify that the store version is errored.
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
        // Now the store should have version 1
        JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(Version.composeKafkaTopic(storeName, 1));
        Assert.assertFalse(jobStatus.isError(), "Error in getting JobStatusResponse: " + jobStatus.getError());
        assertEquals(jobStatus.getStatus(), "ERROR", "The job should be errored");

        // Verify that the topic retention is set to 30 seconds.
        Assert.assertEquals(
            topicManager.getTopicRetention(pubSubTopicRepository.getTopic(versionTopicName)),
            TimeUnit.SECONDS.toMillis(30));
      });

      // Mock the PushJobDetails so that the topic has NOW as the 'reportTimestamp'.
      mockPushJobDetails();

      // Then after the retention time (30 seconds) is expired, TopicCleanupService picks the topic and delete it.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
        Assert.assertFalse(topicManager.containsTopic(pubSubTopicRepository.getTopic(versionTopicName)));
      });
    }
  }

  private void mockPushJobDetails() {
    PushJobDetails pushJobDetails = new PushJobDetails();
    initPushJobDetails(pushJobDetails);

    // Mock the getPushJobDetails API to return the errored push job details.
    VeniceHelixAdmin admin = (VeniceHelixAdmin) veniceCluster.getLeaderVeniceController().getVeniceAdmin();
    AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> mockedPushJobDetailsStoreClient =
        mock(AvroSpecificStoreClient.class);
    CompletableFuture<PushJobDetails> result = new CompletableFuture<>();
    result.complete(pushJobDetails);
    when(mockedPushJobDetailsStoreClient.get(any())).thenReturn(result);
    admin.setPushJobDetailsStoreClient(mockedPushJobDetailsStoreClient);
  }

  private PushJobDetailsStatusTuple getPushJobDetailsStatusTuple(int status) {
    PushJobDetailsStatusTuple tuple = new PushJobDetailsStatusTuple();
    tuple.status = status;
    tuple.timestamp = System.currentTimeMillis();
    return tuple;
  }

  private void initPushJobDetails(PushJobDetails pushJobDetails) {
    pushJobDetails.clusterName = veniceCluster.getClusterName();
    pushJobDetails.overallStatus = new ArrayList<>();
    pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.STARTED.getValue()));
    pushJobDetails.pushId = "";
    pushJobDetails.partitionCount = -1;
    pushJobDetails.valueCompressionStrategy = CompressionStrategy.NO_OP.getValue();
    pushJobDetails.chunkingEnabled = false;
    pushJobDetails.jobDurationInMs = -1;
    pushJobDetails.totalNumberOfRecords = -1;
    pushJobDetails.totalKeyBytes = -1;
    pushJobDetails.totalRawValueBytes = -1;
    pushJobDetails.totalCompressedValueBytes = -1;
    pushJobDetails.failureDetails = "";
    pushJobDetails.pushJobLatestCheckpoint = PushJobCheckpoints.INITIALIZE_PUSH_JOB.getValue();
    pushJobDetails.pushJobConfigs =
        Collections.singletonMap(HEARTBEAT_ENABLED_CONFIG.getConfigName(), String.valueOf(true));
  }

  private Properties getVeniceWriterPropertiesWithDeterministicGuid(
      String writerId,
      PubSubBrokerWrapper pubSubBrokerWrapper) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    veniceWriterProperties
        .putAll(PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(pubSubBrokerWrapper)));
    veniceWriterProperties.put(INSTANCE_ID, writerId);
    veniceWriterProperties.put(PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS, 0L);
    veniceWriterProperties.put(PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS, 0L);
    veniceWriterProperties
        .put(GuidUtils.GUID_GENERATOR_IMPLEMENTATION, GuidUtils.DETERMINISTIC_GUID_GENERATOR_IMPLEMENTATION);
    return veniceWriterProperties;
  }

  private Properties getVeniceWriterProperties(String writerId, PubSubBrokerWrapper pubSubBrokerWrapper) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    veniceWriterProperties
        .putAll(PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(pubSubBrokerWrapper)));
    veniceWriterProperties.put(INSTANCE_ID, writerId);
    return veniceWriterProperties;
  }

  private void createCorruptDIVScenario(
      VeniceWriter<byte[], byte[], byte[]> vw1,
      VeniceWriter<byte[], byte[], byte[]> vw2,
      VeniceWriter<byte[], byte[], byte[]> vw3,
      AvroSerializer<String> stringSerializer) {
    vw3.broadcastStartOfPush(false, Collections.emptyMap());
    vw3.flush();

    vw1.put(
        stringSerializer.serialize("key_writer_1"),
        stringSerializer.serialize("value_writer_1"),
        1,
        null,
        new LeaderMetadataWrapper(0, 0));
    vw1.flush();

    vw2.put(
        stringSerializer.serialize("key_writer_2"),
        stringSerializer.serialize("value_writer_2"),
        1,
        null,
        new LeaderMetadataWrapper(1, 0));
    vw2.put(
        stringSerializer.serialize("key_writer_3"),
        stringSerializer.serialize("value_writer_3"),
        1,
        null,
        new LeaderMetadataWrapper(2, 0));
    vw2.flush();

    vw1.put(
        stringSerializer.serialize("key_writer_4"),
        stringSerializer.serialize("value_writer_4"),
        1,
        null,
        new LeaderMetadataWrapper(3, 0));
    vw1.flush();
    vw1.closePartition(0);
    vw1.flush();

    vw3.broadcastEndOfPush(Collections.emptyMap());
    vw3.flush();
  }

  private void createStoresAndVersions(ControllerClient controllerClient, String[] storeNames) {
    for (String storeName: storeNames) {
      controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString());

      // There should be no version on the store yet.
      assertEquals(
          controllerClient.getStore(storeName).getStore().getCurrentVersion(),
          0,
          "The newly created store must have a current version of 0");

      VersionResponse versionResponse = controllerClient.addVersionAndStartIngestion(
          storeName,
          Utils.getUniqueString("batch-push-job"),
          1,
          1,
          Version.PushType.BATCH,
          null,
          -1,
          1);
      assertFalse(
          versionResponse.isError(),
          "Version creation shouldn't return error, but received: " + versionResponse.getError());
    }
  }
}
