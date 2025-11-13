package com.linkedin.venice.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_SECURE_RANDOM_IMPLEMENTATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_TYPE;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_ACL_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.SslUtils.LOCAL_KEYSTORE_JKS;
import static com.linkedin.venice.utils.SslUtils.LOCAL_PASSWORD;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFile;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.StatefulVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.endToEnd.TestChangelogKey;
import com.linkedin.venice.endToEnd.TestChangelogValue;
import com.linkedin.venice.endToEnd.TestChangelogValueV2;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PushInputSchemaBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class StatefulVeniceChangelogConsumerTest {
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;
  private static final int PARTITION_COUNT = 3;

  // Use a unique key for DELETE with RMD validation
  private final TestChangelogKey deleteWithRmdKeyIndex = new TestChangelogKey();
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private D2Client d2Client;
  private MetricsRepository metricsRepository;
  private String zkAddress;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();

    Properties clusterConfig = new Properties();
    clusterConfig.put(PUSH_STATUS_STORE_ENABLED, true);
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 3);
    clusterConfig.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(1)
        .replicationFactor(1)
        .forkServer(false)
        .extraProperties(clusterConfig)
        .build();

    clusterWrapper = ServiceFactory.getVeniceCluster(options);
    clusterName = clusterWrapper.getClusterName();
    zkAddress = clusterWrapper.getZk().getAddress();

    d2Client = new D2ClientBuilder().setZkHosts(zkAddress)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);

    metricsRepository = getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);

    deleteWithRmdKeyIndex.id = 1000;
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    clusterWrapper.close();
    TestView.resetCounters();
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testVeniceChangelogConsumerDaVinciRecordTransformerImpl() throws Exception {
    String storeName = Utils.getUniqueString("store");
    String inputDirPath = setUpStore(storeName);

    PubSubBrokerWrapper localKafka = clusterWrapper.getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, zkAddress);
    consumerProperties.putAll(clusterWrapper.getPubSubClientProperties());
    consumerProperties.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, true);
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(zkAddress)
            .setControllerRequestRetryCount(3)
            .setBootstrapFileSystemPath(inputDirPath)
            .setD2Client(d2Client)
            // Setting the max buffer size to a low threshold to ensure puts to the buffer get blocked and drained
            // correctly during regular operation and restarts
            .setMaxBufferSize(10);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    StatefulVeniceChangelogConsumer<GenericRecord, GenericRecord> statefulVeniceChangelogConsumer =
        veniceChangelogConsumerClientFactory.getStatefulChangelogConsumer(storeName);

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, 1, null, 10, 10, 100, false);
      // Produce a DELETE record with large timestamp
      sendStreamingDeleteRecord(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000L);
    }

    statefulVeniceChangelogConsumer.start().get();
    assertFalse(statefulVeniceChangelogConsumer.isCaughtUp());

    Map<String, PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsMap =
        new HashMap<>();
    List<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsList =
        new ArrayList<>();
    // 21 changes in near-line. 10 puts, 10 deletes, and 1 record with a producer timestamp
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          statefulVeniceChangelogConsumer);
      // 21 events for near-line events
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 21;
      assertEquals(polledChangeEventsList.size(), expectedRecordCount);
      verifyPut(polledChangeEventsMap, 100, 110, 1, false);
      verifyDelete(polledChangeEventsMap, 110, 120, 1);
    });

    long startingSequenceId = polledChangeEventsList.iterator().next().getPosition().getConsumerSequenceId();
    Map<Integer, Long> partitionSequenceIdMap = new HashMap<>();
    verifyVCCSequenceId(polledChangeEventsList, partitionSequenceIdMap, startingSequenceId);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(statefulVeniceChangelogConsumer.isCaughtUp());
    });

    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    int startIndex = runNearlineJobAndVerifyConsumption(
        120,
        storeName,
        1,
        polledChangeEventsMap,
        polledChangeEventsList,
        statefulVeniceChangelogConsumer,
        false,
        false);
    verifyVCCSequenceId(polledChangeEventsList, partitionSequenceIdMap, startingSequenceId);
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    // Since nothing is produced, so no changed events generated.
    verifyNoRecordsProduced(polledChangeEventsMap, polledChangeEventsList, statefulVeniceChangelogConsumer);

    // Create new version
    Properties props = defaultVPJProps(clusterWrapper, inputDirPath, storeName);
    IntegrationTestPushUtils.runVPJ(props);

    clusterWrapper.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2);
      });
    });

    // Change events should be from version 2 and 20 nearline events produced before
    startIndex = runNearlineJobAndVerifyConsumption(
        startIndex,
        storeName,
        2,
        polledChangeEventsMap,
        polledChangeEventsList,
        statefulVeniceChangelogConsumer,
        false,
        false);
    verifyVCCSequenceId(polledChangeEventsList, partitionSequenceIdMap, startingSequenceId);
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    // Do a repush
    props = defaultVPJProps(clusterWrapper, inputDirPath, storeName);
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, clusterWrapper.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    IntegrationTestPushUtils.runVPJ(props);

    clusterWrapper.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 3);
      });

      // Register new schema to verify it can deserialize records serialized with older schemas
      assertFalse(controllerClient.addValueSchema(storeName, TestChangelogValueV2.SCHEMA$.toString()).isError());
    });

    // Change events should be from version 3 and 20 nearline events produced before
    runNearlineJobAndVerifyConsumption(
        startIndex,
        storeName,
        3,
        polledChangeEventsMap,
        polledChangeEventsList,
        statefulVeniceChangelogConsumer,
        false,
        true);
    verifyVCCSequenceId(polledChangeEventsList, partitionSequenceIdMap, startingSequenceId);
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    // Test restart
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();
    statefulVeniceChangelogConsumer.stop();
    statefulVeniceChangelogConsumer.start().get();

    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          statefulVeniceChangelogConsumer);
      // 40 near-line put events, but one of them overwrites a key from batch push.
      // Also, Deletes won't show up on restart when scanning RocksDB.
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 39;
      assertEquals(polledChangeEventsList.size(), expectedRecordCount);
      verifyPut(polledChangeEventsMap, 100, 110, 3, false);
      verifyPut(polledChangeEventsMap, 120, 130, 3, false);
      verifyPut(polledChangeEventsMap, 140, 150, 3, false);
      verifyPut(polledChangeEventsMap, 160, 170, 3, false);
    });
    verifyVCCSequenceId(polledChangeEventsList, partitionSequenceIdMap, startingSequenceId);

    cleanUpStoreAndVerify(storeName);
  }

  /**
   * Verify VCC sequence id is monotonically increasing.
   * @param polledChangeEventsList to be verified
   * @param previousPartitionSequenceIdMap of previous consumption, if null it will be created
   * @param knownStartingSequenceId of the consumer, if -1 it will be inferred from the first message
   */
  private void verifyVCCSequenceId(
      List<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsList,
      Map<Integer, Long> previousPartitionSequenceIdMap,
      long knownStartingSequenceId) {
    if (polledChangeEventsList.isEmpty()) {
      return;
    }
    long startingSequenceId = knownStartingSequenceId < 0
        ? polledChangeEventsList.iterator().next().getPosition().getConsumerSequenceId()
        : knownStartingSequenceId;
    Map<Integer, Long> partitionSequenceIdMap =
        previousPartitionSequenceIdMap == null ? new HashMap<>() : previousPartitionSequenceIdMap;
    for (PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> message: polledChangeEventsList) {
      int partition = message.getPartition();
      long expectedSequenceId = partitionSequenceIdMap.computeIfAbsent(partition, k -> startingSequenceId);
      assertEquals(
          message.getPosition().getConsumerSequenceId(),
          expectedSequenceId,
          "Unexpected sequence id for partition: " + partition + ", starting sequence id: " + startingSequenceId);
      partitionSequenceIdMap.put(partition, expectedSequenceId + 1);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBlobTransferVeniceChangelogConsumerDaVinciRecordTransformerImpl() throws Exception {
    String storeName = Utils.getUniqueString("store");
    boolean useSpecificRecord = false;
    String inputDirPath1 = setUpStore(storeName);
    String inputDirPath2 = Utils.getTempDataDirectory().getAbsolutePath();
    int port1 = TestUtils.getFreePort();
    int port2 = TestUtils.getFreePort();
    while (port1 == port2) {
      port2 = TestUtils.getFreePort();
    }

    PubSubBrokerWrapper localKafka = clusterWrapper.getPubSubBrokerWrapper();
    String localKafkaUrl = localKafka.getAddress();
    Properties consumerProperties = new Properties();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, zkAddress);
    consumerProperties.put(BLOB_TRANSFER_MANAGER_ENABLED, true);
    consumerProperties.put(DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, port1);
    consumerProperties.put(DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, port2);
    consumerProperties.put(BLOB_TRANSFER_SSL_ENABLED, true);
    consumerProperties.put(BLOB_TRANSFER_ACL_ENABLED, true);

    String keyStorePath = SslUtils.getPathForResource(LOCAL_KEYSTORE_JKS);
    consumerProperties.put(SSL_KEYSTORE_TYPE, "JKS");
    consumerProperties.put(SSL_KEYSTORE_LOCATION, keyStorePath);
    consumerProperties.put(SSL_KEYSTORE_PASSWORD, LOCAL_PASSWORD);
    consumerProperties.put(SSL_TRUSTSTORE_TYPE, "JKS");
    consumerProperties.put(SSL_TRUSTSTORE_LOCATION, keyStorePath);
    consumerProperties.put(SSL_TRUSTSTORE_PASSWORD, LOCAL_PASSWORD);
    consumerProperties.put(SSL_KEY_PASSWORD, LOCAL_PASSWORD);
    consumerProperties.put(SSL_KEYMANAGER_ALGORITHM, "SunX509");
    consumerProperties.put(SSL_TRUSTMANAGER_ALGORITHM, "SunX509");
    consumerProperties.put(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG");
    consumerProperties.putAll(clusterWrapper.getPubSubClientProperties());

    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(zkAddress)
            .setControllerRequestRetryCount(3)
            .setBootstrapFileSystemPath(inputDirPath1)
            .setD2Client(d2Client);

    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    StatefulVeniceChangelogConsumer<GenericRecord, GenericRecord> statefulVeniceChangelogConsumer =
        veniceChangelogConsumerClientFactory.getStatefulChangelogConsumer(storeName);

    clusterWrapper.useControllerClient(controllerClient -> {
      // Register new schema to verify it scan deserialize records serialized with older schemas
      assertFalse(controllerClient.addValueSchema(storeName, TestChangelogValueV2.SCHEMA$.toString()).isError());
    });

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      veniceProducer.start();
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, 1, null, 10, 10, 100, true);
    }

    // Spin up a DVRT CDC instance and wait for it to consume everything, then perform blob transfer
    ForkedJavaProcess.exec(
        ChangelogConsumerDaVinciRecordTransformerUserApp.class,
        inputDirPath2,
        zkAddress,
        localKafkaUrl,
        clusterName,
        storeName,
        Integer.toString(port2),
        Integer.toString(port1),
        Integer.toString(DEFAULT_USER_DATA_RECORD_COUNT + 20),
        Boolean.toString(useSpecificRecord));
    Thread.sleep(30000);

    statefulVeniceChangelogConsumer.start().get();
    assertFalse(statefulVeniceChangelogConsumer.isCaughtUp());

    // Verify snapshots exists
    for (int i = 0; i < PARTITION_COUNT; i++) {
      String snapshotPath = RocksDBUtils.composeSnapshotDir(inputDirPath2 + "/rocksdb", storeName + "_v1", i);
      assertTrue(Files.exists(Paths.get(snapshotPath)));
    }

    Map<String, PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsMap =
        new HashMap<>();
    List<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsList =
        new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          statefulVeniceChangelogConsumer);
      // 20 changes in near-line. 10 puts, 10 deletes. But one of the puts overwrites a key from batch push, and the
      // 10 deletes are against non-existant keys. So there should only be 109 events total
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 9;
      assertEquals(polledChangeEventsList.size(), expectedRecordCount);
      verifyPut(polledChangeEventsMap, 100, 110, 1, false);
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(statefulVeniceChangelogConsumer.isCaughtUp());
    });

    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    // Since nothing is produced, so no changed events generated.
    verifyNoRecordsProduced(polledChangeEventsMap, polledChangeEventsList, statefulVeniceChangelogConsumer);

    runNearlineJobAndVerifyConsumption(
        120,
        storeName,
        1,
        polledChangeEventsMap,
        polledChangeEventsList,
        statefulVeniceChangelogConsumer,
        true,
        false);

    // Since nothing is produced, so no changed events generated.
    verifyNoRecordsProduced(polledChangeEventsMap, polledChangeEventsList, statefulVeniceChangelogConsumer);

    cleanUpStoreAndVerify(storeName);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSpecificRecordVeniceChangelogConsumerDaVinciRecordTransformerImpl() throws Exception {
    String storeName = Utils.getUniqueString("store");
    String inputDirPath = setUpStore(storeName);

    PubSubBrokerWrapper localKafka = clusterWrapper.getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(clusterWrapper.getPubSubClientProperties());
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, zkAddress);
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(zkAddress)
            .setControllerRequestRetryCount(3)
            .setBootstrapFileSystemPath(inputDirPath)
            .setD2Client(d2Client);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    StatefulVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue> statefulVeniceChangelogConsumer =
        veniceChangelogConsumerClientFactory.getStatefulChangelogConsumer(
            storeName,
            TestChangelogKey.class,
            TestChangelogValue.class,
            TestChangelogValue.SCHEMA$);

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, 1, null, 10, 10, 100, false);
    }

    statefulVeniceChangelogConsumer.start().get();
    assertFalse(statefulVeniceChangelogConsumer.isCaughtUp());

    Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap =
        new HashMap<>();
    List<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList =
        new ArrayList<>();
    // 20 changes in near-line. 10 puts, 10 deletes
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromSpecificChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          statefulVeniceChangelogConsumer);
      // 20 events for near-line events
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 20;
      assertEquals(polledChangeEventsList.size(), expectedRecordCount);
      verifySpecificPut(polledChangeEventsMap, 100, 110, 1);
      verifySpecificDelete(polledChangeEventsMap, 110, 120, 1);
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(statefulVeniceChangelogConsumer.isCaughtUp());
    });

    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    clusterWrapper.useControllerClient(controllerClient -> {
      // Register new schema to verify it scan deserialize records serialized with older schemas
      assertFalse(controllerClient.addValueSchema(storeName, TestChangelogValueV2.SCHEMA$.toString()).isError());
    });

    runSpecificNearlineJobAndVerifyConsumption(
        120,
        storeName,
        1,
        polledChangeEventsMap,
        polledChangeEventsList,
        statefulVeniceChangelogConsumer,
        true);

    // Since nothing is produced, so no changed events generated.
    verifyNoSpecificRecordsProduced(polledChangeEventsMap, polledChangeEventsList, statefulVeniceChangelogConsumer);

    cleanUpStoreAndVerify(storeName);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSpecificRecordBlobTransferVeniceChangelogConsumerDaVinciRecordTransformerImpl() throws Exception {
    String storeName = Utils.getUniqueString("store");
    boolean useSpecificRecord = true;
    String inputDirPath1 = setUpStore(storeName);
    String inputDirPath2 = Utils.getTempDataDirectory().getAbsolutePath();
    int port1 = TestUtils.getFreePort();
    int port2 = TestUtils.getFreePort();
    while (port1 == port2) {
      port2 = TestUtils.getFreePort();
    }

    PubSubBrokerWrapper localKafka = clusterWrapper.getPubSubBrokerWrapper();
    String localKafkaUrl = localKafka.getAddress();
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(clusterWrapper.getPubSubClientProperties());
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, zkAddress);
    consumerProperties.put(BLOB_TRANSFER_MANAGER_ENABLED, true);
    consumerProperties.put(DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, port1);
    consumerProperties.put(DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, port2);
    consumerProperties.put(BLOB_TRANSFER_SSL_ENABLED, true);
    consumerProperties.put(BLOB_TRANSFER_ACL_ENABLED, true);

    String keyStorePath = SslUtils.getPathForResource(LOCAL_KEYSTORE_JKS);
    consumerProperties.put(SSL_KEYSTORE_TYPE, "JKS");
    consumerProperties.put(SSL_KEYSTORE_LOCATION, keyStorePath);
    consumerProperties.put(SSL_KEYSTORE_PASSWORD, LOCAL_PASSWORD);
    consumerProperties.put(SSL_TRUSTSTORE_TYPE, "JKS");
    consumerProperties.put(SSL_TRUSTSTORE_LOCATION, keyStorePath);
    consumerProperties.put(SSL_TRUSTSTORE_PASSWORD, LOCAL_PASSWORD);
    consumerProperties.put(SSL_KEY_PASSWORD, LOCAL_PASSWORD);
    consumerProperties.put(SSL_KEYMANAGER_ALGORITHM, "SunX509");
    consumerProperties.put(SSL_TRUSTMANAGER_ALGORITHM, "SunX509");
    consumerProperties.put(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG");

    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(zkAddress)
            .setControllerRequestRetryCount(3)
            .setBootstrapFileSystemPath(inputDirPath1)
            .setD2Client(d2Client);

    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    StatefulVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue> statefulVeniceChangelogConsumer =
        veniceChangelogConsumerClientFactory.getStatefulChangelogConsumer(
            storeName,
            TestChangelogKey.class,
            TestChangelogValue.class,
            TestChangelogValue.SCHEMA$);

    clusterWrapper.useControllerClient(controllerClient -> {
      // Register new schema to verify it scan deserialize records serialized with older schemas
      assertFalse(controllerClient.addValueSchema(storeName, TestChangelogValueV2.SCHEMA$.toString()).isError());
    });

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      veniceProducer.start();
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, 1, null, 10, 10, 100, true);
    }

    // Spin up a DVRT CDC instance and wait for it to consume everything, then perform blob transfer
    ForkedJavaProcess.exec(
        ChangelogConsumerDaVinciRecordTransformerUserApp.class,
        inputDirPath2,
        zkAddress,
        localKafkaUrl,
        clusterName,
        storeName,
        Integer.toString(port2),
        Integer.toString(port1),
        Integer.toString(DEFAULT_USER_DATA_RECORD_COUNT + 20),
        Boolean.toString(useSpecificRecord));
    Thread.sleep(30000);

    statefulVeniceChangelogConsumer.start().get();
    assertFalse(statefulVeniceChangelogConsumer.isCaughtUp());

    // Verify snapshots exists
    for (int i = 0; i < PARTITION_COUNT; i++) {
      String snapshotPath = RocksDBUtils.composeSnapshotDir(inputDirPath2 + "/rocksdb", storeName + "_v1", i);
      assertTrue(Files.exists(Paths.get(snapshotPath)));
    }

    Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap =
        new HashMap<>();
    List<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList =
        new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromSpecificChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          statefulVeniceChangelogConsumer);
      // 20 changes in near-line. 10 puts, 10 deletes. But one of the puts overwrites a key from batch push, and the
      // 10 deletes are against non-existant keys. So there should only be 109 events total
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 9;
      assertEquals(polledChangeEventsList.size(), expectedRecordCount);
      verifySpecificPut(polledChangeEventsMap, 100, 110, 1);
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(statefulVeniceChangelogConsumer.isCaughtUp());
    });

    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    // Since nothing is produced, so no changed events generated.
    verifyNoSpecificRecordsProduced(polledChangeEventsMap, polledChangeEventsList, statefulVeniceChangelogConsumer);

    runSpecificNearlineJobAndVerifyConsumption(
        120,
        storeName,
        1,
        polledChangeEventsMap,
        polledChangeEventsList,
        statefulVeniceChangelogConsumer,
        true);

    // Since nothing is produced, so no changed events generated.
    verifyNoSpecificRecordsProduced(polledChangeEventsMap, polledChangeEventsList, statefulVeniceChangelogConsumer);

    cleanUpStoreAndVerify(storeName);
  }

  public static void pollChangeEventsFromChangeCaptureConsumer(
      Map<String, PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> keyToMessageMap,
      List<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledMessageList,
      StatefulVeniceChangelogConsumer<GenericRecord, GenericRecord> statefulVeniceChangelogConsumer) {
    Collection<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> pubSubMessages =
        statefulVeniceChangelogConsumer.poll(1000);
    for (PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      String key = pubSubMessage.getKey() == null ? null : String.valueOf(pubSubMessage.getKey().get("id"));
      keyToMessageMap.put(key, pubSubMessage);
    }
    polledMessageList.addAll(pubSubMessages);
  }

  public static void pollChangeEventsFromSpecificChangeCaptureConsumer(
      Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> keyToMessageMap,
      List<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledMessageList,
      StatefulVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue> statefulVeniceChangelogConsumer) {
    Collection<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> pubSubMessages =
        statefulVeniceChangelogConsumer.poll(1000);
    for (PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      String key = pubSubMessage.getKey() == null ? null : String.valueOf(pubSubMessage.getKey().id);
      keyToMessageMap.put(key, pubSubMessage);
    }
    polledMessageList.addAll(pubSubMessages);
  }

  private void runSamzaStreamJob(
      SystemProducer veniceProducer,
      String storeName,
      int version,
      Time mockedTime,
      int numPuts,
      int numDels,
      int startIdx,
      boolean useEvolvedSchema) {
    // Send PUT requests.
    for (int i = startIdx; i < startIdx + numPuts; i++) {
      TestChangelogKey key = new TestChangelogKey();
      key.id = i;

      Object valueObject;

      if (useEvolvedSchema) {
        TestChangelogValueV2 value = new TestChangelogValueV2();
        value.firstName = "first_name_stream_" + i;
        value.lastName = "last_name_stream_" + i;
        value.version = version;

        valueObject = value;
      } else {
        TestChangelogValue value = new TestChangelogValue();
        value.firstName = "first_name_stream_" + i;
        value.lastName = "last_name_stream_" + i;

        valueObject = value;
      }

      sendStreamingRecord(
          veniceProducer,
          storeName,
          key,
          valueObject,
          mockedTime == null ? null : mockedTime.getMilliseconds());
    }

    // Send DELETE requests.
    for (int i = startIdx + numPuts; i < startIdx + numPuts + numDels; i++) {
      TestChangelogKey key = new TestChangelogKey();
      key.id = i;

      sendStreamingDeleteRecord(
          veniceProducer,
          storeName,
          key,
          mockedTime == null ? null : mockedTime.getMilliseconds());
    }
  }

  /**
   * @param storeName the name of the store
   * @return the path that's being used for the test
   */
  private String setUpStore(String storeName) throws Exception {
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(clusterWrapper, inputDirPath, storeName);

    Schema recordSchema = new PushInputSchemaBuilder().setKeySchema(TestChangelogKey.SCHEMA$)
        .setValueSchema(TestChangelogValue.SCHEMA$)
        .build();

    writeSimpleAvroFile(inputDir, recordSchema, i -> {
      GenericRecord keyValueRecord = new GenericData.Record(recordSchema);
      TestChangelogKey key = new TestChangelogKey();
      key.id = i;
      keyValueRecord.put(DEFAULT_KEY_FIELD_PROP, key);

      TestChangelogValue value = new TestChangelogValue();
      value.firstName = "first_name" + i;
      value.lastName = "last_name" + i;
      keyValueRecord.put(DEFAULT_VALUE_FIELD_PROP, value);

      return keyValueRecord;
    }, DEFAULT_USER_DATA_RECORD_COUNT);

    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setPartitionCount(PARTITION_COUNT)
        .setBlobTransferEnabled(true);

    try (ControllerClient controllerClient =
        createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms)) {
      clusterWrapper.createMetaSystemStore(storeName);
      clusterWrapper.createPushStatusSystemStore(storeName);
      TestUtils.assertCommand(controllerClient.updateStore(storeName, storeParms));
      TestUtils.assertCommand(controllerClient.addValueSchema(storeName, valueSchemaStr));
      IntegrationTestPushUtils.runVPJ(props);
    }

    return inputDirPath;
  }

  /**
   * @return the end index
   */
  private int runNearlineJobAndVerifyConsumption(
      int startIndex,
      String storeName,
      int version,
      Map<String, PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsMap,
      List<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsList,
      StatefulVeniceChangelogConsumer<GenericRecord, GenericRecord> statefulVeniceChangelogConsumer,
      boolean clearConsumedRecords,
      boolean useEvolvedSchema) {
    // Half puts and half deletes
    int recordsToProduce = 20;
    int numPuts = recordsToProduce / 2;
    int numDeletes = recordsToProduce / 2;

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, version, null, numPuts, numDeletes, startIndex, useEvolvedSchema);
    }

    try (AvroGenericStoreClient<GenericRecord, GenericRecord> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        TestChangelogKey key = new TestChangelogKey();
        key.id = startIndex + numPuts - 1;
        assertNotNull(client.get(key).get());
      });
    }

    // 20 changes in near-line. 10 puts, 10 deletes
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          statefulVeniceChangelogConsumer);
      assertEquals(polledChangeEventsMap.size(), recordsToProduce);

      verifyPut(polledChangeEventsMap, startIndex, startIndex + numPuts, version, useEvolvedSchema);
      verifyDelete(polledChangeEventsMap, startIndex + numPuts, startIndex + numDeletes, version);
    });
    if (clearConsumedRecords) {
      polledChangeEventsList.clear();
      polledChangeEventsMap.clear();
    }

    return startIndex + recordsToProduce;
  }

  /**
   * @return the end index
   */
  private int runSpecificNearlineJobAndVerifyConsumption(
      int startIndex,
      String storeName,
      int version,
      Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap,
      List<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList,
      StatefulVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue> statefulVeniceChangelogConsumer,
      boolean useEvolvedSchema) {
    // Half puts and half deletes
    int recordsToProduce = 20;
    int numPuts = recordsToProduce / 2;
    int numDeletes = recordsToProduce / 2;

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, version, null, numPuts, numDeletes, startIndex, useEvolvedSchema);
    }

    try (AvroGenericStoreClient<TestChangelogKey, TestChangelogValue> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(storeName, TestChangelogValue.class)
                .setVeniceURL(clusterWrapper.getRandomRouterURL())
                .setMetricsRepository(metricsRepository)
                .setUseFastAvro(true))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        TestChangelogKey key = new TestChangelogKey();
        key.id = startIndex + numPuts - 1;

        assertNotNull(client.get(key).get());
      });
    }

    // 20 changes in near-line. 10 puts, 10 deletes
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromSpecificChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          statefulVeniceChangelogConsumer);
      assertEquals(polledChangeEventsMap.size(), recordsToProduce);

      verifySpecificPut(polledChangeEventsMap, startIndex, startIndex + numPuts, version);
      verifySpecificDelete(polledChangeEventsMap, startIndex + numPuts, startIndex + numDeletes, version);
    });
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    return startIndex + recordsToProduce;
  }

  private void verifyPut(
      Map<String, PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsMap,
      int startIndex,
      int endIndex,
      int version,
      boolean useEvolvedSchema) {
    for (int i = startIndex; i < endIndex; i++) {
      String key = Integer.toString(i);
      PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> message =
          polledChangeEventsMap.get((key));
      ChangeEvent<GenericRecord> changeEvent = message.getValue();
      int versionFromMessage = Version.parseVersionFromVersionTopicName(message.getTopicPartition().getTopicName());
      assertEquals(versionFromMessage, version);
      assertNotNull(changeEvent);
      assertNull(changeEvent.getPreviousValue());

      GenericRecord value = changeEvent.getCurrentValue();
      assertEquals(value.get("firstName").toString(), "first_name_stream_" + i);
      assertEquals(value.get("lastName").toString(), "last_name_stream_" + i);

      if (useEvolvedSchema) {
        assertEquals(value.get("version"), version);
      }
    }
  }

  private void verifySpecificPut(
      Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap,
      int startIndex,
      int endIndex,
      int version) {
    for (int i = startIndex; i < endIndex; i++) {
      String key = Integer.toString(i);
      PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate> message =
          polledChangeEventsMap.get((key));
      ChangeEvent<TestChangelogValue> changeEvent = message.getValue();
      int versionFromMessage = Version.parseVersionFromVersionTopicName(message.getTopicPartition().getTopicName());
      assertEquals(versionFromMessage, version);
      assertNotNull(changeEvent);
      assertNull(changeEvent.getPreviousValue());

      TestChangelogValue value = changeEvent.getCurrentValue();
      assertEquals(value.firstName.toString(), "first_name_stream_" + i);
      assertEquals(value.lastName.toString(), "last_name_stream_" + i);
    }
  }

  private void verifyDelete(
      Map<String, PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsMap,
      int startIndex,
      int endIndex,
      int version) {
    for (int i = startIndex; i < endIndex; i++) {
      String key = Integer.toString(i);
      PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate> message =
          polledChangeEventsMap.get((key));
      ChangeEvent<GenericRecord> changeEvent = message.getValue();
      int versionFromMessage = Version.parseVersionFromVersionTopicName(message.getTopicPartition().getTopicName());
      assertEquals(versionFromMessage, version);
      assertNotNull(changeEvent);
      assertNull(changeEvent.getPreviousValue());
      assertNull(changeEvent.getCurrentValue());
    }
  }

  private void verifySpecificDelete(
      Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap,
      int startIndex,
      int endIndex,
      int version) {
    for (int i = startIndex; i < endIndex; i++) {
      PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate> message =
          polledChangeEventsMap.get((Integer.toString(i)));

      TestChangelogKey key = message.getKey();
      assertEquals(key.id, i);

      ChangeEvent<TestChangelogValue> changeEvent = message.getValue();
      int versionFromMessage = Version.parseVersionFromVersionTopicName(message.getTopicPartition().getTopicName());
      assertEquals(versionFromMessage, version);
      assertNotNull(changeEvent);
      assertNull(changeEvent.getPreviousValue());
      assertNull(changeEvent.getCurrentValue());
    }
  }

  private void verifyNoRecordsProduced(
      Map<String, PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsMap,
      List<PubSubMessage<GenericRecord, ChangeEvent<GenericRecord>, VeniceChangeCoordinate>> polledChangeEventsList,
      StatefulVeniceChangelogConsumer<GenericRecord, GenericRecord> statefulVeniceChangelogConsumer) {
    pollChangeEventsFromChangeCaptureConsumer(
        polledChangeEventsMap,
        polledChangeEventsList,
        statefulVeniceChangelogConsumer);
    assertEquals(polledChangeEventsList.size(), 0);
  }

  private void verifyNoSpecificRecordsProduced(
      Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap,
      List<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList,
      StatefulVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue> statefulVeniceChangelogConsumer) {
    pollChangeEventsFromSpecificChangeCaptureConsumer(
        polledChangeEventsMap,
        polledChangeEventsList,
        statefulVeniceChangelogConsumer);
    assertEquals(polledChangeEventsList.size(), 0);
  }

  private void cleanUpStoreAndVerify(String storeName) {
    clusterWrapper.useControllerClient(controllerClient -> {
      // Verify that topics and store is cleaned up
      controllerClient.disableAndDeleteStore(storeName);

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        MultiStoreTopicsResponse storeTopicsResponse = controllerClient.getDeletableStoreTopics();
        assertFalse(storeTopicsResponse.isError());
        assertEquals(storeTopicsResponse.getTopics().size(), 0);
      });
    });
  }
}
