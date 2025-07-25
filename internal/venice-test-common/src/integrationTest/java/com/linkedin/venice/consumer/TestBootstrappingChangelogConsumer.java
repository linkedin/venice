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
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecordWithLogicalTimestamp;
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
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.consumer.BootstrappingVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
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
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PushInputSchemaBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestBootstrappingChangelogConsumer {
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;
  private static final int PARTITION_COUNT = 3;

  // Use a unique key for DELETE with RMD validation
  private static final int deleteWithRmdKeyIndex = 1000;
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
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    clusterWrapper.close();
    TestView.resetCounters();
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "changelogConsumer", dataProviderClass = DataProviderUtils.class)
  public void testVeniceChangelogConsumer(int consumerCount) throws Exception {
    String storeName = Utils.getUniqueString("store");
    String inputDirPath = setUpStore(storeName, false);

    PubSubBrokerWrapper localKafka = clusterWrapper.getPubSubBrokerWrapper();
    String localKafkaUrl = localKafka.getAddress();
    Properties consumerProperties = new Properties();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, zkAddress);
    consumerProperties.putAll(clusterWrapper.getPubSubClientProperties());
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setD2Client(d2Client)
            .setLocalD2ZkHosts(zkAddress)
            .setControllerRequestRetryCount(3)
            .setBootstrapFileSystemPath(Utils.getUniqueString(inputDirPath));
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList = new ArrayList<>();
    for (int i = 0; i < consumerCount; i++) {
      bootstrappingVeniceChangelogConsumerList
          .add(veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(storeName, Integer.toString(i)));
    }

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100, false);
      // Produce a DELETE record with large timestamp
      sendStreamingRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
    }

    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNull(client.get(Integer.toString(deleteWithRmdKeyIndex)).get());
      });
    }

    // Wait for 10 seconds so bootstrap can load results from kafka
    Utils.sleep(10000);
    if (consumerCount == 1) {
      bootstrappingVeniceChangelogConsumerList.get(0).start().get();
    } else {
      for (int i = 0; i < consumerCount; i++) {
        bootstrappingVeniceChangelogConsumerList.get(i).start(Collections.singleton(i)).get();
      }
    }

    Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap = new HashMap<>();
    List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsList = new ArrayList<>();
    // 21 changes in near-line. 10 puts, 10 deletes, and 1 record with a producer timestamp
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);
      // 21 events for near-line events, but the 10 deletes are not returned due to compaction.
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 9 + consumerCount;
      Assert.assertEquals(polledChangeEventsList.size(), expectedRecordCount);

      verifyPut(polledChangeEventsMap, 100, 110, 1);

      // Verify the 10 deletes were compacted away
      for (int i = 110; i < 120; i++) {
        String key = Integer.toString(i);
        PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> message = polledChangeEventsMap.get((key));
        Assert.assertNull(message);
      }
    });
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    runNearlineJobAndVerifyConsumption(
        120,
        storeName,
        1,
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);

    // Since nothing is produced, so no changed events generated.
    verifyNoRecordsProduced(polledChangeEventsMap, polledChangeEventsList, bootstrappingVeniceChangelogConsumerList);

    VeniceChangelogConsumer<Utf8, Utf8> afterImageChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName);
    afterImageChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0, 1, 2))).get();

    List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> changedEventList = new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumerToList(changedEventList, afterImageChangelogConsumer);
      Assert.assertEquals(changedEventList.size(), 141);
    });

    cleanUpStoreAndVerify(storeName);
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testVeniceChangelogConsumerDaVinciRecordTransformerImpl() throws Exception {
    String storeName = Utils.getUniqueString("store");
    boolean useSpecificRecord = false;
    String inputDirPath = setUpStore(storeName, useSpecificRecord);

    PubSubBrokerWrapper localKafka = clusterWrapper.getPubSubBrokerWrapper();
    Properties consumerProperties = new Properties();
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(CLUSTER_NAME, clusterName);
    consumerProperties.put(ZOOKEEPER_ADDRESS, zkAddress);
    consumerProperties.putAll(clusterWrapper.getPubSubClientProperties());
    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setLocalD2ZkHosts(zkAddress)
            .setControllerRequestRetryCount(3)
            .setBootstrapFileSystemPath(inputDirPath)
            .setIsExperimentalClientEnabled(true)
            .setD2Client(d2Client)
            // Setting the max buffer size to a low threshold to ensure puts to the buffer get blocked and drained
            // correctly during regular operation and restarts
            .setMaxBufferSize(10);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList =
        Collections.singletonList(
            veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(storeName, Integer.toString(0)));

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100, useSpecificRecord);
      // Produce a DELETE record with large timestamp
      sendStreamingRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
    }

    bootstrappingVeniceChangelogConsumerList.get(0).start().get();
    assertFalse(bootstrappingVeniceChangelogConsumerList.get(0).isCaughtUp());

    Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap = new HashMap<>();
    List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsList = new ArrayList<>();
    // 21 changes in near-line. 10 puts, 10 deletes, and 1 record with a producer timestamp
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);
      // 21 events for near-line events
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 21;
      Assert.assertEquals(polledChangeEventsList.size(), expectedRecordCount);
      verifyPut(polledChangeEventsMap, 100, 110, 1);
      verifyDelete(polledChangeEventsMap, 110, 120, 1);
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(bootstrappingVeniceChangelogConsumerList.get(0).isCaughtUp());
    });

    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    int startIndex = runNearlineJobAndVerifyConsumption(
        120,
        storeName,
        1,
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);

    // Since nothing is produced, so no changed events generated.
    verifyNoRecordsProduced(polledChangeEventsMap, polledChangeEventsList, bootstrappingVeniceChangelogConsumerList);

    // Create new version
    Properties props = defaultVPJProps(clusterWrapper, inputDirPath, storeName);
    IntegrationTestPushUtils.runVPJ(props);

    clusterWrapper.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2);
      });
    });

    // Change events should be from version 2 and 20 nearline events produced before
    startIndex = runNearlineJobAndVerifyConsumption(
        startIndex,
        storeName,
        2,
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);

    // Do a repush
    props = defaultVPJProps(clusterWrapper, inputDirPath, storeName);
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, clusterWrapper.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    IntegrationTestPushUtils.runVPJ(props);

    clusterWrapper.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 3);
      });
    });

    // Change events should be from version 3 and 20 nearline events produced before
    runNearlineJobAndVerifyConsumption(
        startIndex,
        storeName,
        3,
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);

    // Test restart
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();
    bootstrappingVeniceChangelogConsumerList.get(0).stop();
    bootstrappingVeniceChangelogConsumerList.get(0).start().get();

    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);
      // 40 near-line put events, but one of them overwrites a key from batch push.
      // Also, Deletes won't show up on restart when scanning RocksDB.
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 39;
      Assert.assertEquals(polledChangeEventsList.size(), expectedRecordCount);
      verifyPut(polledChangeEventsMap, 100, 110, 3);
      verifyPut(polledChangeEventsMap, 120, 130, 3);
      verifyPut(polledChangeEventsMap, 140, 150, 3);
      verifyPut(polledChangeEventsMap, 160, 170, 3);
    });

    cleanUpStoreAndVerify(storeName);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBlobTransferVeniceChangelogConsumerDaVinciRecordTransformerImpl() throws Exception {
    String storeName = Utils.getUniqueString("store");
    boolean useSpecificRecord = false;
    String inputDirPath1 = setUpStore(storeName, useSpecificRecord);
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
            .setD2Client(d2Client)
            .setIsExperimentalClientEnabled(true);

    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList =
        Collections.singletonList(
            veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(storeName, Integer.toString(0)));

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      veniceProducer.start();
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100, useSpecificRecord);
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

    bootstrappingVeniceChangelogConsumerList.get(0).start().get();
    assertFalse(bootstrappingVeniceChangelogConsumerList.get(0).isCaughtUp());

    // Verify snapshots exists
    for (int i = 0; i < PARTITION_COUNT; i++) {
      String snapshotPath = RocksDBUtils.composeSnapshotDir(inputDirPath2 + "/rocksdb", storeName + "_v1", i);
      Assert.assertTrue(Files.exists(Paths.get(snapshotPath)));
    }

    Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap = new HashMap<>();
    List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsList = new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);
      // 20 changes in near-line. 10 puts, 10 deletes. But one of the puts overwrites a key from batch push, and the
      // 10 deletes are against non-existant keys. So there should only be 109 events total
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 9;
      Assert.assertEquals(polledChangeEventsList.size(), expectedRecordCount);
      verifyPut(polledChangeEventsMap, 100, 110, 1);
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(bootstrappingVeniceChangelogConsumerList.get(0).isCaughtUp());
    });

    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    // Since nothing is produced, so no changed events generated.
    verifyNoRecordsProduced(polledChangeEventsMap, polledChangeEventsList, bootstrappingVeniceChangelogConsumerList);

    runNearlineJobAndVerifyConsumption(
        120,
        storeName,
        1,
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);

    // Since nothing is produced, so no changed events generated.
    verifyNoRecordsProduced(polledChangeEventsMap, polledChangeEventsList, bootstrappingVeniceChangelogConsumerList);

    cleanUpStoreAndVerify(storeName);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSpecificRecordVeniceChangelogConsumerDaVinciRecordTransformerImpl() throws Exception {
    String storeName = Utils.getUniqueString("store");
    boolean useSpecificRecord = true;
    String inputDirPath = setUpStore(storeName, useSpecificRecord);

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
            .setIsExperimentalClientEnabled(true)
            .setD2Client(d2Client);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    List<BootstrappingVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue>> bootstrappingVeniceChangelogConsumerList =
        Collections.singletonList(
            veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(
                storeName,
                Integer.toString(0),
                TestChangelogKey.class,
                TestChangelogValue.class,
                TestChangelogValue.SCHEMA$));

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100, useSpecificRecord);
    }

    bootstrappingVeniceChangelogConsumerList.get(0).start().get();
    assertFalse(bootstrappingVeniceChangelogConsumerList.get(0).isCaughtUp());

    Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap =
        new HashMap<>();
    List<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList =
        new ArrayList<>();
    // 20 changes in near-line. 10 puts, 10 deletes
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromSpecificChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);
      // 20 events for near-line events
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 20;
      Assert.assertEquals(polledChangeEventsList.size(), expectedRecordCount);
      verifySpecificPut(polledChangeEventsMap, 100, 110, 1);
      verifySpecificDelete(polledChangeEventsMap, 110, 120, 1);
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(bootstrappingVeniceChangelogConsumerList.get(0).isCaughtUp());
    });

    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    runSpecificNearlineJobAndVerifyConsumption(
        120,
        storeName,
        1,
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);

    // Since nothing is produced, so no changed events generated.
    verifyNoSpecificRecordsProduced(
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);

    cleanUpStoreAndVerify(storeName);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSpecificRecordBlobTransferVeniceChangelogConsumerDaVinciRecordTransformerImpl() throws Exception {
    String storeName = Utils.getUniqueString("store");
    boolean useSpecificRecord = true;
    String inputDirPath1 = setUpStore(storeName, useSpecificRecord);
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
            .setD2Client(d2Client)
            .setIsExperimentalClientEnabled(true);

    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);
    List<BootstrappingVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue>> bootstrappingVeniceChangelogConsumerList =
        Collections.singletonList(
            veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(
                storeName,
                Integer.toString(0),
                TestChangelogKey.class,
                TestChangelogValue.class,
                TestChangelogValue.SCHEMA$));

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      veniceProducer.start();
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 100, useSpecificRecord);
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

    bootstrappingVeniceChangelogConsumerList.get(0).start().get();
    assertFalse(bootstrappingVeniceChangelogConsumerList.get(0).isCaughtUp());

    // Verify snapshots exists
    for (int i = 0; i < PARTITION_COUNT; i++) {
      String snapshotPath = RocksDBUtils.composeSnapshotDir(inputDirPath2 + "/rocksdb", storeName + "_v1", i);
      Assert.assertTrue(Files.exists(Paths.get(snapshotPath)));
    }

    Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap =
        new HashMap<>();
    List<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList =
        new ArrayList<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromSpecificChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);
      // 20 changes in near-line. 10 puts, 10 deletes. But one of the puts overwrites a key from batch push, and the
      // 10 deletes are against non-existant keys. So there should only be 109 events total
      int expectedRecordCount = DEFAULT_USER_DATA_RECORD_COUNT + 9;
      Assert.assertEquals(polledChangeEventsList.size(), expectedRecordCount);
      verifySpecificPut(polledChangeEventsMap, 100, 110, 1);
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(bootstrappingVeniceChangelogConsumerList.get(0).isCaughtUp());
    });

    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    // Since nothing is produced, so no changed events generated.
    verifyNoSpecificRecordsProduced(
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);

    runSpecificNearlineJobAndVerifyConsumption(
        120,
        storeName,
        1,
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);

    // Since nothing is produced, so no changed events generated.
    verifyNoSpecificRecordsProduced(
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);

    cleanUpStoreAndVerify(storeName);
  }

  private void pollChangeEventsFromChangeCaptureConsumerToList(
      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEvents,
      VeniceChangelogConsumer<Utf8, Utf8> veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        veniceChangelogConsumer.poll(1000);
    polledChangeEvents.addAll(pubSubMessages);
  }

  private void pollChangeEventsFromChangeCaptureConsumer(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> keyToMessageMap,
      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledMessageList,
      List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList) {
    for (BootstrappingVeniceChangelogConsumer<Utf8, Utf8> bootstrappingVeniceChangelogConsumer: bootstrappingVeniceChangelogConsumerList) {
      Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
          bootstrappingVeniceChangelogConsumer.poll(1000);
      for (PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
        String key = pubSubMessage.getKey() == null ? null : pubSubMessage.getKey().toString();
        keyToMessageMap.put(key, pubSubMessage);
      }
      polledMessageList.addAll(pubSubMessages);
    }
  }

  private void pollChangeEventsFromSpecificChangeCaptureConsumer(
      Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> keyToMessageMap,
      List<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledMessageList,
      List<BootstrappingVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue>> bootstrappingVeniceChangelogConsumerList) {
    for (BootstrappingVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue> bootstrappingVeniceChangelogConsumer: bootstrappingVeniceChangelogConsumerList) {
      Collection<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> pubSubMessages =
          bootstrappingVeniceChangelogConsumer.poll(1000);
      for (PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
        String key = pubSubMessage.getKey() == null ? null : String.valueOf(pubSubMessage.getKey().id);
        keyToMessageMap.put(key, pubSubMessage);
      }
      polledMessageList.addAll(pubSubMessages);
    }
  }

  private void runSamzaStreamJob(
      SystemProducer veniceProducer,
      String storeName,
      Time mockedTime,
      int numPuts,
      int numDels,
      int startIdx,
      boolean useSpecificRecord) {
    // Send PUT requests.
    for (int i = startIdx; i < startIdx + numPuts; i++) {
      if (useSpecificRecord) {
        TestChangelogKey key = new TestChangelogKey();
        key.id = i;

        TestChangelogValue value = new TestChangelogValue();
        value.firstName = "first_name_stream_" + i;
        value.lastName = "last_name_stream_" + i;

        sendStreamingRecord(
            veniceProducer,
            storeName,
            key,
            value,
            mockedTime == null ? null : mockedTime.getMilliseconds());
      } else {
        sendStreamingRecord(
            veniceProducer,
            storeName,
            Integer.toString(i),
            "stream_" + i,
            mockedTime == null ? null : mockedTime.getMilliseconds());
      }
    }
    // Send DELETE requests.
    for (int i = startIdx + numPuts; i < startIdx + numPuts + numDels; i++) {
      if (useSpecificRecord) {
        TestChangelogKey key = new TestChangelogKey();
        key.id = i;

        sendStreamingDeleteRecord(
            veniceProducer,
            storeName,
            key,
            mockedTime == null ? null : mockedTime.getMilliseconds());
      } else {
        sendStreamingDeleteRecord(
            veniceProducer,
            storeName,
            Integer.toString(i),
            mockedTime == null ? null : mockedTime.getMilliseconds());
      }

    }
  }

  /**
   * @param storeName the name of the store
   * @param useSpecificRecord Whether to push data using a specific record
   * @return the path that's being used for the test
   */
  private String setUpStore(String storeName, boolean useSpecificRecord) throws Exception {
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(clusterWrapper, inputDirPath, storeName);

    Schema recordSchema;
    String keySchemaStr;
    String valueSchemaStr;
    if (useSpecificRecord) {
      recordSchema = new PushInputSchemaBuilder().setKeySchema(TestChangelogKey.SCHEMA$)
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
    } else {
      recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    }

    keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

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
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap,
      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsList,
      List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList) {
    // Half puts and half deletes
    int recordsToProduce = 20;
    int numPuts = recordsToProduce / 2;
    int numDeletes = recordsToProduce / 2;

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, numPuts, numDeletes, startIndex, false);
    }

    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNotNull(client.get(Integer.toString(startIndex + numPuts - 1)).get());
      });
    }

    // 20 changes in near-line. 10 puts, 10 deletes
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);
      Assert.assertEquals(polledChangeEventsMap.size(), recordsToProduce);

      verifyPut(polledChangeEventsMap, startIndex, startIndex + numPuts, version);
      verifyDelete(polledChangeEventsMap, startIndex + numPuts, startIndex + numDeletes, version);
    });
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

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
      List<BootstrappingVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue>> bootstrappingVeniceChangelogConsumerList) {
    // Half puts and half deletes
    int recordsToProduce = 20;
    int numPuts = recordsToProduce / 2;
    int numDeletes = recordsToProduce / 2;

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM)) {
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, numPuts, numDeletes, startIndex, true);
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

        Assert.assertNotNull(client.get(key).get());
      });
    }

    // 20 changes in near-line. 10 puts, 10 deletes
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromSpecificChangeCaptureConsumer(
          polledChangeEventsMap,
          polledChangeEventsList,
          bootstrappingVeniceChangelogConsumerList);
      Assert.assertEquals(polledChangeEventsMap.size(), recordsToProduce);

      verifySpecificPut(polledChangeEventsMap, startIndex, startIndex + numPuts, version);
      verifySpecificDelete(polledChangeEventsMap, startIndex + numPuts, startIndex + numDeletes, version);
    });
    polledChangeEventsList.clear();
    polledChangeEventsMap.clear();

    return startIndex + recordsToProduce;
  }

  private void verifyPut(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap,
      int startIndex,
      int endIndex,
      int version) {
    for (int i = startIndex; i < endIndex; i++) {
      String key = Integer.toString(i);
      PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> message = polledChangeEventsMap.get((key));
      ChangeEvent<Utf8> changeEvent = message.getValue();
      int versionFromMessage = Version.parseVersionFromVersionTopicName(message.getTopicPartition().getTopicName());
      Assert.assertEquals(versionFromMessage, version);
      Assert.assertNotNull(changeEvent);
      Assert.assertNull(changeEvent.getPreviousValue());
      Assert.assertEquals(changeEvent.getCurrentValue().toString(), "stream_" + i);
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
      Assert.assertEquals(versionFromMessage, version);
      Assert.assertNotNull(changeEvent);
      Assert.assertNull(changeEvent.getPreviousValue());

      TestChangelogValue value = changeEvent.getCurrentValue();
      Assert.assertEquals(value.firstName.toString(), "first_name_stream_" + i);
      Assert.assertEquals(value.lastName.toString(), "last_name_stream_" + i);
    }
  }

  private void verifyDelete(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap,
      int startIndex,
      int endIndex,
      int version) {
    for (int i = startIndex; i < endIndex; i++) {
      String key = Integer.toString(i);
      PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> message = polledChangeEventsMap.get((key));
      ChangeEvent<Utf8> changeEvent = message.getValue();
      int versionFromMessage = Version.parseVersionFromVersionTopicName(message.getTopicPartition().getTopicName());
      Assert.assertEquals(versionFromMessage, version);
      Assert.assertNotNull(changeEvent);
      Assert.assertNull(changeEvent.getPreviousValue());
      Assert.assertNull(changeEvent.getCurrentValue());
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
      Assert.assertEquals(versionFromMessage, version);
      Assert.assertNotNull(changeEvent);
      Assert.assertNull(changeEvent.getPreviousValue());
      Assert.assertNull(changeEvent.getCurrentValue());
    }
  }

  private void verifyNoRecordsProduced(
      Map<String, PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsMap,
      List<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> polledChangeEventsList,
      List<BootstrappingVeniceChangelogConsumer<Utf8, Utf8>> bootstrappingVeniceChangelogConsumerList) {
    pollChangeEventsFromChangeCaptureConsumer(
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);
    Assert.assertEquals(polledChangeEventsList.size(), 0);
  }

  private void verifyNoSpecificRecordsProduced(
      Map<String, PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsMap,
      List<PubSubMessage<TestChangelogKey, ChangeEvent<TestChangelogValue>, VeniceChangeCoordinate>> polledChangeEventsList,
      List<BootstrappingVeniceChangelogConsumer<TestChangelogKey, TestChangelogValue>> bootstrappingVeniceChangelogConsumerList) {
    pollChangeEventsFromSpecificChangeCaptureConsumer(
        polledChangeEventsMap,
        polledChangeEventsList,
        bootstrappingVeniceChangelogConsumerList);
    Assert.assertEquals(polledChangeEventsList.size(), 0);
  }

  private void cleanUpStoreAndVerify(String storeName) {
    clusterWrapper.useControllerClient(controllerClient -> {
      // Verify that topics and store is cleaned up
      controllerClient.disableAndDeleteStore(storeName);

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        MultiStoreTopicsResponse storeTopicsResponse = controllerClient.getDeletableStoreTopics();
        Assert.assertFalse(storeTopicsResponse.isError());
        Assert.assertEquals(storeTopicsResponse.getTopics().size(), 0);
      });
    });
  }
}
