package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_INSTANCE_NAME_SUFFIX;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_INCREMENTAL_PUSH_STATUS_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.USE_PUSH_STATUS_STORE_FOR_INCREMENTAL_PUSH;
import static com.linkedin.venice.common.PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.config.VeniceServerConfig.IncrementalPushStatusWriteMode;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PushStatusStoreTest {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusStoreTest.class);
  private static final int TEST_TIMEOUT_MS = 60_000;
  private static final int NUMBER_OF_SERVERS = 2;
  private static final int PARTITION_COUNT = 2;
  private static final int REPLICATION_FACTOR = 2;

  private VeniceClusterWrapper cluster;
  private ControllerClient controllerClient;
  private D2Client d2Client;
  private PushStatusStoreReader reader;
  private String storeName;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass
  public void setUp() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    // all tests in this class will be reading incremental push status from push status store
    extraProperties.setProperty(USE_PUSH_STATUS_STORE_FOR_INCREMENTAL_PUSH, String.valueOf(true));
    extraProperties.setProperty(
        SERVER_INCREMENTAL_PUSH_STATUS_WRITE_MODE,
        IncrementalPushStatusWriteMode.PUSH_STATUS_SYSTEM_STORE_ONLY.toString());

    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfServers(NUMBER_OF_SERVERS)
            .replicationFactor(REPLICATION_FACTOR)
            .partitionSize(10000)
            .extraProperties(extraProperties)
            .build());
    controllerClient = cluster.getControllerClient();
    d2Client = D2TestUtils.getAndStartD2Client(cluster.getZk().getAddress());
    reader = new PushStatusStoreReader(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        TimeUnit.MINUTES.toSeconds(10));
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(reader);
    D2ClientUtils.shutdownClient(d2Client);
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @BeforeMethod
  public void setUpStore() {
    storeName = createStoreAndSystemStores();
  }

  private String createStoreAndSystemStores() {
    String storeName = Utils.getUniqueString("store");
    TestUtils.assertCommand(controllerClient.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, "\"string\""));
    cluster.createMetaSystemStore(storeName);
    cluster.createPushStatusSystemStore(storeName);
    TestUtils.assertCommand(
        controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                .setPartitionCount(PARTITION_COUNT)
                .setIncrementalPushEnabled(true)));
    return storeName;
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT_MS * 2)
  public void testKafkaPushJob(boolean isIsolated) throws Exception {
    Properties vpjProperties = getVPJProperties();
    // setup initial version
    runVPJ(vpjProperties, 1, cluster);

    Map<String, Object> extraBackendConfigMap =
        isIsolated ? TestUtils.getIngestionIsolationPropertyMap() : new HashMap<>();
    extraBackendConfigMap.put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true);
    extraBackendConfigMap.put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 10);
    extraBackendConfigMap.put(PUSH_STATUS_STORE_ENABLED, true);
    extraBackendConfigMap.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 5);
    String expectedInstanceSuffix = "sampleApp_i015";
    extraBackendConfigMap.put(PUSH_STATUS_INSTANCE_NAME_SUFFIX, expectedInstanceSuffix);

    try (DaVinciClient<Integer, Integer> daVinciClient = ServiceFactory.getGenericAvroDaVinciClientWithRetries(
        storeName,
        cluster.getZk().getAddress(),
        new DaVinciConfig(),
        extraBackendConfigMap)) {
      daVinciClient.subscribeAll().get();
      runVPJ(vpjProperties, 2, cluster);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        Map<CharSequence, Integer> partitionStatus = reader.getPartitionStatus(storeName, 2, 0, Optional.empty());
        assertEquals(partitionStatus.size(), 1);
        String expectedHostName = Utils.getHostName() + "_" + expectedInstanceSuffix;
        assertTrue(partitionStatus.containsKey(new Utf8(expectedHostName)));
        assertEquals(partitionStatus.get(new Utf8(expectedHostName)).intValue(), ExecutionStatus.COMPLETED.getValue());
      });
    }
    Admin admin = cluster.getVeniceControllers().get(0).getVeniceAdmin();
    String pushStatusStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName), 1);
    assertTrue(admin.isResourceStillAlive(pushStatusStoreTopic));
    assertFalse(admin.isTopicTruncated(pushStatusStoreTopic));
    TestUtils.assertCommand(controllerClient.disableAndDeleteStore(storeName));

    PubSubTopic pushStatusStorePubSubTopic = pubSubTopicRepository.getTopic(pushStatusStoreTopic);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      assertFalse(admin.isResourceStillAlive(pushStatusStoreTopic));
      assertTrue(
          !admin.getTopicManager().containsTopic(pushStatusStorePubSubTopic)
              || admin.isTopicTruncated(pushStatusStoreTopic));
    });
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testIncrementalPush() throws Exception {
    VeniceProperties backendConfig = getBackendConfigBuilder().build();
    Properties vpjProperties = getVPJProperties();
    runVPJ(vpjProperties, 1, cluster);
    try (DaVinciClient<Integer, Utf8> daVinciClient =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      vpjProperties = getVPJProperties();
      vpjProperties.setProperty(INCREMENTAL_PUSH, "true");
      runVPJ(vpjProperties, 1, cluster);
      assertEquals(daVinciClient.get(1).get().toString(), "name 1");
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testIncrementalPushStatusStoredInPushStatusStore() throws Exception {
    Properties vpjProperties = getVPJProperties();
    runVPJ(vpjProperties, 1, cluster);
    try (AvroGenericStoreClient<Integer, Utf8> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2Client(d2Client)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME))) {
      vpjProperties = getVPJProperties();
      vpjProperties.setProperty(INCREMENTAL_PUSH, "true");
      int expectedVersionNumber = 1;
      long vpjStart = System.currentTimeMillis();
      String jobName = Utils.getUniqueString("batch-job-" + expectedVersionNumber);
      try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
        job.run();
        cluster.waitVersion(storeName, expectedVersionNumber, controllerClient);
        LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
        validateThinClientGet(storeClient, 1, "name 1");
        String incPushVersion = job.getIncrementalPushVersion();
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
          Map<CharSequence, Integer> statuses = reader.getPartitionStatus(
              storeName,
              1,
              partitionId,
              Optional.ofNullable(incPushVersion),
              Optional.of(SERVER_INCREMENTAL_PUSH_PREFIX));
          assertNotNull(statuses);
          assertEquals(statuses.size(), REPLICATION_FACTOR);
          for (Integer status: statuses.values()) {
            assertTrue(ExecutionStatus.isIncrementalPushStatus(status));
          }
        }
      }
    }
  }

  /* The following test is targeted at verifying the behavior of controller when queryJobStatus is invoked for inc-push */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testIncrementalPushStatusReadingFromPushStatusStoreInController() throws Exception {
    Properties vpjProperties = getVPJProperties();
    runVPJ(vpjProperties, 1, cluster);
    try (AvroGenericStoreClient<Integer, Utf8> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2Client(d2Client)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME))) {
      vpjProperties.setProperty(INCREMENTAL_PUSH, "true");
      int expectedVersionNumber = 1;
      long vpjStart = System.currentTimeMillis();
      String jobName = Utils.getUniqueString("batch-job-" + expectedVersionNumber);
      try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
        job.run();
        cluster.waitVersion(storeName, expectedVersionNumber, controllerClient);
        LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
        validateThinClientGet(storeClient, 1, "name 1");
        String incPushVersion = job.getIncrementalPushVersion();
        // verify partition replicas have reported their status to the push status store
        Map<Integer, Map<CharSequence, Integer>> pushStatusMap =
            reader.getPartitionStatuses(storeName, 1, incPushVersion, 2);
        assertNotNull(pushStatusMap, "Server incremental push status cannot be null");
        assertEquals(pushStatusMap.size(), PARTITION_COUNT, "Incremental push status of some partitions is missing");
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
          Map<CharSequence, Integer> pushStatus = pushStatusMap.get(partitionId);
          assertNotNull(pushStatus, "Push status of a partition is missing");
          for (Integer status: pushStatus.values()) {
            assertEquals(status.intValue(), ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED.getValue());
          }
        }

        // expect NOT_CREATED when all non-existing incremental push version is used to query the status
        JobStatusQueryResponse response =
            controllerClient.queryJobStatus(job.getTopicToMonitor(), Optional.of("randomIPVersion"));
        assertEquals(response.getStatus(), ExecutionStatus.NOT_CREATED.name());

        // verify that controller responds with EOIP when all partitions have sufficient replicas with EOIP
        response = controllerClient
            .queryJobStatus(job.getTopicToMonitor(), Optional.ofNullable(job.getIncrementalPushVersion()));
        assertEquals(response.getStatus(), ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED.name());

        int valueSchemaId = AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion();
        Schema valueSchema = AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema();
        Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
        SchemaEntry valueSchemaEntry = new SchemaEntry(valueSchemaId, valueSchema);
        DerivedSchemaEntry updateSchemaEntry = new DerivedSchemaEntry(valueSchemaId, 1, updateSchema);
        PushStatusStoreWriter statusStoreWriter = new PushStatusStoreWriter(
            cluster.getLeaderVeniceController().getVeniceHelixAdmin().getVeniceWriterFactory(),
            "dummyInstance",
            valueSchemaEntry,
            updateSchemaEntry);

        // After deleting the inc push status belonging to just one partition we should expect
        // SOIP from the controller since other partition has replicas with EOIP status
        statusStoreWriter.deletePartitionIncrementalPushStatus(storeName, 1, incPushVersion, 1).get();
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          // N.B.: Even though we block on the deleter's future, that only means the delete message is persisted into
          // Kafka, but querying the system store may still yield a stale result, hence the need for retrying.
          JobStatusQueryResponse jobStatusQueryResponse = controllerClient
              .queryJobStatus(job.getTopicToMonitor(), Optional.ofNullable(job.getIncrementalPushVersion()));
          assertEquals(jobStatusQueryResponse.getStatus(), ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED.name());
        });

        // expect NOT_CREATED when statuses of all partitions are not available in the push status store
        statusStoreWriter.deletePartitionIncrementalPushStatus(storeName, 1, incPushVersion, 0).get();
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          JobStatusQueryResponse jobStatusQueryResponse = controllerClient
              .queryJobStatus(job.getTopicToMonitor(), Optional.ofNullable(job.getIncrementalPushVersion()));
          assertEquals(jobStatusQueryResponse.getStatus(), ExecutionStatus.NOT_CREATED.name());
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testAutomaticPurge() throws Exception {
    VeniceProperties backendConfig = getBackendConfigBuilder().build();
    Properties vpjProperties = getVPJProperties();
    // setup initial version
    runVPJ(vpjProperties, 1, cluster);
    try (DaVinciClient daVinciClient =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 1);
      });
      runVPJ(vpjProperties, 2, cluster);
      runVPJ(vpjProperties, 3, cluster);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 0);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS * 2)
  public void testDeleteUserStoreVersionWhenPushStatusStoreRTIsAbsent() throws Exception {
    VeniceHelixAdmin admin = (VeniceHelixAdmin) cluster.getVeniceControllers().get(0).getVeniceAdmin();
    runVPJ(getVPJProperties(), 1, cluster);
    String storeName2 = createStoreAndSystemStores();
    runVPJ(getVPJProperties(storeName2), 1, cluster);

    String pushStatusStoreRT =
        Utils.composeRealTimeTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName));
    admin.getTopicManager().ensureTopicIsDeletedAndBlock(pubSubTopicRepository.getTopic(pushStatusStoreRT));
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        true,
        () -> assertFalse(admin.getTopicManager().containsTopic(pubSubTopicRepository.getTopic(pushStatusStoreRT))));

    VeniceProperties backendConfig = getBackendConfigBuilder().build();
    try (DaVinciClient daVinciClient =
        ServiceFactory.getGenericAvroDaVinciClient(storeName2, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> assertEquals(reader.getPartitionStatus(storeName2, 1, 0, Optional.empty()).size(), 1));
    }

    ExecutorService asyncExecutor = Executors.newSingleThreadExecutor();
    try {
      // Delete store1 version in main thread and store2 version in a new thread at the same time.
      // Store1 version deletion should finish instead of waiting forever even though system store RT does not exist
      // Store2 da-vinci push status should be deleted successfully
      asyncExecutor.submit(() -> admin.deleteOneStoreVersion(cluster.getClusterName(), storeName2, 1));
      admin.deleteOneStoreVersion(cluster.getClusterName(), storeName, 1);
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> assertEquals(reader.getPartitionStatus(storeName2, 1, 0, Optional.empty()).size(), 0));
    } finally {
      TestUtils.shutdownExecutor(asyncExecutor);
    }
  }

  private PropertyBuilder getBackendConfigBuilder() {
    return DaVinciTestContext.getDaVinciPropertyBuilder(cluster.getZk().getAddress())
        .put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 5)
        .put(PUSH_STATUS_STORE_ENABLED, true);
  }

  private Properties getVPJProperties() throws Exception {
    return getVPJProperties(storeName);
  }

  private Properties getVPJProperties(String storeName) throws Exception {
    // Setup VPJ job properties.
    // Produce input data.
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    writeSimpleAvroFileWithIntToStringSchema(inputDir);
    return defaultVPJProps(cluster, inputDirPath, storeName);
  }

  private void runVPJ(Properties vpjProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long vpjStart = System.currentTimeMillis();
    String jobName = Utils.getUniqueString("batch-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
      job.run();
      String storeName = (String) vpjProperties.get(VENICE_STORE_NAME_PROP);
      cluster.waitVersion(storeName, expectedVersionNumber, controllerClient);
      LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
    }
  }

  private void validateThinClientGet(AvroGenericStoreClient<Integer, Utf8> storeClient, int key, String expectedValue) {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
      Utf8 result = storeClient.get(key).get();
      assertNotNull(result);
      assertEquals(result.toString(), expectedValue);
    });
  }
}
