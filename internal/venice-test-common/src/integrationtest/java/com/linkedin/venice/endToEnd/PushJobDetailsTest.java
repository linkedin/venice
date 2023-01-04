package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_ALL_STORES;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.PUSH_JOB_STATUS_UPLOAD_ENABLE;
import static com.linkedin.venice.hadoop.VenicePushJob.PushJobCheckpoints;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ARCHIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DATA_RECOVERY_COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DROPPED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NEW;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.PROGRESS;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.TOPIC_SWITCH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.WARNING;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestPushUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestPushUtils.writeSimpleAvroFileWithUserSchema;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PushJobDetailsTest {
  private final Map<Integer, Schema> schemaVersionMap = new HashMap<>();
  private final int latestSchemaId = 2;
  private VeniceClusterWrapper venice;
  private VeniceControllerWrapper parentController;
  private ZkServerWrapper zkWrapper;
  private ControllerClient parentControllerClient;
  private Properties controllerProperties;
  private Schema recordSchema;
  private String inputDirPath;

  @BeforeClass
  public void setUp() throws IOException {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    extraProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    extraProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    venice = ServiceFactory.getVeniceCluster(1, 1, 1, 1, 1000000, false, false, extraProperties);
    zkWrapper = ServiceFactory.getZkServer();
    controllerProperties = new Properties();
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    controllerProperties
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    controllerProperties.setProperty(PUSH_JOB_STATUS_STORE_CLUSTER_NAME, venice.getClusterName());
    controllerProperties.setProperty(ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_ALL_STORES, "true");
    controllerProperties.setProperty(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE, "true");
    controllerProperties.setProperty(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE, "true");
    parentController = ServiceFactory.getVeniceController(
        new VeniceControllerCreateOptions.Builder(venice.getClusterName(), venice.getKafka())
            .childControllers(new VeniceControllerWrapper[] { venice.getLeaderVeniceController() })
            .extraProperties(controllerProperties)
            .zkAddress(zkWrapper.getAddress())
            .build());
    parentControllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
    venice.useControllerClient(
        controllerClient -> TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(VeniceSystemStoreUtils.getPushJobDetailsStoreName(), 1),
            controllerClient,
            2,
            TimeUnit.MINUTES));
    File inputDir = getTempDataDirectory();
    inputDirPath = "file://" + inputDir.getAbsolutePath();
    recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
    for (int i = 1; i <= latestSchemaId; i++) {
      schemaVersionMap.put(i, Utils.getSchemaFromResource("avro/PushJobDetails/v" + i + "/PushJobDetails.avsc"));
    }
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(parentController);
    Utils.closeQuietlyWithErrorLogged(venice);
    Utils.closeQuietlyWithErrorLogged(zkWrapper);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPushJobDetails() throws ExecutionException, InterruptedException, IOException {
    String testStoreName = "test-push-store";
    parentControllerClient.createNewStore(
        testStoreName,
        "test-user",
        recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString(),
        recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString());
    // Set store quota to unlimited else local VPJ jobs will fail due to quota enforcement NullPointerException because
    // hadoop job client cannot fetch counters properly.
    parentControllerClient
        .updateStore(testStoreName, new UpdateStoreQueryParams().setStorageQuotaInByte(-1).setPartitionCount(2));
    Properties pushJobProps = defaultVPJProps(venice, inputDirPath, testStoreName);
    pushJobProps.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, String.valueOf(true));
    pushJobProps.setProperty(VENICE_DISCOVER_URL_PROP, parentController.getControllerUrl());
    try (VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job", pushJobProps)) {
      testPushJob.run();
    }

    // Verify the sent push job details.
    try (AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig
                .defaultSpecificClientConfig(VeniceSystemStoreUtils.getPushJobDetailsStoreName(), PushJobDetails.class)
                .setVeniceURL(venice.getRandomRouterURL()))) {
      PushJobStatusRecordKey key = new PushJobStatusRecordKey();
      key.storeName = testStoreName;
      key.versionNumber = 1;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        try {
          assertNotNull(client.get(key).get(), "RT writes are not reflected in store yet");
        } catch (Exception e) {
          fail("Unexpected exception thrown while reading from the venice store", e);
        }
      });
      PushJobDetails value = client.get(key).get();
      assertEquals(
          value.clusterName.toString(),
          venice.getClusterName(),
          "Unexpected cluster name from push job details");
      assertTrue(value.reportTimestamp > 0, "Push job details report timestamp is missing");
      List<Integer> expectedStatuses = Arrays.asList(
          PushJobDetailsStatus.STARTED.getValue(),
          PushJobDetailsStatus.TOPIC_CREATED.getValue(),
          PushJobDetailsStatus.WRITE_COMPLETED.getValue(),
          PushJobDetailsStatus.COMPLETED.getValue());
      assertEquals(
          value.overallStatus.size(),
          expectedStatuses.size(),
          "Unexpected number of overall statuses in push job details");
      for (int i = 0; i < expectedStatuses.size(); i++) {
        assertEquals(value.overallStatus.get(i).status, (int) expectedStatuses.get(i));
        assertTrue(value.overallStatus.get(i).timestamp > 0, "Timestamp for status tuple is missing");
      }
      assertFalse(value.coloStatus.isEmpty(), "Colo status shouldn't be empty");
      for (List<PushJobDetailsStatusTuple> tuple: value.coloStatus.values()) {
        assertEquals(
            tuple.get(tuple.size() - 1).status,
            PushJobDetailsStatus.COMPLETED.getValue(),
            "Latest status for every colo should be COMPLETED");
        assertTrue(tuple.get(tuple.size() - 1).timestamp > 0, "Timestamp for colo status tuple is missing");
      }
      assertTrue(value.jobDurationInMs > 0);
      assertTrue(value.totalNumberOfRecords > 0);
      assertTrue(value.totalKeyBytes > 0);
      assertTrue(value.totalRawValueBytes > 0);
      assertTrue(value.totalCompressedValueBytes > 0);
      assertNotNull(value.pushJobConfigs);
      assertFalse(value.pushJobConfigs.isEmpty());
      assertNotNull(value.producerConfigs);
      assertTrue(value.producerConfigs.isEmpty());
    }

    // Verify records (note, records 1-100 have been pushed)
    try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(testStoreName).setVeniceURL(venice.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          for (int i = 1; i < 100; i++) {
            String key = String.valueOf(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
            assertEquals(value.toString(), "test_name_" + key);
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPushJobDetailsFailureTags() throws ExecutionException, InterruptedException {
    String testStoreName = "test-push-failure-store";
    parentControllerClient.createNewStore(
        testStoreName,
        "test-user",
        recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString(),
        recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString());
    // hadoop job client cannot fetch counters properly and should fail the job
    parentControllerClient.updateStore(testStoreName, new UpdateStoreQueryParams().setStorageQuotaInByte(0));
    Properties pushJobProps = defaultVPJProps(venice, inputDirPath, testStoreName);
    pushJobProps.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, String.valueOf(true));
    pushJobProps.setProperty(VENICE_DISCOVER_URL_PROP, parentController.getControllerUrl());
    try (VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job", pushJobProps)) {
      assertThrows(VeniceException.class, testPushJob::run);
    }
    try (AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig
                .defaultSpecificClientConfig(VeniceSystemStoreUtils.getPushJobDetailsStoreName(), PushJobDetails.class)
                .setVeniceURL(venice.getRandomRouterURL()))) {
      PushJobStatusRecordKey key = new PushJobStatusRecordKey();
      key.storeName = testStoreName;
      key.versionNumber = 1;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        try {
          assertNotNull(client.get(key).get(), "RT writes are not reflected in store yet");
        } catch (Exception e) {
          fail("Unexpected exception thrown while reading from the venice store", e);
        }
      });
      PushJobDetails value = client.get(key).get();
      assertEquals(
          value.pushJobLatestCheckpoint.intValue(),
          PushJobCheckpoints.START_MAP_REDUCE_JOB.getValue(),
          "Unexpected latest push job checkpoint reported");
      assertFalse(value.failureDetails.toString().isEmpty());
    }
  }

  /**
   * This is to ensure the known {@link com.linkedin.venice.pushmonitor.ExecutionStatus} statuses reported as part of
   * push job details can be parsed properly. This test should fail and alert developers when adding new statuses in
   * {@link com.linkedin.venice.pushmonitor.ExecutionStatus} without modifying this test or {@link PushJobDetailsStatus}.
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPushJobDetailsStatusEnums() {
    // A list of known ExecutionStatus that we don't report/expose to job status polling.
    ExecutionStatus[] unreportedStatusesArray = { NEW, NOT_STARTED, PROGRESS, START_OF_BUFFER_REPLAY_RECEIVED,
        TOPIC_SWITCH_RECEIVED, DROPPED, WARNING, ARCHIVED, CATCH_UP_BASE_TOPIC_OFFSET_LAG, DATA_RECOVERY_COMPLETED };
    HashSet<ExecutionStatus> unreportedStatuses = new HashSet<>(Arrays.asList(unreportedStatusesArray));
    HashSet<Integer> processedSignals = new HashSet<>();
    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (unreportedStatuses.contains(status)) {
        continue; // Ignore parsing of statuses that are never reported.
      }
      Integer intValue = PushJobDetailsStatus.valueOf(status.toString()).getValue();
      assertFalse(
          processedSignals.contains(intValue),
          "Each PushJobDetailsStatus should have its own unique int value");
      processedSignals.add(intValue);
    }
  }
}
