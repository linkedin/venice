package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.status.protocol.PushJobStatusRecordValue;
import com.linkedin.venice.status.protocol.enums.PushJobStatus;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;
import static com.linkedin.venice.ConfigKeys.*;


public class TestPushJobStatusUpload {
  private static final Logger logger = Logger.getLogger(TestPushJobStatusUpload.class);
  private VeniceClusterWrapper venice;
  private VeniceControllerWrapper parentController;
  private ZkServerWrapper zkWrapper;
  private ControllerClient controllerClient;
  private ControllerClient parentControllerClient;
  private String pushJobStatusStoreName;
  private Properties controllerProperties;

  @BeforeClass
  public void setup() {
    pushJobStatusStoreName = "test-push-job-status-store";
    venice = ServiceFactory.getVeniceCluster(1, 1, 1, 1, 1000000, false, false);
    zkWrapper = ServiceFactory.getZkServer();
    controllerProperties = new Properties();
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    controllerProperties.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    controllerProperties.setProperty(PUSH_JOB_STATUS_STORE_CLUSTER_NAME, venice.getClusterName());
    controllerProperties.setProperty(PUSH_JOB_STATUS_STORE_NAME, pushJobStatusStoreName);
    parentController = ServiceFactory.getVeniceParentController(venice.getClusterName(), zkWrapper.getAddress(),
        venice.getKafka(), new VeniceControllerWrapper[]{venice.getMasterVeniceController()},
        new VeniceProperties(controllerProperties), false);
    controllerClient = venice.getControllerClient();
    parentControllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
    TestUtils.waitForNonDeterministicPushCompletion(Version.composeKafkaTopic(pushJobStatusStoreName, 1),
        controllerClient, 2, TimeUnit.MINUTES, Optional.of(logger));
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getPushJobDetailsStoreName(), 1), controllerClient,
        2, TimeUnit.MINUTES, Optional.of(logger));
  }

  @AfterClass
  public void cleanup() {
    controllerClient.close();
    parentControllerClient.close();
    parentController.close();
    venice.close();
    zkWrapper.close();
  }

  @Test
  public void testPushJobStatusUpload() throws ExecutionException, InterruptedException {
    // Create some push job statuses.
    ArrayList<Pair<PushJobStatusRecordKey, PushJobStatusRecordValue>> keyValuePairs = new ArrayList();
    PushJobStatus[] statuses = new PushJobStatus[]{PushJobStatus.ERROR, PushJobStatus.KILLED, PushJobStatus.SUCCESS};
    for (int i = 0; i < 3; i++) {
      PushJobStatusRecordKey key = new PushJobStatusRecordKey();
      PushJobStatusRecordValue value = new PushJobStatusRecordValue();
      key.storeName = "dummy-store-" + i;
      key.versionNumber = i;
      value.storeName = key.storeName;
      value.clusterName = venice.getClusterName();
      value.versionNumber = key.versionNumber;
      value.status = statuses[i];
      value.pushDuration = 1;
      value.pushId = System.currentTimeMillis() + "-test-push-id";
      value.message = "test message " + i;
      keyValuePairs.add(new Pair(key, value));
    }
    // Upload push job statuses via the endpoint.
    for (int i = 0; i < 3; i++) {
      PushJobStatusRecordKey key = keyValuePairs.get(i).getFirst();
      PushJobStatusRecordValue value = keyValuePairs.get(i).getSecond();
      parentControllerClient.uploadPushJobStatus(key.storeName.toString(), key.versionNumber, value.status,
          value.pushDuration, value.pushId.toString(), value.message.toString());
    }
    // Verify the uploaded push job status records.
    try (AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobStatusRecordValue> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(pushJobStatusStoreName, PushJobStatusRecordValue.class)
                .setVeniceURL(venice.getRandomRouterURL()))) {
      for (Pair<PushJobStatusRecordKey, PushJobStatusRecordValue> pair : keyValuePairs) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          try {
            assertNotNull(client.get(pair.getFirst()).get(), "RT writes are not reflected in store yet");
          } catch (Exception e) {
            fail("Unexpected exception thrown while reading from the venice store", e);
          }
        });
        PushJobStatusRecordValue value = client.get(pair.getFirst()).get();
        assertEquals(value.storeName.toString(), pair.getSecond().storeName.toString(),
            "Push job store name mismatch");
        assertEquals(value.clusterName.toString(), pair.getSecond().clusterName.toString(),
            "Push job cluster name mismatch");
        assertEquals(value.versionNumber, pair.getSecond().versionNumber, "Push job store version number mismatch");
        assertEquals(value.status, pair.getSecond().status, "Push job status mismatch");
        assertEquals(value.pushId.toString(), pair.getSecond().pushId.toString(), "Push job pushId mismatch");
        assertEquals(value.message.toString(), pair.getSecond().message.toString(), "Message mismatch");
      }
    }
  }

  @Test
  public void testPushJobDetails() throws ExecutionException, InterruptedException, IOException {
    String testStoreName = "test-push-store";
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
    parentControllerClient.createNewStore(testStoreName, "test-user",
        recordSchema.getField("id").schema().toString(), recordSchema.getField("name").schema().toString());
    // Set store quota to unlimited else local H2V jobs will fail due to quota enforcement NullPointerException because
    // hadoop job client cannot fetch counters properly.
    parentControllerClient.updateStore(testStoreName, new UpdateStoreQueryParams().setStorageQuotaInByte(-1));
    Properties pushJobProps = defaultH2VProps(venice, inputDirPath, testStoreName);
    pushJobProps.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, String.valueOf(true));
    pushJobProps.setProperty(POLL_JOB_STATUS_INTERVAL_MS, String.valueOf(1000));
    pushJobProps.setProperty(VENICE_URL_PROP, parentController.getControllerUrl());
    pushJobProps.setProperty(VENICE_DISCOVER_URL_PROP, parentController.getControllerUrl());
    KafkaPushJob testPushJob = new KafkaPushJob("test-push-job-details-job", pushJobProps);
    testPushJob.run();

    // Verify the sent push job details.
    try (AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client =
        ClientFactory.getAndStartSpecificAvroClient(ClientConfig.defaultSpecificClientConfig(
            VeniceSystemStoreUtils.getPushJobDetailsStoreName(), PushJobDetails.class)
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
      assertEquals(value.clusterName.toString(), venice.getClusterName(), "Unexpected cluster name from push job details");
      assertTrue(value.reportTimestamp > 0, "Push job details report timestamp is missing");
      List<Integer> expectedStatuses = Arrays.asList(PushJobDetailsStatus.STARTED.getValue(),
          PushJobDetailsStatus.TOPIC_CREATED.getValue(), PushJobDetailsStatus.WRITE_COMPLETED.getValue(),
          PushJobDetailsStatus.COMPLETED.getValue());
      assertEquals(value.overallStatus.size(), expectedStatuses.size(),"Unexpected number of overall statuses in push job details");
      for (int i = 0; i < expectedStatuses.size(); i++) {
        assertEquals(new Integer(value.overallStatus.get(i).status), expectedStatuses.get(i));
        assertTrue(value.overallStatus.get(i).timestamp > 0, "Timestamp for status tuple is missing");
      }
      assertFalse(value.coloStatus.isEmpty(), "Colo status shouldn't be empty");
      for (List<PushJobDetailsStatusTuple> tuple : value.coloStatus.values()) {
        assertEquals(tuple.get(tuple.size() - 1).status, PushJobDetailsStatus.COMPLETED.getValue(),
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
  }

  /**
   * This is to ensure the known {@link com.linkedin.venice.pushmonitor.ExecutionStatus} statuses reported as part of
   * push job details can be parsed properly. This test should fail and alert developers when adding new statuses in
   * {@link com.linkedin.venice.pushmonitor.ExecutionStatus} without modifying this test or {@link PushJobDetailsStatus}.
   */
  @Test
  public void testPushJobDetailsStatusEnums() {
    // A list of known ExecutionStatus that we don't report/expose to job status polling.
    ExecutionStatus[] unreportedStatusesArray = {NEW, PROGRESS, START_OF_BUFFER_REPLAY_RECEIVED, TOPIC_SWITCH_RECEIVED,
        DROPPED, WARNING, ARCHIVED};
    HashSet<ExecutionStatus> unreportedStatuses = new HashSet<>(Arrays.asList(unreportedStatusesArray));
    HashSet<Integer> processedSignals = new HashSet<>();
    for (ExecutionStatus status : ExecutionStatus.values()) {
      if (unreportedStatuses.contains(status)) {
        continue; // Ignore parsing of statuses that are never reported.
      }
      Integer intValue = PushJobDetailsStatus.valueOf(status.toString()).getValue();
      assertFalse(processedSignals.contains(intValue), "Each PushJobDetailsStatus should have its own unique int value");
      processedSignals.add(intValue);
    }
  }
}
