package com.linkedin.venice.endToEnd;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.status.protocol.PushJobStatusRecordValue;
import com.linkedin.venice.status.protocol.enums.PushJobStatus;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestPushJobStatusUpload {
  @Test(timeOut = 4 * Time.MS_PER_MINUTE)
  public void testPushJobStatusUpload() throws ExecutionException, InterruptedException {
    String pushJobStatusStoreName = "test-push-job-status-store";
    Properties properties = new Properties();
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1, 2, 1, 1, 1000000, false, false);
    ZkServerWrapper parentZk = ServiceFactory.getZkServer();
    properties.setProperty(ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME, venice.getClusterName());
    properties.setProperty(ConfigKeys.PUSH_JOB_STATUS_STORE_NAME, pushJobStatusStoreName);
    // Disable topic cleanup since the parent and child controller are using the same kafka cluster/topic in test environment.
    properties.setProperty(ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    VeniceControllerWrapper parentController =
        ServiceFactory.getVeniceParentController(venice.getClusterName(), parentZk.getAddress(), venice.getKafka(),
            new VeniceControllerWrapper[]{venice.getMasterVeniceController()}, new VeniceProperties(properties), false);
    final ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
    try {
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
      // Wait for the push job status store and topic to be created
      TestUtils.waitForNonDeterministicAssertion(4, TimeUnit.MINUTES, true, () -> {
        String emptyPushStatus = controllerClient.queryOverallJobStatus(
            Version.composeKafkaTopic(pushJobStatusStoreName, 1), Optional.empty()).getStatus();
        if (emptyPushStatus.equals(ExecutionStatus.ERROR.toString())) {
          fail("Unexpected empty push failure for setting up the store " + pushJobStatusStoreName);
        }
        assertEquals(emptyPushStatus, ExecutionStatus.COMPLETED.toString(), "Empty push to set up the "
            + pushJobStatusStoreName + " is not completed yet");
      });
      // Upload more push job statuses via the endpoint
      for (int i = 0; i < 3; i++) {
        PushJobStatusRecordKey key = keyValuePairs.get(i).getFirst();
        PushJobStatusRecordValue value = keyValuePairs.get(i).getSecond();
        controllerClient.uploadPushJobStatus(key.storeName.toString(), key.versionNumber, value.status,
            value.pushDuration, value.pushId.toString(), value.message.toString());
      }
      // Verify the uploaded push job status records
      AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobStatusRecordValue> client =
          ClientFactory.getAndStartSpecificAvroClient(
              ClientConfig.defaultSpecificClientConfig(pushJobStatusStoreName, PushJobStatusRecordValue.class)
                  .setVeniceURL(venice.getRandomRouterURL()));
      try {
        for (Pair<PushJobStatusRecordKey, PushJobStatusRecordValue> pair : keyValuePairs) {
          TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
            try {
              assertNotNull(client.get(pair.getFirst()).get());
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
      } finally {
        client.close();
      }
    } finally {
      controllerClient.close();
      parentController.close();
      parentZk.close();
      venice.close();
    }
  }
}
