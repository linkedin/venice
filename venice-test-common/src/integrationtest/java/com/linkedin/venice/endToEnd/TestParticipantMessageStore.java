package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.participant.ParticipantMessageStoreUtils;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.Metric;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static org.testng.Assert.*;


public class TestParticipantMessageStore {
  @Test(timeOut = 2 * Time.MS_PER_MINUTE)
  public void testKillPushMessageEndToEnd() {
    testParticipantStoreHelper(true, false);
  }

  @Test(timeOut = 2 * Time.MS_PER_MINUTE)
  public void testParticipantMessageDisabled() {
    testParticipantStoreHelper(false, true);
  }

  private void testParticipantStoreHelper(boolean participantMessageEnabled, boolean adminHelixMessagingChannelEnabled) {
    Properties propertiesToEnableKillViaParticipantMessageStore = new Properties();
    propertiesToEnableKillViaParticipantMessageStore.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED,
        Boolean.toString(participantMessageEnabled));
    propertiesToEnableKillViaParticipantMessageStore.setProperty(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED,
        Boolean.toString(adminHelixMessagingChannelEnabled));

    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(
        1, 0, 1, 1, 1000000,
        false, false, propertiesToEnableKillViaParticipantMessageStore);
    Properties featureProperties = new Properties();
    D2Client d2Client = D2TestUtils.getAndStartD2Client(venice.getZk().getAddress());
    featureProperties.put(VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER, ClientConfig.defaultGenericClientConfig("")
        .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME)
        .setD2Client(d2Client));
    Properties serverProperties = new Properties();
    serverProperties.setProperty(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, Long.toString(1000));
    venice.addVeniceServer(featureProperties, serverProperties);
    String participantMessageStoreName = ParticipantMessageStoreUtils.getStoreNameForCluster(venice.getClusterName());
    ZkServerWrapper parentZk = ServiceFactory.getZkServer();
    VeniceControllerWrapper parentController =
        ServiceFactory.getVeniceParentController(venice.getClusterName(), parentZk.getAddress(), venice.getKafka(),
            new VeniceControllerWrapper[]{venice.getMasterVeniceController()},
            new VeniceProperties(propertiesToEnableKillViaParticipantMessageStore), false);
    final ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
    if (participantMessageEnabled) {
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        // Ensure the participant message store is ready to serve
        assertFalse(controllerClient.getStore(participantMessageStoreName).isError());
      });
    }
    try {
      String storeName = "test-store";
      long storeSize = 10 * 1024 * 1024;
      controllerClient.createNewStore(storeName, "test-user", "\"string\"", "\"string\"");
      VersionCreationResponse versionCreationResponse = controllerClient.requestTopicForWrites(storeName, storeSize,
          ControllerApiConstants.PushType.BATCH, Version.guidBasedDummyPushId(), true, true);
      assertFalse(versionCreationResponse.isError());
      String topicName = versionCreationResponse.getKafkaTopic();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        // Verify the push job is STARTED.
        assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.STARTED.toString());
      });
      if (!participantMessageEnabled) {
        // Mimic ongoing push and give ParticipantStoreConsumptionTask cycles to poll the nonexistent participant store.
        Utils.sleep(3000);
      }
      ControllerResponse response = controllerClient.killOfflinePushJob(topicName);
      assertFalse(response.isError());
      if (participantMessageEnabled) {
        // Verify the kill push message is in the participant message store.
        ParticipantMessageKey key = new ParticipantMessageKey();
        key.resourceName = topicName;
        key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
        try (AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> client =
            ClientFactory.getAndStartSpecificAvroClient(
                ClientConfig.defaultSpecificClientConfig(participantMessageStoreName,
                    ParticipantMessageValue.class).setVeniceURL(venice.getRandomRouterURL()))) {
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
            try {
              // Verify that the kill offline message has made it to the participant message store.
              assertNotNull(client.get(key).get());
            } catch (Exception e) {
              // no op
            }
          });
        }
      }
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        // Poll job status to verify the job is killed
        assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.ERROR.toString());
      });
      if (participantMessageEnabled) {
        // Verify participant store consumption stats
        String metricPrefix = "." + venice.getClusterName() + "-participant_store_consumption_task";
        String requestMetricExample = ParticipantMessageStoreUtils.getStoreNameForCluster(venice.getClusterName())
            + "--success_request_key_count.Avg";
        Map<String, ? extends Metric> metrics = venice.getVeniceServers().iterator().next().getMetricsRepository().metrics();
        assertEquals(metrics.get(metricPrefix + "--killed_push_jobs.Count").value(), 1.0);
        assertTrue(metrics.get(metricPrefix + "--kill_push_job_latency.Avg").value() > 0);
        // One from the server stats and the other from the client stats.
        assertTrue(metrics.get("." + requestMetricExample).value() > 0);
        assertTrue(metrics.get(".venice-client." + requestMetricExample).value() > 0);
      }
    } finally {
      controllerClient.close();
      parentController.close();
      parentZk.close();
      venice.close();
    }
  }
}
