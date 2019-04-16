package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.integration.utils.IntegrationTestUtils;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageStoreUtils;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestParticipantMessageStore {

  /**
   * This test is disabled for now since {@link com.linkedin.venice.ConfigKeys#PARTICIPANT_MESSAGE_STORE_ENABLED}
   * is disabled in {@link IntegrationTestUtils#getClusterProps(String, File, String, KafkaBrokerWrapper, boolean)}.
   */
  @Test(timeOut = 2 * Time.MS_PER_MINUTE, enabled = false)
  public void testKillPushMessage() {
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1, 2, 1, 1, 1000000, false, false);
    String participantMessageStoreName = ParticipantMessageStoreUtils.getStoreNameForCluster(venice.getClusterName());
    ZkServerWrapper parentZk = ServiceFactory.getZkServer();
    VeniceControllerWrapper parentController =
        ServiceFactory.getVeniceParentController(venice.getClusterName(), parentZk.getAddress(), venice.getKafka(),
            new VeniceControllerWrapper[]{venice.getMasterVeniceController()}, false);
    final ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
    TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
      assertFalse(controllerClient.getStore(participantMessageStoreName).isError());
    });
    try {
      String testResourceName1 = "test-store_v1";
      ControllerResponse response = controllerClient.killOfflinePushJob(testResourceName1);
      assertFalse(response.isError());
      // Verify the kill push message is in the participant message store
      ParticipantMessageKey key = new ParticipantMessageKey();
      key.resourceName = testResourceName1;
      key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
      try (
          AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> client = ClientFactory.getAndStartSpecificAvroClient(
              ClientConfig.defaultSpecificClientConfig(participantMessageStoreName, ParticipantMessageValue.class)
                  .setVeniceURL(venice.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          try {
            assertNotNull(client.get(key).get());
          } catch (Exception e) {
            fail("Unexpected exception thrown while reading from the participant message store", e);
          }
        });
      }
    } finally {
      controllerClient.close();
      parentController.close();
      parentZk.close();
      venice.close();
    }
  }
}
