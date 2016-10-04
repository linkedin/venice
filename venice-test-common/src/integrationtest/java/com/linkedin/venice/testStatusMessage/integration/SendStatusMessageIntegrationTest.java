package com.linkedin.venice.testStatusMessage.integration;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.integration.utils.DelayedZkClientUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.status.StatusMessageHandler;
import com.linkedin.venice.status.StoreStatusMessage;
import com.linkedin.venice.utils.MockTestStateModel;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SendStatusMessageIntegrationTest {
  private ZkServerWrapper zkServerWrapper;
  private String zkAddress;
  private HelixAdmin admin;
  private String cluster = TestUtils.getUniqueString("sendStatusMessage");
  private HelixManager controller;
  private ArrayList<HelixManager> participants = new ArrayList<>();

  @BeforeClass
  public void setup()
      throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    admin = new ZKHelixAdmin(zkAddress);
    admin.addCluster(cluster);
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(cluster).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(cluster, MockTestStateModel.UNIT_TEST_STATE_MODEL,
        MockTestStateModel.getDefinition());

    controller = HelixControllerMain.startHelixController(zkAddress, cluster, "integrationController",
        HelixControllerMain.STANDALONE);
    controller.connect();
  }

  @AfterClass
  public void cleanup(){
    controller.disconnect();
    zkServerWrapper.close();
  }

  private HelixStatusMessageChannel getParticipantDelayedChannel(int port, long lowerDelay, long upperDelay)
      throws Exception {
    DelayedZkClientUtils.startDelayingSocketIoForNewZkClients(lowerDelay, upperDelay);
    HelixManager participant = TestUtils.getParticipant(cluster, Utils.getHelixNodeIdentifier(port), zkAddress, port,
        MockTestStateModel.UNIT_TEST_STATE_MODEL);
    participant.connect();
    DelayedZkClientUtils.stopDelayingSocketIoForNewZkClients();
    participants.add(participant);
    return new HelixStatusMessageChannel(participant);
  }

  private void cleanUpParticipants() {
    for (HelixManager participant : participants) {
      participant.disconnect();
    }
  }

  /**
   * Send status message by three participant with the different network latency.
   *
   * @throws Exception
   */
  @Test
  public void testReceiveMessageFromThreeParticipants()
      throws Exception {
    // channel1 delay 6ms
    final HelixStatusMessageChannel channel1 = getParticipantDelayedChannel(50123, 2, 2);
    // channel2 delay 9ms;
    final HelixStatusMessageChannel channel2 = getParticipantDelayedChannel(50223, 3, 3);
    // channel3 delay 150ms
    final HelixStatusMessageChannel channel3 = getParticipantDelayedChannel(50323, 50, 50);

    try {
      HelixStatusMessageChannel controllerChannel = new HelixStatusMessageChannel(controller);
      final LinkedList<StoreStatusMessage> receivedMessageList = new LinkedList<>();
      controllerChannel.registerHandler(StoreStatusMessage.class, new StatusMessageHandler<StoreStatusMessage>() {
        @Override
        public void handleMessage(StoreStatusMessage message) {
          receivedMessageList.add(message);
        }
      });
      StoreStatusMessage channel1m1 = new StoreStatusMessage("test", 0, "test", ExecutionStatus.STARTED);
      StoreStatusMessage channel2m1 = new StoreStatusMessage("test", 1, "test", ExecutionStatus.STARTED);
      StoreStatusMessage channel1m2 = new StoreStatusMessage("test", 0, "test", ExecutionStatus.COMPLETED);
      StoreStatusMessage channel3m1 = new StoreStatusMessage("test", 2, "test", ExecutionStatus.STARTED);

      Thread t1 = new Thread(new Runnable() {
        @Override
        public void run() {
          channel1.sendToController(channel1m1);
          channel1.sendToController(channel1m2);
        }
      });
      t1.start();
      Thread t2 = new Thread(new Runnable() {
        @Override
        public void run() {
          channel2.sendToController(channel2m1);
        }
      });
      while (!t1.isAlive()) {
      }
      t2.start();

      t1.join();
      t2.join();

      Assert.assertEquals(receivedMessageList.size(), 3);
      Assert.assertEquals(receivedMessageList.get(0), channel1m1, "The first message should come from channel 1");
      Assert.assertEquals(receivedMessageList.get(1), channel2m1, "The second message should come from channel 2");
      Assert.assertEquals(receivedMessageList.get(2), channel1m2, "The third message should come from channel 1");
      try {
        channel3.sendToController(channel3m1);
        Assert.fail(
            "Channel 3 has 150ms latency, sending and receive a helix message spend at least 150*4+150*2+150*2=1200ms, but the "
                + "timeout we setup for message channel is 1000ms, so sending should be timeout.");
      } catch (VeniceException e) {

        //expected time out
      }
    } finally {
      cleanUpParticipants();
    }
  }
}
