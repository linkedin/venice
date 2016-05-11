package com.linkedin.venice.helix;

import com.linkedin.venice.controlmessage.StoreStatusMessage;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for HelixControlMessageChannel
 */
public class TestHelixControlMessageChannel {
  private String cluster = "UnitTestCluster";
  private String kafkaTopic = "test_resource_1";
  private int partitionId = 0;
  private String instanceId = "localhost_1234";
  private ExecutionStatus status = ExecutionStatus.COMPLETED;
  private ZkServerWrapper zkServerWrapper;
  private String zkAddress;
  private HelixControlMessageChannel channel;
  private HelixManager manager;
  private HelixAdmin admin;
  private HelixManager controller;
  private final long WAIT_ZK_TIME = 1000l;

  @BeforeMethod
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
    admin.addStateModelDef(cluster, TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
        TestHelixRoutingDataRepository.UnitTestStateModel.getDefinition());

    admin.addResource(cluster, kafkaTopic, 1, TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(cluster, kafkaTopic, 1);

    controller = HelixControllerMain
        .startHelixController(zkAddress, cluster, "UnitTestController", HelixControllerMain.STANDALONE);
    controller.connect();

    manager = HelixManagerFactory.getZKHelixManager(cluster, instanceId, InstanceType.PARTICIPANT, zkAddress);
    manager.getStateMachineEngine()
        .registerStateModelFactory(TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
            new TestHelixRoutingDataRepository.UnitTestStateModelFactory());

    manager.connect();
    channel = new HelixControlMessageChannel(manager);
  }

  @AfterMethod
  public void cleanup() {
    manager.disconnect();
    controller.disconnect();
    admin.dropCluster(cluster);
    admin.close();
    zkServerWrapper.close();
  }

  private void compareConversion(StoreStatusMessage veniceMessage) {
    Message helixMessage = channel.convertVeniceMessageToHelixMessage(veniceMessage);
    Assert.assertEquals(veniceMessage.getMessageId(), helixMessage.getMsgId(),
            "Message Ids are different.");
    Assert.assertEquals(StoreStatusMessage.class.getName(),
            helixMessage.getRecord().getSimpleField(HelixControlMessageChannel.VENICE_MESSAGE_CLASS),
            "Class names are different.");
    Map<String, String> fields = helixMessage.getRecord().getMapField(HelixControlMessageChannel.VENICE_MESSAGE_FIELD);
    for (Map.Entry<String, String> entry : veniceMessage.getFields().entrySet()) {
      Assert.assertEquals(entry.getValue(), fields.get(entry.getKey()),
              "Message fields are different.");
    }

    StoreStatusMessage convertedVeniceMessage = (StoreStatusMessage) channel.convertHelixMessageToVeniceMessage(helixMessage);
    Assert.assertEquals(veniceMessage, convertedVeniceMessage,
            "Message fields are different. Convert it failed,");
  }

  @Test
  public void testConvertBetweenVeniceMessageAndHelixMessage()
      throws ClassNotFoundException {
    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    compareConversion(veniceMessage);

    veniceMessage.setOffset(10);
    compareConversion(veniceMessage);

    veniceMessage.setDescription("Sample Description ");
    compareConversion(veniceMessage);
  }

  @Test
  public void testRegisterHandler() {
    StoreStatusMessageHandler hander = new StoreStatusMessageHandler();

    channel.registerHandler(StoreStatusMessage.class, hander);

    Assert.assertEquals(hander, channel.getHandler(StoreStatusMessage.class),
        "Can not get correct handler.Register is failed.");

    channel.unRegisterHandler(StoreStatusMessage.class, hander);
    try {
      channel.getHandler(StoreStatusMessage.class);
      Assert.fail("Handler should be un-register before.");
    } catch (VeniceException e) {
      //Expected.
    }
  }

  @Test
  public void testSendMessage()
      throws IOException, InterruptedException {
    //Register handler for message in controler side.
    HelixControlMessageChannel controllerChannel = new HelixControlMessageChannel(controller);
    StoreStatusMessageHandler handler = new StoreStatusMessageHandler();
    controllerChannel.registerHandler(StoreStatusMessage.class, handler);

    Thread.sleep(WAIT_ZK_TIME);
    StoreStatusMessage veniceMessage =
        new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    channel.sendToController(veniceMessage);
    StoreStatusMessage receivedMessage = handler.getStatus(veniceMessage.getKafkaTopic());
    Assert.assertNotNull(receivedMessage, "Message is not received.");
    Assert.assertEquals(veniceMessage.getMessageId(), receivedMessage.getMessageId(),
        "Message is not received correctly. Id is wrong.");

    Assert.assertEquals(veniceMessage.getFields(), receivedMessage.getFields(),
        "Message is not received correctly. Fields are wrong");
  }
}
