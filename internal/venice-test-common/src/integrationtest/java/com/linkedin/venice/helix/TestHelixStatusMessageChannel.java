package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.stats.HelixMessageChannelStats;
import com.linkedin.venice.status.StatusMessageHandler;
import com.linkedin.venice.status.StoreStatusMessage;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for HelixStatusMessageChannel
 */
public class TestHelixStatusMessageChannel {
  private final static String cluster = "UnitTestCluster";
  private final static String kafkaTopic = "test_resource_1";
  private final static int partitionId = 0;
  private final static int port = 4396;
  private String instanceId;
  private ExecutionStatus status = ExecutionStatus.COMPLETED;
  private ZkServerWrapper zkServerWrapper;
  private String zkAddress;
  private HelixStatusMessageChannel channel;
  private HelixMessageChannelStats helixMessageChannelStats;
  private SafeHelixManager manager;
  private HelixAdmin admin;
  private SafeHelixManager controller;
  private RoutingDataRepository routingDataRepository;
  private final static long WAIT_ZK_TIME = 1000l;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    admin = new ZKHelixAdmin(zkAddress);
    admin.addCluster(cluster);
    HelixConfigScope configScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(cluster).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(cluster, LeaderStandbySMD.name, LeaderStandbySMD.build());

    admin.addResource(cluster, kafkaTopic, 1, LeaderStandbySMD.name, IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(cluster, kafkaTopic, 1);

    controller = new SafeHelixManager(
        HelixControllerMain
            .startHelixController(zkAddress, cluster, "UnitTestController", HelixControllerMain.STANDALONE));
    controller.connect();
    instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    manager = TestUtils.getParticipant(cluster, instanceId, zkAddress, port, LeaderStandbySMD.name);
    manager.connect();
    helixMessageChannelStats = new HelixMessageChannelStats(new MetricsRepository(), cluster);
    channel = new HelixStatusMessageChannel(manager, helixMessageChannelStats);
    routingDataRepository = new HelixExternalViewRepository(controller);
    routingDataRepository.refresh();
  }

  @AfterMethod(alwaysRun = true)
  public void cleanUp() {
    manager.disconnect();
    controller.disconnect();
    admin.dropCluster(cluster);
    admin.close();
    zkServerWrapper.close();
  }

  private void compareConversion(StoreStatusMessage veniceMessage) {
    Message helixMessage = channel.convertVeniceMessageToHelixMessage(veniceMessage);
    Assert.assertEquals(veniceMessage.getMessageId(), helixMessage.getMsgId(), "Message Ids are different.");
    Assert.assertEquals(
        StoreStatusMessage.class.getName(),
        helixMessage.getRecord().getSimpleField(HelixStatusMessageChannel.VENICE_MESSAGE_CLASS),
        "Class names are different.");
    Map<String, String> fields = helixMessage.getRecord().getMapField(HelixStatusMessageChannel.VENICE_MESSAGE_FIELD);
    for (Map.Entry<String, String> entry: veniceMessage.getFields().entrySet()) {
      Assert.assertEquals(entry.getValue(), fields.get(entry.getKey()), "Message fields are different.");
    }

    StoreStatusMessage convertedVeniceMessage =
        (StoreStatusMessage) channel.convertHelixMessageToVeniceMessage(helixMessage);
    Assert.assertEquals(veniceMessage, convertedVeniceMessage, "Message fields are different. Convert it failed,");
  }

  @Test
  public void testConvertBetweenVeniceMessageAndHelixMessage() throws ClassNotFoundException {
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

    Assert.assertEquals(
        hander,
        channel.getHandler(StoreStatusMessage.class),
        "Can not get correct handler.Register is failed.");

    channel.unRegisterHandler(StoreStatusMessage.class, hander);
    try {
      channel.getHandler(StoreStatusMessage.class);
      Assert.fail("Handler should be un-register before.");
    } catch (VeniceException e) {
      // Expected.
    }
  }

  @Test
  public void testSendMessage() throws IOException, InterruptedException {
    // Register handler for message in controler side.
    StoreStatusMessageHandler handler = new StoreStatusMessageHandler();
    getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    channel.sendToController(veniceMessage);
    StoreStatusMessage receivedMessage = handler.getStatus(veniceMessage.getKafkaTopic());
    Assert.assertNotNull(receivedMessage, "Message is not received.");
    Assert.assertEquals(
        veniceMessage.getMessageId(),
        receivedMessage.getMessageId(),
        "Message is not received correctly. Id is wrong.");

    Assert.assertEquals(
        veniceMessage.getFields(),
        receivedMessage.getFields(),
        "Message is not received correctly. Fields are wrong");
  }

  private HelixStatusMessageChannel getControllerChannel(StatusMessageHandler<StoreStatusMessage> handler) {
    HelixStatusMessageChannel controllerChannel = new HelixStatusMessageChannel(controller, helixMessageChannelStats);
    controllerChannel.registerHandler(StoreStatusMessage.class, handler);
    return controllerChannel;
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testSendMessageFailed() throws IOException, InterruptedException {
    int retryCount = 1;
    FailedTestStoreStatusMessageHandler handler = new FailedTestStoreStatusMessageHandler(retryCount);
    getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    channel.sendToController(veniceMessage, 0, 0);
    Assert.fail("Sending should be failed, because we thrown an exception during handing message.");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testSendMessageRetryFailed() throws IOException, InterruptedException {
    int retryCount = 5;
    FailedTestStoreStatusMessageHandler handler = new FailedTestStoreStatusMessageHandler(retryCount);
    getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    channel.sendToController(veniceMessage, 2, 0);
    Assert.fail("Sending should be failed, because after retrying 2 times, handling message is stil failed.");
  }

  @Test
  public void testSendMessageRetrySuccessful() throws IOException, InterruptedException {
    int retryCount = 2;
    FailedTestStoreStatusMessageHandler handler = new FailedTestStoreStatusMessageHandler(retryCount);
    getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    try {
      channel.sendToController(veniceMessage, retryCount, 0);
    } catch (VeniceException e) {
      Assert.fail("Sending should be successful after retrying " + retryCount + " times", e);
    }
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testSendMessageTimeout() throws IOException, InterruptedException {
    int timeoutCount = 1;
    TimeoutTestStoreStatusMessageHandler handler = new TimeoutTestStoreStatusMessageHandler(timeoutCount);
    getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    channel.sendToController(veniceMessage, 0, 0);
    Assert.fail("Sending should be failed, because timeout");
  }

  @Test
  public void testSendMessageHandleTimeout() throws IOException, InterruptedException {
    int timeoutCount = 1;
    TimeoutTestStoreStatusMessageHandler handler = new TimeoutTestStoreStatusMessageHandler(timeoutCount);
    getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    try {
      channel.sendToController(veniceMessage, timeoutCount, 0);
    } catch (VeniceException e) {
      Assert.fail("Sending should be successful after retry " + timeoutCount + " times", e);
    }
  }

  @Test
  public void testSendMessageToStorageNodes() throws Exception {
    // Send message to two storage nodes.
    int timeoutCount = 1;
    // register handler for the channel of storage node
    channel.registerHandler(StoreStatusMessage.class, new TimeoutTestStoreStatusMessageHandler(timeoutCount));

    // Start a new participant
    SafeHelixManager newParticipant = TestUtils.getParticipant(
        cluster,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), port + 1),
        zkAddress,
        port + 1,
        LeaderStandbySMD.name);
    newParticipant.connect();
    HelixStatusMessageChannel newChannel = new HelixStatusMessageChannel(newParticipant, helixMessageChannelStats);
    newChannel.registerHandler(StoreStatusMessage.class, new TimeoutTestStoreStatusMessageHandler(timeoutCount));
    admin.rebalance(cluster, kafkaTopic, 2);

    // Wait until helix has assigned participant to the given resource.
    TestUtils.waitForNonDeterministicCompletion(
        WAIT_ZK_TIME,
        TimeUnit.MILLISECONDS,
        () -> routingDataRepository.containsKafkaTopic(kafkaTopic)
            && routingDataRepository.getWorkingInstances(kafkaTopic, 0).size() == 2);

    HelixStatusMessageChannel controllerChannel =
        getControllerChannel(new TimeoutTestStoreStatusMessageHandler(timeoutCount));
    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    try {
      controllerChannel.sendToStorageNodes(cluster, veniceMessage, kafkaTopic, timeoutCount);
    } catch (VeniceException e) {
      Assert.fail("Sending should be successful after retry " + timeoutCount + " times", e);
    } finally {
      newParticipant.disconnect();
    }
  }

  @Test
  public void testSendMessageToAllLiveInstances() throws Exception {
    int timeoutCount = 0;
    // register handler for the channel of storage node
    channel.registerHandler(StoreStatusMessage.class, new TimeoutTestStoreStatusMessageHandler(timeoutCount));

    // Start a new participant
    SafeHelixManager newParticipant = TestUtils.getParticipant(
        cluster,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), port + 1),
        zkAddress,
        port + 1,
        LeaderStandbySMD.name);
    newParticipant.connect();
    HelixStatusMessageChannel newChannel = new HelixStatusMessageChannel(newParticipant, helixMessageChannelStats);
    boolean[] received = new boolean[1];
    received[0] = false;
    newChannel.registerHandler(StoreStatusMessage.class, message -> received[0] = true);
    // Wait until new instance is connected to zk.
    TestUtils.waitForNonDeterministicCompletion(
        WAIT_ZK_TIME,
        TimeUnit.MILLISECONDS,
        () -> routingDataRepository.isLiveInstance(Utils.getHelixNodeIdentifier(Utils.getHostName(), port + 1)));

    HelixStatusMessageChannel controllerChannel =
        getControllerChannel(new TimeoutTestStoreStatusMessageHandler(timeoutCount));
    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    try {
      controllerChannel.sendToStorageNodes(cluster, veniceMessage, kafkaTopic, timeoutCount);
      Assert.assertTrue(
          received[0],
          "We should send message to all live instance regardless it's assigned to resource or not.");
    } catch (VeniceException e) {
      Assert.fail("Sending should be successful after retry " + timeoutCount + " times", e);
    } finally {
      newParticipant.disconnect();
    }
  }

  @Test
  public void testSendMessageToNodeWithoutRegisteringHandler() throws Exception {
    int timeoutCount = 1;
    // Start a new participant
    SafeHelixManager newParticipant = TestUtils.getParticipant(
        cluster,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), port + 1),
        zkAddress,
        port + 1,
        LeaderStandbySMD.name);
    newParticipant.connect();
    admin.rebalance(cluster, kafkaTopic, 2);

    // Wait until helix has assigned participant to the given resource.
    TestUtils.waitForNonDeterministicCompletion(
        WAIT_ZK_TIME,
        TimeUnit.MILLISECONDS,
        () -> routingDataRepository.containsKafkaTopic(kafkaTopic)
            && routingDataRepository.getWorkingInstances(kafkaTopic, 0).size() == 2);

    HelixStatusMessageChannel controllerChannel =
        getControllerChannel(new TimeoutTestStoreStatusMessageHandler(timeoutCount));
    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    try {
      controllerChannel.sendToStorageNodes(cluster, veniceMessage, kafkaTopic, timeoutCount);
      Assert.fail("Sending should be failed, because storage node have not processed this message.");
    } catch (VeniceException e) {
      // expected.
    } finally {
      newParticipant.disconnect();
    }
  }

  @Test
  public void testSendMessageCrossingClusters() throws Exception {
    int timeoutCount = 1;

    String newCluster = "testSendMessageCrossingClusters";
    admin.addCluster(newCluster);
    HelixConfigScope configScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(newCluster).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(newCluster, LeaderStandbySMD.name, LeaderStandbySMD.build());
    boolean isReceived[] = new boolean[1];
    channel.registerHandler(StoreStatusMessage.class, new StatusMessageHandler<StoreStatusMessage>() {
      @Override
      public void handleMessage(StoreStatusMessage message) {
        isReceived[0] = true;
      }
    });

    String id = Utils.getHelixNodeIdentifier(Utils.getHostName(), port + 10);
    SafeHelixManager newClusterParticipant =
        TestUtils.getParticipant(newCluster, id, zkAddress, port + 10, LeaderStandbySMD.name);
    newClusterParticipant.connect();
    HelixStatusMessageChannel newChannel =
        new HelixStatusMessageChannel(newClusterParticipant, helixMessageChannelStats);
    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    try {
      newChannel.sendToStorageNodes(cluster, veniceMessage, kafkaTopic, timeoutCount);
      Assert.assertTrue(isReceived[0], "Storage node in another cluster should receive the message.");
    } catch (VeniceException e) {
      Assert.fail("Sending should be successful after retry " + timeoutCount + " times", e);
    } finally {
      newClusterParticipant.disconnect();
    }
  }

  /**
   * Handler in controller side used to deal with status update message from storage node.
   */
  private static class FailedTestStoreStatusMessageHandler implements StatusMessageHandler<StoreStatusMessage> {
    private int errorReplyCount;
    private int errorReply = 0;

    public FailedTestStoreStatusMessageHandler(int errorReplyCount) {
      this.errorReplyCount = errorReplyCount;
    }

    @Override
    public void handleMessage(StoreStatusMessage message) {
      if (errorReply == errorReplyCount) {
        // handle message correctly.
      } else {
        errorReply++;
        throw new VeniceException("Failed to handle message." + " ErrorReply #" + errorReply);
      }
    }
  }

  /**
   * Handler in controller side used to deal with status update message from storage node.
   */
  private static class TimeoutTestStoreStatusMessageHandler implements StatusMessageHandler<StoreStatusMessage> {
    private int timeOutReplyCount;
    private int timeOutReply = 0;

    public TimeoutTestStoreStatusMessageHandler(int timeOutReplyCount) {
      this.timeOutReplyCount = timeOutReplyCount;
    }

    @Override
    public void handleMessage(StoreStatusMessage message) {
      if (timeOutReply == timeOutReplyCount) {
        // handle message correctly.
      } else {
        timeOutReply++;
        try {
          Thread.sleep(HelixStatusMessageChannel.DEFAULT_SEND_MESSAGE_TIME_OUT + 300);
        } catch (InterruptedException e) {
        }
      }
    }
  }
}
