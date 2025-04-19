package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.LatencyUtils.getElapsedTimeFromMsToMs;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestVeniceControllerStateModel {
  private static final Logger LOGGER = LogManager.getLogger(TestVeniceControllerStateModel.class);
  private VeniceControllerStateModel stateModel;
  private Message mockMessage;
  private NotificationContext mockContext;

  private HelixVeniceClusterResources mockClusterResources;
  private VeniceControllerMultiClusterConfig mockMultiClusterConfig;
  private SafeHelixManager mockHelixManager;
  private static final String TOPIC_NAME = "test_v1";

  @BeforeMethod
  public void setUp() {
    // Mock dependencies
    mockMessage = mock(Message.class);
    mockContext = mock(NotificationContext.class);
    mockClusterResources = mock(HelixVeniceClusterResources.class);
    mockMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);
    mockHelixManager = mock(SafeHelixManager.class);

    // Initialize VeniceControllerStateModel with mocked dependencies
    stateModel = new VeniceControllerStateModel(
        "test-cluster",
        mock(ZkClient.class),
        mock(HelixAdapterSerializer.class),
        mockMultiClusterConfig,
        mock(VeniceHelixAdmin.class),
        mock(MetricsRepository.class),
        mock(ClusterLeaderInitializationRoutine.class),
        mock(RealTimeTopicSwitcher.class),
        Optional.empty(),
        mock(HelixAdminClient.class));
  }

  @Test
  public void testOnLeaderStateTransitionBehaviour() {
    final long DELAY = 3000; // 3 seconds delay
    // Mock message behavior
    when(mockMessage.getTgtName()).thenReturn("test-controller");
    when(mockMessage.getFromState()).thenReturn("LEADER");
    when(mockMessage.getToState()).thenReturn("STANDBY");
    when(mockMessage.getResourceName()).thenReturn(TOPIC_NAME);
    doAnswer(invocation -> {
      // Simulate a long delay behavior
      LOGGER.info("Simulating a long delay in stopLeakedPushStatusCleanUpService ...");
      Thread.sleep(DELAY);
      return null;
    }).when(mockClusterResources).stopLeakedPushStatusCleanUpService();
    stateModel.setClusterResources(mockClusterResources);

    // 1st state transition. It should run asynchronously and not block the main thread.
    // We expect the main thread to finish in less than DELAY milliseconds.
    long startTime = System.currentTimeMillis();
    stateModel.onBecomeStandbyFromLeader(mockMessage, mockContext);
    long elapsedTime = getElapsedTimeFromMsToMs(startTime);
    LOGGER.info("Elapsed time for the first state transition: {} ms", elapsedTime);
    assertTrue(
        elapsedTime < DELAY,
        String.format(
            "Controller Leader -> Standby ST is executed asynchronously. Expected a delay of less than %d seconds",
            DELAY / 1000));
    stateModel.setClusterConfig(mock(VeniceControllerClusterConfig.class));

    // This is a workaround for the test.
    // We need to mock the HelixManager from the Executor thread so that the 3rd state transition to be executed
    // successfully.
    when(mockHelixManager.isConnected()).thenReturn(true);
    stateModel.executeStateTransition(mockMessage, () -> {
      stateModel.setHelixManager(mockHelixManager);
    }, true);

    // 2nd state transition. It runs synchronously and should block the main thread.
    // We expect the main thread to take more than DELAY milliseconds to finish it as it has to wait for the 1st
    // state transition to finish.
    stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext);
    elapsedTime = getElapsedTimeFromMsToMs(startTime);
    LOGGER.info("Elapsed time for the second state transition: {} ms", elapsedTime);
    assertTrue(
        elapsedTime >= DELAY,
        String.format(
            "Controller Standby -> Leader ST is executed synchronously. Expected a delay of more than %d seconds",
            DELAY / 1000));
  }
}
