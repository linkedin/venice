package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.LatencyUtils.getElapsedTimeFromMsToMs;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.HelixUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
        mock(HelixAdminClient.class),
        Optional.empty(),
        Optional.empty());
  }

  @Test
  public void testOnLeaderStateTransitionBehaviour() throws Exception {
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
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenReturn(TimeUnit.SECONDS.toMillis(10));
    stateModel.setClusterConfig(clusterConfig);

    stateModel = spy(stateModel);
    when(mockHelixManager.isConnected()).thenReturn(true);
    when(mockHelixManager.getInstanceName()).thenReturn("test-instance");
    doAnswer(invocation -> {
      stateModel.setHelixManager(mockHelixManager);
      return null;
    }).when(stateModel).initHelixManager("test-controller");
    doNothing().when(stateModel).initClusterResources();

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

  @Test
  public void testStateModelClose() {
    VeniceDistClusterControllerStateModelFactory factory = new VeniceDistClusterControllerStateModelFactory(
        mock(ZkClient.class),
        mock(HelixAdapterSerializer.class),
        mock(VeniceHelixAdmin.class),
        mock(VeniceControllerMultiClusterConfig.class),
        mock(MetricsRepository.class),
        mock(ClusterLeaderInitializationRoutine.class),
        mock(RealTimeTopicSwitcher.class),
        Optional.empty(),
        mock(HelixAdminClient.class),
        Optional.empty(),
        Optional.empty());
    int testPartition = 0;
    String resourceName = Version.composeKafkaTopic("testStore", 1);
    String partitionName = HelixUtils.getPartitionName(resourceName, testPartition);
    factory.createNewStateModel(resourceName, partitionName);
    factory.close();
    // Verify that when the factor is closed, the state model is also closed and resources are released.
    assertTrue(factory.getModel(resourceName).getWorkService().isShutdown());
  }

  @Test(timeOut = 10000)
  public void testStateTransitionTimeoutCancelsBackgroundTask() throws Exception {
    CountDownLatch initializationStarted = new CountDownLatch(1);
    CountDownLatch initializationInterrupted = new CountDownLatch(1);
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenAnswer(invocation -> {
      assertTrue(initializationStarted.await(5, TimeUnit.SECONDS));
      return 100L;
    });
    stateModel.setClusterConfig(clusterConfig);
    configureLeaderTransitionMessage();

    stateModel = spy(stateModel);
    SafeHelixManager initializedManager = mockConnectedHelixManager("test-instance");
    doAnswer(invocation -> {
      stateModel.setHelixManager(initializedManager);
      return null;
    }).when(stateModel).initHelixManager("test-controller");
    doAnswer(invocation -> {
      initializationStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        initializationInterrupted.countDown();
        Thread.currentThread().interrupt();
        throw e;
      }
      return null;
    }).when(stateModel).initClusterResources();

    assertThrows(VeniceException.class, () -> stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext));

    assertTrue(initializationInterrupted.await(5, TimeUnit.SECONDS));
    verify(clusterConfig).getControllerStandbyToLeaderTransitionTimeoutMs();
    stateModel.reset();
  }

  @Test(timeOut = 10000)
  public void testStateTransitionExecutionException() throws Exception {
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenReturn(TimeUnit.SECONDS.toMillis(5));
    stateModel.setClusterConfig(clusterConfig);
    configureLeaderTransitionMessage();

    stateModel = spy(stateModel);
    SafeHelixManager initializedManager = mockConnectedHelixManager("test-instance");
    doAnswer(invocation -> {
      stateModel.setHelixManager(initializedManager);
      return null;
    }).when(stateModel).initHelixManager("test-controller");
    doAnswer(invocation -> {
      throw new VeniceException("Resource initialization failed");
    }).when(stateModel).initClusterResources();

    assertThrows(VeniceException.class, () -> stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext));
    stateModel.reset();
  }

  @Test(timeOut = 10000)
  public void testInterruptedTransitionPreservesInterruptStatus() throws Exception {
    CountDownLatch initializationStarted = new CountDownLatch(1);
    CountDownLatch initializationInterrupted = new CountDownLatch(1);
    CountDownLatch transitionFinished = new CountDownLatch(1);
    AtomicBoolean transitionFailed = new AtomicBoolean(false);
    AtomicBoolean interruptStatusPreserved = new AtomicBoolean(false);
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenReturn(TimeUnit.SECONDS.toMillis(30));
    stateModel.setClusterConfig(clusterConfig);
    configureLeaderTransitionMessage();

    stateModel = spy(stateModel);
    SafeHelixManager initializedManager = mockConnectedHelixManager("test-instance");
    doAnswer(invocation -> {
      stateModel.setHelixManager(initializedManager);
      return null;
    }).when(stateModel).initHelixManager("test-controller");
    doAnswer(invocation -> {
      initializationStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        initializationInterrupted.countDown();
        Thread.currentThread().interrupt();
        throw e;
      }
      return null;
    }).when(stateModel).initClusterResources();

    Thread transitionThread = new Thread(() -> {
      try {
        stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext);
      } catch (VeniceException e) {
        transitionFailed.set(true);
        interruptStatusPreserved.set(Thread.currentThread().isInterrupted());
      } finally {
        transitionFinished.countDown();
      }
    });
    transitionThread.start();

    assertTrue(initializationStarted.await(5, TimeUnit.SECONDS));
    transitionThread.interrupt();
    assertTrue(transitionFinished.await(5, TimeUnit.SECONDS));
    assertTrue(initializationInterrupted.await(5, TimeUnit.SECONDS));
    assertTrue(transitionFailed.get());
    assertTrue(interruptStatusPreserved.get());
    stateModel.reset();
  }

  @Test
  public void testResetDisconnectsManagerWithoutClusterResources() {
    SafeHelixManager manager = mockConnectedHelixManager("test-instance");
    stateModel.setHelixManager(manager);

    stateModel.reset();

    verify(manager).disconnect();
  }

  @Test(timeOut = 10000)
  public void testResetWaitsForInitializationIgnoringInterruption() throws Exception {
    CountDownLatch initializationStarted = new CountDownLatch(1);
    CountDownLatch initializationInterrupted = new CountDownLatch(1);
    CountDownLatch allowInitializationToFinish = new CountDownLatch(1);
    CountDownLatch resetStarted = new CountDownLatch(1);
    CountDownLatch resetFinished = new CountDownLatch(1);
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenAnswer(invocation -> {
      assertTrue(initializationStarted.await(5, TimeUnit.SECONDS));
      return 100L;
    });
    stateModel.setClusterConfig(clusterConfig);
    configureLeaderTransitionMessage();

    SafeHelixManager manager = mockConnectedHelixManager("test-instance");
    HelixVeniceClusterResources resources = mock(HelixVeniceClusterResources.class);
    stateModel = spy(stateModel);
    doAnswer(invocation -> {
      stateModel.setHelixManager(manager);
      return null;
    }).when(stateModel).initHelixManager("test-controller");
    doAnswer(invocation -> {
      stateModel.setClusterResources(resources);
      initializationStarted.countDown();
      awaitIgnoringInterrupt(allowInitializationToFinish, initializationInterrupted);
      return null;
    }).when(stateModel).initClusterResources();

    assertThrows(VeniceException.class, () -> stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext));
    assertTrue(initializationInterrupted.await(5, TimeUnit.SECONDS));

    Thread resetThread = new Thread(() -> {
      resetStarted.countDown();
      stateModel.reset();
      resetFinished.countDown();
    });
    resetThread.start();
    assertTrue(resetStarted.await(5, TimeUnit.SECONDS));
    assertFalse(resetFinished.await(200, TimeUnit.MILLISECONDS));
    verify(manager, never()).disconnect();

    allowInitializationToFinish.countDown();
    assertTrue(resetFinished.await(5, TimeUnit.SECONDS));
    verify(resources).clear();
    verify(manager).disconnect();
  }

  @Test(timeOut = 10000)
  public void testStaleManagerFailsTransitionUntilReset() throws Exception {
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenReturn(TimeUnit.SECONDS.toMillis(5));
    stateModel.setClusterConfig(clusterConfig);
    configureLeaderTransitionMessage();

    SafeHelixManager staleManager = mockConnectedHelixManager("stale-instance");
    SafeHelixManager newManager = mockConnectedHelixManager("new-instance");
    stateModel.setHelixManager(staleManager);
    stateModel = spy(stateModel);
    doAnswer(invocation -> {
      stateModel.setHelixManager(newManager);
      return null;
    }).when(stateModel).initHelixManager("test-controller");
    doNothing().when(stateModel).initClusterResources();

    assertThrows(VeniceException.class, () -> stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext));
    verify(stateModel, never()).initHelixManager("test-controller");

    stateModel.reset();
    stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext);

    verify(staleManager).disconnect();
    verify(stateModel).initHelixManager("test-controller");
    stateModel.reset();
  }

  private void configureLeaderTransitionMessage() {
    when(mockMessage.getTgtName()).thenReturn("test-controller");
    when(mockMessage.getFromState()).thenReturn("STANDBY");
    when(mockMessage.getToState()).thenReturn("LEADER");
    when(mockMessage.getResourceName()).thenReturn("test-cluster_0");
  }

  private SafeHelixManager mockConnectedHelixManager(String instanceName) {
    SafeHelixManager helixManager = mock(SafeHelixManager.class);
    when(helixManager.isConnected()).thenReturn(true);
    when(helixManager.getInstanceName()).thenReturn(instanceName);
    return helixManager;
  }

  private void awaitIgnoringInterrupt(CountDownLatch latch, CountDownLatch interrupted) {
    while (true) {
      try {
        latch.await();
        return;
      } catch (InterruptedException e) {
        interrupted.countDown();
      }
    }
  }
}
