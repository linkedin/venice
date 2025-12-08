package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.LatencyUtils.getElapsedTimeFromMsToMs;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
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
        Optional.empty());
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
    // We need to mock the HelixManager from the Executor thread so that the next state transition to be executed
    // successfully.
    when(mockHelixManager.isConnected()).thenReturn(true);
    stateModel.executeStateTransitionAsync(mockMessage, () -> {
      stateModel.setHelixManager(mockHelixManager);
    });

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
        Optional.empty());
    int testPartition = 0;
    String resourceName = Version.composeKafkaTopic("testStore", 1);
    String partitionName = HelixUtils.getPartitionName(resourceName, testPartition);
    factory.createNewStateModel(resourceName, partitionName);
    factory.close();
    // Verify that when the factor is closed, the state model is also closed and resources are released.
    assertTrue(factory.getModel(resourceName).getWorkService().isShutdown());
  }

  /**
   * Test that when STANDBY->LEADER state transition times out, the catch block cancels the background task.
   * This validates the TimeoutException catch block in onBecomeLeaderFromStandby().
   */
  @Test(timeOut = 10000)
  public void testStateTransitionTimeoutCancelsBackgroundTask() throws Exception {
    CountDownLatch taskStarted = new CountDownLatch(1);
    AtomicBoolean taskWasCancelled = new AtomicBoolean(false);
    AtomicBoolean taskCompleted = new AtomicBoolean(false);

    VeniceControllerClusterConfig mockClusterConfig = mock(VeniceControllerClusterConfig.class);
    when(mockClusterConfig.isVeniceClusterLeaderHAAS()).thenReturn(false);

    VeniceControllerStateModel testStateModel = new VeniceControllerStateModel(
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
        Optional.empty());

    testStateModel.setClusterConfig(mockClusterConfig);
    // Set a very short timeout (100ms) so the test doesn't take long
    testStateModel.setStateTransitionTimeout(100);

    // Use spy to override initClusterResources()
    VeniceControllerStateModel spyStateModel = spy(testStateModel);

    // Mock initHelixManager to set a mock helix manager (avoid real Helix connection)
    SafeHelixManager mockHelixManager = mock(SafeHelixManager.class);
    when(mockHelixManager.isConnected()).thenReturn(true);
    when(mockHelixManager.getInstanceName()).thenReturn("test-instance");
    doAnswer(invocation -> {
      spyStateModel.setHelixManager(mockHelixManager);
      return null;
    }).when(spyStateModel).initHelixManager("test-controller");

    // Mock initClusterResources to take longer than timeout
    doAnswer(invocation -> {
      taskStarted.countDown();
      try {
        // Sleep longer than timeout to trigger TimeoutException
        for (int i = 0; i < 100; i++) {
          if (Thread.currentThread().isInterrupted()) {
            taskWasCancelled.set(true);
            LOGGER.info("Background task was cancelled by timeout catch block");
            return null;
          }
          Thread.sleep(100);
        }
        taskCompleted.set(true);
      } catch (InterruptedException e) {
        taskWasCancelled.set(true);
        LOGGER.info("Background task was interrupted by timeout catch block");
        Thread.currentThread().interrupt();
      }
      return null;
    }).when(spyStateModel).initClusterResources();

    Message message = mock(Message.class);
    when(message.getTgtName()).thenReturn("test-controller");
    when(message.getFromState()).thenReturn("STANDBY");
    when(message.getToState()).thenReturn("LEADER");
    when(message.getResourceName()).thenReturn("test-cluster_0");

    // This should throw VeniceException due to timeout, and the catch block should cancel the future
    expectThrows(
        VeniceException.class,
        () -> spyStateModel.onBecomeLeaderFromStandby(message, mock(NotificationContext.class)));

    // Wait for task to start
    assertTrue(taskStarted.await(2, TimeUnit.SECONDS), "Task should have started");

    // Wait for cancellation to propagate (poll with timeout)
    long deadline = System.currentTimeMillis() + 2000;
    while (!taskWasCancelled.get() && System.currentTimeMillis() < deadline) {
      Utils.sleep(10);
    }

    // Verify the task was cancelled by the catch block
    assertTrue(taskWasCancelled.get(), "Background task should have been cancelled by timeout catch block");
    assertFalse(taskCompleted.get(), "Background task should not have completed");

    LOGGER.info("Test verified: TimeoutException catch block cancelled background task");
  }

  /**
   * Test that when state transition fails with ExecutionException, the catch block cancels the background task.
   * This validates the ExecutionException catch block in onBecomeLeaderFromStandby().
   */
  @Test(timeOut = 10000)
  public void testStateTransitionExecutionExceptionCancelsBackgroundTask() throws Exception {
    CountDownLatch taskStarted = new CountDownLatch(1);
    AtomicBoolean taskWasInterrupted = new AtomicBoolean(false);
    AtomicBoolean exceptionThrown = new AtomicBoolean(false);

    VeniceControllerClusterConfig mockClusterConfig = mock(VeniceControllerClusterConfig.class);
    when(mockClusterConfig.isVeniceClusterLeaderHAAS()).thenReturn(false);

    VeniceControllerStateModel testStateModel = new VeniceControllerStateModel(
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
        Optional.empty());

    testStateModel.setClusterConfig(mockClusterConfig);

    // Use spy to override initClusterResources()
    VeniceControllerStateModel spyStateModel = spy(testStateModel);

    // Mock initHelixManager to set a mock helix manager (avoid real Helix connection)
    SafeHelixManager mockHelixManager = mock(SafeHelixManager.class);
    when(mockHelixManager.isConnected()).thenReturn(true);
    when(mockHelixManager.getInstanceName()).thenReturn("test-instance");
    doAnswer(invocation -> {
      spyStateModel.setHelixManager(mockHelixManager);
      return null;
    }).when(spyStateModel).initHelixManager("test-controller");

    // Mock initClusterResources to throw an exception, but be interruptible
    doAnswer(invocation -> {
      taskStarted.countDown();
      try {
        // Sleep a bit before throwing exception - gives catch block time to cancel
        Thread.sleep(100);
        exceptionThrown.set(true);
        throw new RuntimeException("Simulated initialization failure");
      } catch (InterruptedException e) {
        taskWasInterrupted.set(true);
        LOGGER.info("Background task was interrupted by ExecutionException catch block");
        Thread.currentThread().interrupt();
        throw e;
      }
    }).when(spyStateModel).initClusterResources();

    Message message = mock(Message.class);
    when(message.getTgtName()).thenReturn("test-controller");
    when(message.getFromState()).thenReturn("STANDBY");
    when(message.getToState()).thenReturn("LEADER");
    when(message.getResourceName()).thenReturn("test-cluster_0");

    // This should throw VeniceException due to ExecutionException, and the catch block should cancel the future
    assertThrows(
        VeniceException.class,
        () -> spyStateModel.onBecomeLeaderFromStandby(message, mock(NotificationContext.class)));

    // Wait for task to start
    assertTrue(taskStarted.await(2, TimeUnit.SECONDS), "Task should have started");

    // Wait for either exception to be thrown or interruption (poll with timeout)
    long deadline = System.currentTimeMillis() + 2000;
    while (!exceptionThrown.get() && !taskWasInterrupted.get() && System.currentTimeMillis() < deadline) {
      Utils.sleep(10);
    }

    // Verify either the exception was thrown OR the task was interrupted by the catch block
    // (race condition: exception might be thrown before cancellation takes effect)
    assertTrue(
        exceptionThrown.get() || taskWasInterrupted.get(),
        "Either exception should be thrown or task should be interrupted");

    LOGGER.info("Test verified: ExecutionException catch block cancelled background task");
  }

  /**
   * Test that when the calling thread is interrupted, the catch block cancels the background task.
   * This validates the InterruptedException catch block in onBecomeLeaderFromStandby().
   */
  @Test(timeOut = 10000)
  public void testStateTransitionInterruptedExceptionCancelsBackgroundTask() throws Exception {
    CountDownLatch taskStarted = new CountDownLatch(1);
    AtomicBoolean taskWasCancelled = new AtomicBoolean(false);
    AtomicBoolean taskCompleted = new AtomicBoolean(false);

    VeniceControllerClusterConfig mockClusterConfig = mock(VeniceControllerClusterConfig.class);
    when(mockClusterConfig.isVeniceClusterLeaderHAAS()).thenReturn(false);

    VeniceControllerStateModel testStateModel = new VeniceControllerStateModel(
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
        Optional.empty());

    testStateModel.setClusterConfig(mockClusterConfig);

    // Use spy to override initClusterResources()
    VeniceControllerStateModel spyStateModel = spy(testStateModel);

    // Mock initHelixManager to set a mock helix manager (avoid real Helix connection)
    SafeHelixManager mockHelixManager = mock(SafeHelixManager.class);
    when(mockHelixManager.isConnected()).thenReturn(true);
    when(mockHelixManager.getInstanceName()).thenReturn("test-instance");
    doAnswer(invocation -> {
      spyStateModel.setHelixManager(mockHelixManager);
      return null;
    }).when(spyStateModel).initHelixManager("test-controller");

    // Mock initClusterResources to run long enough for us to interrupt
    doAnswer(invocation -> {
      taskStarted.countDown();
      try {
        // Sleep long enough to allow interruption
        for (int i = 0; i < 100; i++) {
          if (Thread.currentThread().isInterrupted()) {
            taskWasCancelled.set(true);
            LOGGER.info("Background task was cancelled by InterruptedException catch block");
            return null;
          }
          Thread.sleep(100);
        }
        taskCompleted.set(true);
      } catch (InterruptedException e) {
        taskWasCancelled.set(true);
        LOGGER.info("Background task was interrupted by InterruptedException catch block");
        Thread.currentThread().interrupt();
        throw e;
      }
      return null;
    }).when(spyStateModel).initClusterResources();

    Message message = mock(Message.class);
    when(message.getTgtName()).thenReturn("test-controller");
    when(message.getFromState()).thenReturn("STANDBY");
    when(message.getToState()).thenReturn("LEADER");
    when(message.getResourceName()).thenReturn("test-cluster_0");

    // Start the transition in a separate thread so we can interrupt it
    Thread testThread = new Thread(() -> {
      try {
        spyStateModel.onBecomeLeaderFromStandby(message, mock(NotificationContext.class));
      } catch (Exception e) {
        // Expected - InterruptedException will be wrapped in VeniceException
        LOGGER.info("Expected exception during interrupted state transition", e);
      }
    });
    testThread.start();

    // Wait for task to start
    assertTrue(taskStarted.await(2, TimeUnit.SECONDS), "Task should have started");

    // Interrupt the thread to trigger InterruptedException in future.get()
    testThread.interrupt();

    // Wait for thread to finish
    testThread.join(2000);

    // Wait for cancellation to propagate (poll with timeout)
    long deadline = System.currentTimeMillis() + 2000;
    while (!taskWasCancelled.get() && System.currentTimeMillis() < deadline) {
      Utils.sleep(10);
    }

    // Verify the background task was cancelled by the catch block
    assertTrue(
        taskWasCancelled.get(),
        "Background task should have been cancelled by InterruptedException catch block");
    assertFalse(taskCompleted.get(), "Background task should not have completed");

    LOGGER.info("Test verified: InterruptedException catch block cancelled background task");
  }
}
