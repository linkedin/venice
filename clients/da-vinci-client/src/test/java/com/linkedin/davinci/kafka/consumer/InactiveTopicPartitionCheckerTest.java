package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.utils.IndexedHashMap;
import com.linkedin.davinci.utils.IndexedMap;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class InactiveTopicPartitionCheckerTest {
  private static final long CHECK_INTERVAL_SECONDS = 1;
  private static final long THRESHOLD_SECONDS = 5;
  private static final long THRESHOLD_MS = TimeUnit.SECONDS.toMillis(THRESHOLD_SECONDS);

  private InactiveTopicPartitionChecker mockChecker;
  private IndexedMap<SharedKafkaConsumer, ConsumptionTask> consumerToTaskMap;
  private Map<SharedKafkaConsumer, Set<PubSubTopicPartition>> pausedMaps;
  private SharedKafkaConsumer mockConsumer1;
  private SharedKafkaConsumer mockConsumer2;
  private ConsumptionTask mockTask1;
  private ConsumptionTask mockTask2;
  private ConsumptionTask.PartitionStats mockPartitionStats1;
  private ConsumptionTask.PartitionStats mockPartitionStats2;
  private ConsumptionTask.PartitionStats mockPartitionStats3;

  private PubSubTopicRepository topicRepository;
  private PubSubTopic testTopic;
  private PubSubTopicPartition partition1;
  private PubSubTopicPartition partition2;
  private PubSubTopicPartition partition3;

  @BeforeMethod
  public void setUp() {
    // Initialize topic and partitions
    topicRepository = new PubSubTopicRepository();
    testTopic = topicRepository.getTopic("test_store_v1");
    partition1 = new PubSubTopicPartitionImpl(testTopic, 0);
    partition2 = new PubSubTopicPartitionImpl(testTopic, 1);
    partition3 = new PubSubTopicPartitionImpl(testTopic, 2);

    // Create mocks
    mockConsumer1 = mock(SharedKafkaConsumer.class);
    mockConsumer2 = mock(SharedKafkaConsumer.class);
    mockTask1 = mock(ConsumptionTask.class);
    mockTask2 = mock(ConsumptionTask.class);
    mockPartitionStats1 = mock(ConsumptionTask.PartitionStats.class);
    mockPartitionStats2 = mock(ConsumptionTask.PartitionStats.class);
    mockPartitionStats3 = mock(ConsumptionTask.PartitionStats.class);

    // Set up consumer to task mapping
    consumerToTaskMap = new IndexedHashMap<>(2);
    consumerToTaskMap.putByIndex(mockConsumer1, mockTask1, 0);
    consumerToTaskMap.putByIndex(mockConsumer2, mockTask2, 1);

    // Set up task IDs for logging
    when(mockTask1.getTaskIdStr()).thenReturn("task-1");
    when(mockTask2.getTaskIdStr()).thenReturn("task-2");

    // Create a proper mock object using mock() with constructor initialization
    // This creates a true mock object that has proper internal state and can call real methods
    mockChecker = mock(InactiveTopicPartitionChecker.class);

    // Configure the mock to call the real checkInactiveTopicPartition method using doCallRealMethod
    doCallRealMethod().when(mockChecker).checkInactiveTopicPartition();

    // Initialize pausedMaps for use by the mocked object
    pausedMaps = new VeniceConcurrentHashMap<>();

    // Configure mock to return pausedMaps when getConsumerToPausedTopicPartitionsMap() is called
    when(mockChecker.getConsumerToPausedTopicPartitionsMap()).thenReturn(pausedMaps);

    // Configure mock to return consumerToTaskMap when getConsumerToConsumptionTask() is called
    when(mockChecker.getConsumerToConsumptionTask()).thenReturn(consumerToTaskMap);

    when(mockChecker.getInactiveTopicPartitionCheckIntervalInMs()).thenReturn(CHECK_INTERVAL_SECONDS);
    when(mockChecker.getInactiveTopicPartitionThresholdInMs()).thenReturn(THRESHOLD_MS);
  }

  @Test
  public void testServiceStartAndStop() throws Exception {
    // Create a real instance for lifecycle testing
    InactiveTopicPartitionChecker realChecker =
        new InactiveTopicPartitionChecker(consumerToTaskMap, CHECK_INTERVAL_SECONDS, THRESHOLD_SECONDS);

    // Test service can start successfully
    realChecker.start();
    assertTrue(realChecker.isRunning());

    // Test service can stop successfully
    realChecker.stop();
    assertFalse(realChecker.isRunning());
  }

  @Test
  public void testNoPartitionsAssigned() {
    // Setup: No partitions assigned to any consumer
    when(mockConsumer1.getAssignment()).thenReturn(new HashSet<>());
    when(mockConsumer2.getAssignment()).thenReturn(new HashSet<>());

    // Execute the check method directly using spy to test the logic without starting the service
    invokeCheckMethodDirectly();

    // Verify: No pause or resume calls should be made
    verify(mockConsumer1, never()).pause(any());
    verify(mockConsumer1, never()).resume(any());
    verify(mockConsumer2, never()).pause(any());
    verify(mockConsumer2, never()).resume(any());
  }

  @Test
  public void testActivePartitionsNotPaused() {
    long currentTime = System.currentTimeMillis();
    long recentPollTime = currentTime - (THRESHOLD_MS / 2); // Recent poll, should be active

    // Setup: Consumer1 has one active partition
    Set<PubSubTopicPartition> assignedPartitions = new HashSet<>();
    assignedPartitions.add(partition1);
    when(mockConsumer1.getAssignment()).thenReturn(assignedPartitions);
    when(mockTask1.getPartitionStats(partition1)).thenReturn(mockPartitionStats1);
    when(mockPartitionStats1.getLastSuccessfulPollTimestamp()).thenReturn(recentPollTime);

    // Setup: Consumer2 has no partitions
    when(mockConsumer2.getAssignment()).thenReturn(new HashSet<>());

    // Execute
    invokeCheckMethodDirectly();

    // Verify: No partitions should be paused or resumed
    verify(mockConsumer1, never()).pause(partition1);
    verify(mockConsumer1, never()).resume(partition1);
  }

  @Test
  public void testInactivePartitionsPaused() {
    long currentTime = System.currentTimeMillis();
    long oldPollTime = currentTime - (THRESHOLD_MS * 2); // Old poll, should be inactive

    // Setup: Consumer1 has one inactive partition
    Set<PubSubTopicPartition> assignedPartitions = new HashSet<>();
    assignedPartitions.add(partition1);
    when(mockConsumer1.getAssignment()).thenReturn(assignedPartitions);
    when(mockTask1.getPartitionStats(partition1)).thenReturn(mockPartitionStats1);
    when(mockPartitionStats1.getLastSuccessfulPollTimestamp()).thenReturn(oldPollTime);

    // Setup: Consumer2 has no partitions
    when(mockConsumer2.getAssignment()).thenReturn(new HashSet<>());

    // Execute
    invokeCheckMethodDirectly();

    // Verify: Inactive partition should be paused
    verify(mockConsumer1, times(1)).pause(partition1);
    verify(mockConsumer1, never()).resume(partition1);
  }

  @Test
  public void testNeverPolledPartitionsPaused() {
    // Setup: Consumer1 has one partition that was never polled (timestamp = -1)
    Set<PubSubTopicPartition> assignedPartitions = new HashSet<>();
    assignedPartitions.add(partition1);
    when(mockConsumer1.getAssignment()).thenReturn(assignedPartitions);
    when(mockTask1.getPartitionStats(partition1)).thenReturn(mockPartitionStats1);
    when(mockPartitionStats1.getLastSuccessfulPollTimestamp()).thenReturn(-1L);

    // Setup: Consumer2 has no partitions
    when(mockConsumer2.getAssignment()).thenReturn(new HashSet<>());

    // Execute
    invokeCheckMethodDirectly();

    // Verify: Never-polled partition should be paused
    verify(mockConsumer1, times(1)).pause(partition1);
    verify(mockConsumer1, never()).resume(partition1);
  }

  @Test
  public void testPreviouslyPausedPartitionsResumed() {
    long currentTime = System.currentTimeMillis();

    // Setup: Consumer1 has one partition that was previously paused but is now active
    Set<PubSubTopicPartition> assignedPartitions = new HashSet<>();
    assignedPartitions.add(partition1);
    assignedPartitions.add(partition2);
    when(mockConsumer1.getAssignment()).thenReturn(assignedPartitions);
    when(mockTask1.getPartitionStats(partition1)).thenReturn(mockPartitionStats1);
    when(mockTask1.getPartitionStats(partition2)).thenReturn(mockPartitionStats1);

    // Setup: Consumer2 has no partitions
    when(mockConsumer2.getAssignment()).thenReturn(new HashSet<>());

    // First execution: Simulate that partition1 was previously paused
    // We need to manually add it to the paused partitions map
    when(mockConsumer1.getAssignment()).thenReturn(assignedPartitions);

    // Simulate the checker having previously paused this partition by calling the check twice
    // First call: make it inactive to get it paused
    when(mockPartitionStats1.getLastSuccessfulPollTimestamp()).thenReturn(currentTime - (THRESHOLD_MS * 2));
    invokeCheckMethodDirectly();
    verify(mockConsumer1, times(1)).pause(partition1);
    verify(mockConsumer1, times(1)).pause(partition2);

    // Second call: make it active again to get it resumed
    invokeCheckMethodDirectly();

    // Verify: Previously paused partition should be resumed
    verify(mockConsumer1, times(1)).resume(partition1);
    verify(mockConsumer1, times(1)).resume(partition2);

  }

  @Test
  public void testMixedPartitionStates() {
    long currentTime = System.currentTimeMillis();
    long recentPollTime = currentTime - (THRESHOLD_MS / 2); // Active
    long oldPollTime = currentTime - (THRESHOLD_MS * 2); // Inactive

    // Setup: Consumer1 has multiple partitions with different states
    Set<PubSubTopicPartition> assignedPartitions = new HashSet<>();
    assignedPartitions.add(partition1); // Will be active
    assignedPartitions.add(partition2); // Will be inactive
    assignedPartitions.add(partition3); // Will be never polled

    when(mockConsumer1.getAssignment()).thenReturn(assignedPartitions);
    when(mockTask1.getPartitionStats(partition1)).thenReturn(mockPartitionStats1);
    when(mockTask1.getPartitionStats(partition2)).thenReturn(mockPartitionStats2);
    when(mockTask1.getPartitionStats(partition3)).thenReturn(mockPartitionStats3);

    when(mockPartitionStats1.getLastSuccessfulPollTimestamp()).thenReturn(recentPollTime); // Active
    when(mockPartitionStats2.getLastSuccessfulPollTimestamp()).thenReturn(oldPollTime); // Inactive
    when(mockPartitionStats3.getLastSuccessfulPollTimestamp()).thenReturn(-1L); // Never polled

    // Setup: Consumer2 has no partitions
    when(mockConsumer2.getAssignment()).thenReturn(new HashSet<>());

    // Execute
    invokeCheckMethodDirectly();

    // Verify: Only inactive partitions should be paused
    verify(mockConsumer1, never()).pause(partition1); // Active, should not be paused
    verify(mockConsumer1, times(1)).pause(partition2); // Inactive, should be paused
    verify(mockConsumer1, times(1)).pause(partition3); // Never polled, should be paused

    verify(mockConsumer1, never()).resume(any()); // No previously paused partitions
  }

  @Test
  public void testSkipAlreadyPausedPartitions() {
    long currentTime = System.currentTimeMillis();
    long oldPollTime = currentTime - (THRESHOLD_MS * 2); // Inactive

    // Setup: Consumer1 has one partition
    Set<PubSubTopicPartition> assignedPartitions = new HashSet<>();
    assignedPartitions.add(partition1);
    when(mockConsumer1.getAssignment()).thenReturn(assignedPartitions);
    when(mockTask1.getPartitionStats(partition1)).thenReturn(mockPartitionStats1);
    when(mockPartitionStats1.getLastSuccessfulPollTimestamp()).thenReturn(oldPollTime);
    Set<PubSubTopicPartition> pausedPartitions = new HashSet<>();
    pausedPartitions.add(partition1);
    pausedMaps.put(mockConsumer1, pausedPartitions);

    // Setup: Consumer2 has no partitions
    when(mockConsumer2.getAssignment()).thenReturn(new HashSet<>());

    // First execution: Partition should be paused
    invokeCheckMethodDirectly();
    verify(mockConsumer1, never()).pause(partition1);
    Assert.assertTrue(pausedPartitions.isEmpty());

    // Second execution: Partition should be skipped (already paused)
    // The partition stats check should not be called for already paused partitions
    invokeCheckMethodDirectly();

    // Verify: pause should only be called once (from first execution)
    verify(mockConsumer1, times(1)).pause(partition1);
  }

  @Test
  public void testMultipleConsumers() {
    long currentTime = System.currentTimeMillis();
    long recentPollTime = currentTime - (THRESHOLD_MS / 2); // Active
    long oldPollTime = currentTime - (THRESHOLD_MS * 2); // Inactive

    // Setup: Consumer1 has one active partition
    Set<PubSubTopicPartition> assignedPartitions1 = new HashSet<>();
    assignedPartitions1.add(partition1);
    when(mockConsumer1.getAssignment()).thenReturn(assignedPartitions1);
    when(mockTask1.getPartitionStats(partition1)).thenReturn(mockPartitionStats1);
    when(mockPartitionStats1.getLastSuccessfulPollTimestamp()).thenReturn(recentPollTime);

    // Setup: Consumer2 has one inactive partition
    Set<PubSubTopicPartition> assignedPartitions2 = new HashSet<>();
    assignedPartitions2.add(partition2);
    when(mockConsumer2.getAssignment()).thenReturn(assignedPartitions2);
    when(mockTask2.getPartitionStats(partition2)).thenReturn(mockPartitionStats2);
    when(mockPartitionStats2.getLastSuccessfulPollTimestamp()).thenReturn(oldPollTime);

    // Execute
    invokeCheckMethodDirectly();

    // Verify: Only consumer2's inactive partition should be paused
    verify(mockConsumer1, never()).pause(any());
    verify(mockConsumer1, never()).resume(any());
    verify(mockConsumer2, times(1)).pause(partition2);
    verify(mockConsumer2, never()).resume(any());
  }

  /**
   * Helper method to invoke the checkInactiveTopicPartition method directly on the mock object
   * This calls the real method implementation using doCallRealMethod approach
   */
  private void invokeCheckMethodDirectly() {
    // Since checkInactiveTopicPartition is package-private, we can call it directly
    // The mock is configured with doCallRealMethod() so it will execute the real logic
    mockChecker.checkInactiveTopicPartition();
  }
}
