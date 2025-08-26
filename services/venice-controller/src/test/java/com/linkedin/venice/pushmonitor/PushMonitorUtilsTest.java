package com.linkedin.venice.pushmonitor;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PushMonitorUtilsTest {
  @Test
  public void testCompleteStatusCanBeReportedWithOfflineInstancesBelowFailFastThreshold() {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushMonitorUtils.setDVCDeadInstanceTime("store_v1", System.currentTimeMillis());
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);
    /**
      * Instance a is offline and its push status is not completed.
      * Instance b,c,d are online and their push status is completed.
      * In this case, the overall DaVinci push status can be COMPLETED as long as 1 is below the fail fast threshold.
      */
    doReturn(PushStatusStoreReader.InstanceStatus.DEAD).when(reader).getInstanceStatus(eq("store"), eq("a"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("b"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("c"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("d"));
    // Bootstrapping nodes should be ignored
    doReturn(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING).when(reader).getInstanceStatus(eq("store"), eq("e"));
    doReturn(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING).when(reader).getInstanceStatus(eq("store"), eq("f"));
    doReturn(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING).when(reader).getInstanceStatus(eq("store"), eq("g"));
    doReturn(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING).when(reader).getInstanceStatus(eq("store"), eq("h"));

    Map<CharSequence, Integer> map = new HashMap<>();
    /**
     * 2 is STARTED state, 10 is COMPLETED state.
     * Reference: {@link ExecutionStatus}
     */
    map.put("a", 2);
    map.put("b", 10);
    map.put("c", 10);
    map.put("d", 10);
    map.put("e", 2);
    map.put("f", 2);
    map.put("g", 2);
    map.put("h", 2);

    // Test partition level key first
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());
    doReturn(map).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());

    // 1 offline instances is below the fail fast threshold, the overall DaVinci status can be COMPLETED.
    validatePushStatus(reader, "store_v1", Optional.empty(), 2, 0.25, ExecutionStatus.COMPLETED);

    // Test version level key
    doReturn(map).when(reader).getVersionStatus("store", 1, Optional.empty());
    doReturn(Collections.emptyMap()).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    // 1 offline instances is below the fail fast threshold, the overall DaVinci status can be COMPLETED.
    validatePushStatus(reader, "store_v1", Optional.empty(), 2, 0.25, ExecutionStatus.COMPLETED);

    // Test migration case: node "b" and "c" were reporting partition status at STARTED state, but later it completed
    // and reported
    // version level key status as COMPLETED.
    Map<CharSequence, Integer> retiredStatusMap = new HashMap<>();
    retiredStatusMap.put("b", 2);
    retiredStatusMap.put("c", 2);
    doReturn(retiredStatusMap).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    validatePushStatus(reader, "store_v1", Optional.empty(), 2, 0.25, ExecutionStatus.COMPLETED);

    // Test partition level key for incremental push
    String incrementalPushVersion = "incrementalPushVersion";
    Map<CharSequence, Integer> incrementalPushStatusMap = new HashMap<>();
    /**
     * 7 is START_OF_INCREMENTAL_PUSH_RECEIVED state, 8 is END_OF_INCREMENTAL_PUSH_RECEIVED state.
     * Reference: {@link ExecutionStatus}
     */
    incrementalPushStatusMap.put("a", 7);
    incrementalPushStatusMap.put("b", 8);
    incrementalPushStatusMap.put("c", 8);
    incrementalPushStatusMap.put("d", 8);
    incrementalPushStatusMap.put("e", 7);
    incrementalPushStatusMap.put("f", 7);
    incrementalPushStatusMap.put("g", 7);
    incrementalPushStatusMap.put("h", 7);

    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.of(incrementalPushVersion));
    doReturn(incrementalPushStatusMap).when(reader)
        .getPartitionStatus("store", 1, 0, Optional.of(incrementalPushVersion));
    // 1 offline instances is below the fail fast threshold, the overall DaVinci status can be
    // END_OF_INCREMENTAL_PUSH_RECEIVED.
    validatePushStatus(
        reader,
        "store_v1",
        Optional.of(incrementalPushVersion),
        2,
        0.25,
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);

    // Test version level key for incremental push
    doReturn(incrementalPushStatusMap).when(reader).getVersionStatus("store", 1, Optional.of(incrementalPushVersion));
    doReturn(Collections.emptyMap()).when(reader)
        .getPartitionStatus("store", 1, 0, Optional.of(incrementalPushVersion));
    // 1 offline instances is below the fail fast threshold, the overall DaVinci status can be
    // END_OF_INCREMENTAL_PUSH_RECEIVED.
    validatePushStatus(
        reader,
        "store_v1",
        Optional.of(incrementalPushVersion),
        2,
        0.25,
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);

    // Test migration case: node "b" and "c" were reporting partition status at START_OF_INCREMENTAL_PUSH_RECEIVED
    // state, but later it completed and reported version level key status as END_OF_INCREMENTAL_PUSH_RECEIVED.
    Map<CharSequence, Integer> retiredIncrementalPushStatusMap = new HashMap<>();
    retiredIncrementalPushStatusMap.put("b", 7);
    retiredIncrementalPushStatusMap.put("c", 8);
    doReturn(retiredIncrementalPushStatusMap).when(reader)
        .getPartitionStatus("store", 1, 0, Optional.of(incrementalPushVersion));
    validatePushStatus(
        reader,
        "store_v1",
        Optional.of(incrementalPushVersion),
        2,
        0.25,
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDaVinciPushStatusScan(boolean useDaVinciSpecificExecutionStatusForError) {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("a"));
    doReturn(PushStatusStoreReader.InstanceStatus.DEAD).when(reader).getInstanceStatus(eq("store"), eq("b"));
    doReturn(PushStatusStoreReader.InstanceStatus.DEAD).when(reader).getInstanceStatus(eq("store"), eq("c"));
    doReturn(PushStatusStoreReader.InstanceStatus.DEAD).when(reader).getInstanceStatus(eq("store"), eq("d"));

    Map<CharSequence, Integer> map = new HashMap<>();
    map.put("a", 3);
    map.put("b", 3);
    map.put("c", 3);
    map.put("d", 10);
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());
    doReturn(null).when(reader).getVersionStatus("store", 2, Optional.empty());
    doReturn(map).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    doReturn(map).when(reader).getPartitionStatus("store", 2, 0, Optional.empty());

    Set<String> offlineInstances = new HashSet<>();
    offlineInstances.add("b");
    offlineInstances.add("c");
    /**
     * Testing count-based threshold.
     */
    // Valid, because we have 4 replicas, 1 completed, 2 offline, 1 online, threshold number is max(2, 0.25*4) = 2.
    validateOfflineReplicaInPushStatusWhenBreachingFailFastThreshold(
        reader,
        "store_v1",
        2,
        0.25,
        ExecutionStatus.STARTED,
        null,
        useDaVinciSpecificExecutionStatusForError);

    // Expected to fail.
    validateOfflineReplicaInPushStatusWhenBreachingFailFastThreshold(
        reader,
        "store_v1",
        1,
        0.25,
        useDaVinciSpecificExecutionStatusForError
            ? ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES
            : ExecutionStatus.ERROR,
        "Too many dead instances: 2, total instances: 4, example offline instances: " + offlineInstances,
        useDaVinciSpecificExecutionStatusForError);

    /**
     * Testing ratio-based threshold.
     */
    // Valid, because we have 4 replicas, 1 completed, 2 offline, 1 online, threshold number is max(1, 0.5*4) = 2.
    validateOfflineReplicaInPushStatusWhenBreachingFailFastThreshold(
        reader,
        "store_v2",
        1,
        0.5,
        ExecutionStatus.STARTED,
        null,
        useDaVinciSpecificExecutionStatusForError);
    // Expected to fail.
    validateOfflineReplicaInPushStatusWhenBreachingFailFastThreshold(
        reader,
        "store_v2",
        1,
        0.25,
        useDaVinciSpecificExecutionStatusForError
            ? ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES
            : ExecutionStatus.ERROR,
        "Too many dead instances: 2, total instances: 4, example offline instances: " + offlineInstances,
        useDaVinciSpecificExecutionStatusForError);
  }

  private void validateOfflineReplicaInPushStatusWhenBreachingFailFastThreshold(
      PushStatusStoreReader reader,
      String topicName,
      int maxOfflineInstanceCount,
      double maxOfflineInstanceRatio,
      ExecutionStatus expectedStatus,
      String expectedErrorDetails,
      boolean useDaVinciSpecificExecutionStatusForError) {
    /**
     * Even if offline instances number exceed the max offline threshold count it will remain STARTED for the first check,
     * as we need to wait until daVinciErrorInstanceWaitTime has passed since it first occurs.
     */
    ExecutionStatusWithDetails executionStatusWithDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        topicName,
        1,
        Optional.empty(),
        maxOfflineInstanceCount,
        maxOfflineInstanceRatio,
        useDaVinciSpecificExecutionStatusForError);

    System.out.println(executionStatusWithDetails.getDetails());
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.STARTED);
    // Sleep 1ms and try again.
    Utils.sleep(1);
    executionStatusWithDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        topicName,
        1,
        Optional.empty(),
        maxOfflineInstanceCount,
        maxOfflineInstanceRatio,
        useDaVinciSpecificExecutionStatusForError);
    System.out.println(executionStatusWithDetails.getDetails());
    Assert.assertEquals(executionStatusWithDetails.getStatus(), expectedStatus);
    if (expectedStatus.isError()) {
      Assert.assertEquals(executionStatusWithDetails.getDetails(), expectedErrorDetails);
    }
  }

  private void validatePushStatus(
      PushStatusStoreReader reader,
      String topicName,
      Optional<String> incrementalPushVersion,
      int maxOfflineInstanceCount,
      double maxOfflineInstanceRatio,
      ExecutionStatus expectedStatus) {
    ExecutionStatusWithDetails executionStatusWithDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        topicName,
        1,
        incrementalPushVersion,
        maxOfflineInstanceCount,
        maxOfflineInstanceRatio,
        true);
    System.out.println(executionStatusWithDetails.getDetails());
    Assert.assertEquals(executionStatusWithDetails.getStatus(), expectedStatus);
  }

  @Test
  public void testIncompletePartitionDetailsWithInstances() {
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);

    // Mock partition status with different execution statuses
    Map<CharSequence, Integer> partition0Status = new HashMap<>();
    partition0Status.put("instance1", 2); // STARTED
    partition0Status.put("instance2", 2); // STARTED
    partition0Status.put("bootstrappingInstance", 2); // STARTED but should be ignored

    Map<CharSequence, Integer> partition1Status = new HashMap<>();
    partition1Status.put("instance3", 10); // COMPLETED
    partition1Status.put("instance4", 2); // STARTED

    Map<CharSequence, Integer> partition2Status = new HashMap<>();
    partition2Status.put("instance5", 2); // STARTED
    partition1Status.put("instance6", 10); // COMPLETED

    // Mock the reader to return different statuses for different partitions
    doReturn(partition0Status).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    doReturn(partition1Status).when(reader).getPartitionStatus("store", 1, 1, Optional.empty());
    doReturn(partition2Status).when(reader).getPartitionStatus("store", 1, 2, Optional.empty());

    // Mock version status to return null so it falls back to partition level
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());

    // Mock instance status for all instances
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance1");
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance2");
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance3");
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance4");
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance5");
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance6");
    doReturn(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING).when(reader)
        .getInstanceStatus("store", "bootstrappingInstance");

    // Set instances to ignore
    Set<CharSequence> instancesToIgnore = new HashSet<>();
    instancesToIgnore.add("bootstrappingInstance");

    // Call the method under test
    ExecutionStatusWithDetails result = PushMonitorUtils.getDaVinciPartitionLevelPushStatusAndDetails(
        reader,
        "store_v1",
        3, // 3 partitions
        Optional.empty(),
        2, // maxOfflineInstanceCount
        0.25, // maxOfflineInstanceRatio
        true, // useDaVinciSpecificExecutionStatusForError
        instancesToIgnore);

    // Verify the result contains the expected details
    String details = result.getDetails();
    System.out.println("Details: " + details);

    // Parse and validate the partition details structure
    Set<String> partition0Instances = new HashSet<>();
    partition0Instances.add("instance1");
    partition0Instances.add("instance2");
    validatePartitionInstances(details, 0, partition0Instances);

    Set<String> partition1Instances = new HashSet<>();
    partition1Instances.add("instance4");
    validatePartitionInstances(details, 1, partition1Instances);

    Set<String> partition2Instances = new HashSet<>();
    partition2Instances.add("instance5");
    validatePartitionInstances(details, 2, partition2Instances);

    // Verify that bootstrapping instance is not included
    Assert.assertFalse(details.contains("bootstrappingInstance"));

    // Verify that completed instances are not included
    Assert.assertFalse(details.contains("instance3"));
    Assert.assertFalse(details.contains("instance6"));
  }

  /**
   * Helper method to validate that a partition contains the expected instances.
   * This method parses the details string to find the specific partition and validates
   * that it contains exactly the expected instances, regardless of order.
   */
  private void validatePartitionInstances(String details, int partitionId, Set<String> expectedInstances) {
    // Find the partition section
    String partitionPattern = "Partition: " + partitionId + " \\(instances: \\[(.*?)\\]\\)";
    Pattern pattern = Pattern.compile(partitionPattern);
    Matcher matcher = pattern.matcher(details);

    Assert.assertTrue(matcher.find(), "Partition: " + partitionId + " not found in details: " + details);

    // Extract the instances string and parse it
    String instancesStr = matcher.group(1);
    Set<String> actualInstances = new HashSet<>();

    if (!instancesStr.trim().isEmpty()) {
      String[] instanceArray = instancesStr.split(", ");
      for (String instance: instanceArray) {
        actualInstances.add(instance.trim());
      }
    }

    // Validate that the actual instances match the expected ones (order doesn't matter)
    Assert.assertEquals(
        actualInstances,
        expectedInstances,
        "Partition: " + partitionId + " instances mismatch. Expected: " + expectedInstances + ", Actual: "
            + actualInstances);
  }

  @Test
  public void testIncompletePartitionDetailsWithMoreThan10Partitions() {
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);

    // Create more than 10 partitions to test the capping logic
    for (int i = 0; i < 15; i++) {
      Map<CharSequence, Integer> partitionStatus = new HashMap<>();
      partitionStatus.put("instance" + i, 2); // STARTED status
      doReturn(partitionStatus).when(reader).getPartitionStatus("store", 1, i, Optional.empty());
    }

    // Mock version status to return null so it falls back to partition level
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());

    // Mock instance status for all instances
    for (int i = 0; i < 15; i++) {
      doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance" + i);
    }

    // Call the method under test
    ExecutionStatusWithDetails result = PushMonitorUtils.getDaVinciPartitionLevelPushStatusAndDetails(
        reader,
        "store_v1",
        15, // 15 partitions
        Optional.empty(),
        2, // maxOfflineInstanceCount
        0.25, // maxOfflineInstanceRatio
        true, // useDaVinciSpecificExecutionStatusForError
        Collections.emptySet()); // no instances to ignore

    // Verify that only partition IDs are shown (capped at 10) when more than 10 partitions
    String details = result.getDetails();
    Assert.assertTrue(details.contains("[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]"));
    Assert.assertFalse(details.contains("Partition 10"));
    Assert.assertFalse(details.contains("instances:"));
  }

  @Test
  public void testIncompletePartitionDetailsWithExactly10Partitions() {
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);

    // Create exactly 10 partitions to test the boundary case
    for (int i = 0; i < 10; i++) {
      Map<CharSequence, Integer> partitionStatus = new HashMap<>();
      partitionStatus.put("instance" + i, 2); // STARTED status
      doReturn(partitionStatus).when(reader).getPartitionStatus("store", 1, i, Optional.empty());
    }

    // Mock version status to return null so it falls back to partition level
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());

    // Mock instance status for all instances
    for (int i = 0; i < 10; i++) {
      doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance" + i);
    }

    // Call the method under test
    ExecutionStatusWithDetails result = PushMonitorUtils.getDaVinciPartitionLevelPushStatusAndDetails(
        reader,
        "store_v1",
        10, // exactly 10 partitions
        Optional.empty(),
        2, // maxOfflineInstanceCount
        0.25, // maxOfflineInstanceRatio
        true, // useDaVinciSpecificExecutionStatusForError
        Collections.emptySet()); // no instances to ignore

    // Verify that instance details are shown for exactly 10 partitions
    String details = result.getDetails();

    // Validate a few key partitions to ensure instance details are shown
    Set<String> partition0Instances = new HashSet<>();
    partition0Instances.add("instance0");
    validatePartitionInstances(details, 0, partition0Instances);

    Set<String> partition9Instances = new HashSet<>();
    partition9Instances.add("instance9");
    validatePartitionInstances(details, 9, partition9Instances);
    Assert.assertTrue(details.contains("instances:"));
  }

  @Test
  public void testIncompletePartitionDetailsWithMixedStatuses() {
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);

    // Create partitions with mixed execution statuses
    Map<CharSequence, Integer> partition0Status = new HashMap<>();
    partition0Status.put("instance1", 2); // STARTED
    partition0Status.put("instance2", 10); // COMPLETED
    partition0Status.put("instance3", 3); // ERROR

    Map<CharSequence, Integer> partition1Status = new HashMap<>();
    partition1Status.put("instance4", 2); // STARTED
    partition1Status.put("instance5", 2); // STARTED

    doReturn(partition0Status).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    doReturn(partition1Status).when(reader).getPartitionStatus("store", 1, 1, Optional.empty());

    // Mock version status to return null so it falls back to partition level
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());

    // Mock instance status for all instances
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance1");
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance2");
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance3");
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance4");
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus("store", "instance5");

    // Call the method under test
    ExecutionStatusWithDetails result = PushMonitorUtils.getDaVinciPartitionLevelPushStatusAndDetails(
        reader,
        "store_v1",
        2, // 2 partitions
        Optional.empty(),
        2, // maxOfflineInstanceCount
        0.25, // maxOfflineInstanceRatio
        true, // useDaVinciSpecificExecutionStatusForError
        Collections.emptySet()); // no instances to ignore

    // Verify that only non-completed instances are included
    String details = result.getDetails();

    // Validate that partitions contain the correct instances
    Set<String> partition0Instances = new HashSet<>();
    partition0Instances.add("instance1");
    partition0Instances.add("instance3");
    validatePartitionInstances(details, 0, partition0Instances);

    Set<String> partition1Instances = new HashSet<>();
    partition1Instances.add("instance4");
    partition1Instances.add("instance5");
    validatePartitionInstances(details, 1, partition1Instances);

    // Verify that completed instance is not included
    Assert.assertFalse(details.contains("instance2"));
  }
}
