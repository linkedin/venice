package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controller.HelixAdminClient;
import com.linkedin.venice.controller.stats.DisabledPartitionStats;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Reproduces the stale reference bug in {@link AbstractPushMonitor#loadAllPushes}.
 *
 * <h3>The Bug</h3>
 * During controller restart, {@code loadAllPushes()} iterates over bulk-loaded
 * {@link OfflinePushStatus} objects (snapshot T1). For each push it:
 * <ol>
 *   <li>Puts the stale T1 object into {@code topicToPushMap}</li>
 *   <li>Calls {@code updateOfflinePush(topic)} which reads <b>fresh</b> data
 *       from ZK (snapshot T2, where all replicas are COMPLETED) and <b>replaces</b>
 *       the entry in {@code topicToPushMap} with a new object</li>
 *   <li>Checks {@code offlinePushStatus.getCurrentStatus().isTerminal()} using
 *       the <b>stale loop variable</b> (T1), NOT the refreshed object from the map</li>
 *   <li>Calls {@code checkPushStatus(offlinePushStatus, ...)} with the stale
 *       T1 object, which has incomplete partition statuses and returns non-terminal status</li>
 * </ol>
 * Result: the push stays stuck at END_OF_PUSH_RECEIVED forever, even though ZK shows COMPLETED.
 *
 * <h3>The Fix</h3>
 * After {@code updateOfflinePush()}, re-read the object from {@code topicToPushMap}:
 * <pre>
 *   offlinePushStatus = topicToPushMap.get(offlinePushStatus.getKafkaTopic());
 * </pre>
 */
public class LoadAllPushesStaleReferenceTest extends AbstractPushMonitorTest {
  @Override
  protected AbstractPushMonitor getPushMonitor(StoreCleaner storeCleaner) {
    return new PartitionStatusBasedPushMonitor(
        getClusterName(),
        getMockAccessor(),
        storeCleaner,
        getMockStoreRepo(),
        getMockRoutingDataRepo(),
        getMockPushHealthStats(),
        mock(RealTimeTopicSwitcher.class),
        getClusterLockManager(),
        getAggregateRealTimeSourceKafkaUrl(),
        Collections.emptyList(),
        mock(HelixAdminClient.class),
        getMockControllerConfig(),
        null,
        mock(DisabledPartitionStats.class),
        getMockVeniceWriterFactory(),
        getCurrentVersionChangeNotifier());
  }

  @Override
  protected AbstractPushMonitor getPushMonitor(RealTimeTopicSwitcher mockRealTimeTopicSwitcher) {
    return new PartitionStatusBasedPushMonitor(
        getClusterName(),
        getMockAccessor(),
        getMockStoreCleaner(),
        getMockStoreRepo(),
        getMockRoutingDataRepo(),
        getMockPushHealthStats(),
        mockRealTimeTopicSwitcher,
        getClusterLockManager(),
        getAggregateRealTimeSourceKafkaUrl(),
        Collections.emptyList(),
        mock(HelixAdminClient.class),
        getMockControllerConfig(),
        null,
        mock(DisabledPartitionStats.class),
        getMockVeniceWriterFactory(),
        getCurrentVersionChangeNotifier());
  }

  /**
   * Reproduces the stale reference bug where loadAllPushes() uses a stale loop variable
   * after updateOfflinePush() has replaced it in topicToPushMap with fresh ZK data.
   *
   * <p>Setup:
   * <ul>
   *   <li>T1 (bulk load): OfflinePushStatus has overall status END_OF_PUSH_RECEIVED and
   *       partition statuses where NOT all replicas are COMPLETED</li>
   *   <li>T2 (updateOfflinePush refresh): A <b>different</b> OfflinePushStatus object where
   *       all replicas are COMPLETED</li>
   * </ul>
   *
   * <p>Expected: After loadAllPushes(), the push transitions to COMPLETED.
   * <p>Actual (with bug): The push stays at END_OF_PUSH_RECEIVED because checkPushStatus()
   * is called with the stale T1 object whose partition statuses are incomplete.
   */
  @Test
  public void testLoadAllPushesStaleReferenceAfterUpdateOfflinePush() {
    String topic = getTopic();
    Store store = prepareMockStore(topic);

    // === T1: Stale snapshot from bulk load ===
    // Overall status is END_OF_PUSH_RECEIVED (non-terminal), partition replicas are NOT all COMPLETED.
    OfflinePushStatus stalePushStatus = new OfflinePushStatus(
        topic,
        getNumberOfPartition(),
        getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    stalePushStatus.setCurrentStatus(END_OF_PUSH_RECEIVED);

    // Set partition statuses for T1: only one replica has END_OF_PUSH_RECEIVED, others still STARTED.
    // This means checkPushStatus on this object will NOT return COMPLETED.
    for (int i = 0; i < getNumberOfPartition(); i++) {
      PartitionStatus partitionStatus = mock(ReadOnlyPartitionStatus.class);
      when(partitionStatus.getPartitionId()).thenReturn(i);
      // Only first replica has EOP_RECEIVED, others are STARTED -> push not complete per T1
      when(partitionStatus.getReplicaHistoricStatusList("instance0"))
          .thenReturn(Collections.singletonList(new StatusSnapshot(END_OF_PUSH_RECEIVED, "")));
      when(partitionStatus.getReplicaHistoricStatusList("instance1"))
          .thenReturn(Collections.singletonList(new StatusSnapshot(ExecutionStatus.STARTED, "")));
      when(partitionStatus.getReplicaHistoricStatusList("instance2"))
          .thenReturn(Collections.singletonList(new StatusSnapshot(ExecutionStatus.STARTED, "")));
      stalePushStatus.setPartitionStatus(partitionStatus);
    }

    List<OfflinePushStatus> statusList = new ArrayList<>();
    statusList.add(stalePushStatus);

    // Mock bulk load to return the stale T1 snapshot
    doReturn(statusList).when(getMockAccessor()).loadOfflinePushStatusesAndPartitionStatuses();

    // === T2: Fresh snapshot from ZK (returned by updateOfflinePush -> getOfflinePushStatusAndItsPartitionStatuses) ===
    // This is a DIFFERENT object where all replicas are COMPLETED.
    OfflinePushStatus freshPushStatus = new OfflinePushStatus(
        topic,
        getNumberOfPartition(),
        getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    // The fresh status still shows END_OF_PUSH_RECEIVED as overall status (it hasn't been terminal-ized yet),
    // but ALL partition replicas are COMPLETED, so checkPushStatus should derive COMPLETED.
    freshPushStatus.setCurrentStatus(END_OF_PUSH_RECEIVED);

    for (int i = 0; i < getNumberOfPartition(); i++) {
      PartitionStatus freshPartitionStatus = mock(ReadOnlyPartitionStatus.class);
      when(freshPartitionStatus.getPartitionId()).thenReturn(i);
      // All replicas are COMPLETED in T2
      when(freshPartitionStatus.getReplicaHistoricStatusList(anyString()))
          .thenReturn(Collections.singletonList(new StatusSnapshot(COMPLETED, "")));
      freshPushStatus.setPartitionStatus(freshPartitionStatus);
    }

    // updateOfflinePush() calls this to get the fresh T2 data
    when(getMockAccessor().getOfflinePushStatusAndItsPartitionStatuses(eq(topic))).thenReturn(freshPushStatus);

    // Setup routing data so checkPushStatus path is taken
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, getNumberOfPartition());
    doReturn(true).when(getMockRoutingDataRepo()).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(getMockRoutingDataRepo()).getPartitionAssignments(topic);
    for (int i = 0; i < getNumberOfPartition(); i++) {
      Partition partition = mock(Partition.class);
      Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
      instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
      when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
      when(partition.getId()).thenReturn(i);
      partitionAssignment.addPartition(partition);
    }

    // === Act ===
    getMonitor().loadAllPushes();

    // === Assert ===
    // With the bug: the push stays at END_OF_PUSH_RECEIVED because checkPushStatus used the stale T1 object
    // whose partition replicas are incomplete.
    // With the fix: the push transitions to COMPLETED because checkPushStatus uses the fresh T2 object
    // whose partition replicas are all COMPLETED.
    OfflinePushStatus resultStatus = getMonitor().getOfflinePushOrThrow(topic);
    Assert.assertEquals(
        resultStatus.getCurrentStatus(),
        COMPLETED,
        "BUG: loadAllPushes() used stale loop variable after updateOfflinePush() replaced it in topicToPushMap. "
            + "The push should have transitioned to COMPLETED using the fresh T2 data, but instead "
            + "checkPushStatus was called with stale T1 data (incomplete partition replicas) and returned non-terminal status. "
            + "Fix: after updateOfflinePush(), re-read the object from topicToPushMap.");

    // Also verify version swap happened (store current version should be bumped to 1)
    Assert.assertEquals(store.getCurrentVersion(), 1, "Version should have been swapped after push completed");
    verify(getMockStoreRepo(), atLeastOnce()).updateStore(store);

    Mockito.reset(getMockAccessor());
  }
}
