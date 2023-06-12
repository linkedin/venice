package com.linkedin.venice.controller;

import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.helix.CachedReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ErrorPartitionResetTaskTest {
  private static long PROCESSING_CYCLE_DELAY = 100;
  private static int ERROR_PARTITION_RESET_LIMIT = 1;
  private static int PARTITION_COUNT = 3;
  private static long VERIFY_TIMEOUT = PROCESSING_CYCLE_DELAY * 3;

  private final ExecutorService errorPartitionResetExecutorService = Executors.newSingleThreadExecutor();
  private final Instance[] instances = { new Instance("a", "a", 1), new Instance("b", "b", 2),
      new Instance("c", "c", 3), new Instance("d", "d", 4), new Instance("e", "e", 5) };

  private HelixAdminClient helixAdminClient;
  private CachedReadOnlyStoreRepository readOnlyStoreRepository;
  private HelixExternalViewRepository routingDataRepository;
  private PushMonitor pushMonitor;
  private MetricsRepository metricsRepository;

  @BeforeMethod
  public void setUp() {
    helixAdminClient = mock(HelixAdminClient.class);
    readOnlyStoreRepository = mock(CachedReadOnlyStoreRepository.class);
    routingDataRepository = mock(HelixExternalViewRepository.class);
    pushMonitor = mock(PushMonitor.class);
    metricsRepository = new MetricsRepository();
  }

  @AfterClass
  public void cleanUp() throws InterruptedException {
    TestUtils.shutdownExecutor(errorPartitionResetExecutorService);
  }

  @Test
  public void testErrorPartitionReset() {
    String clusterName = Utils.getUniqueString("testCluster");
    // Setup a store where two of its partitions has exactly one error replica.
    Store store = getStoreWithCurrentVersion();
    String resourceName = store.getVersion(store.getCurrentVersion()).get().kafkaTopicName();
    EnumMap<HelixState, List<Instance>> errorStateInstanceMap = new EnumMap<>(HelixState.class);
    EnumMap<HelixState, List<Instance>> healthyStateInstanceMap = new EnumMap<>(HelixState.class);
    errorStateInstanceMap.put(HelixState.ERROR, Arrays.asList(instances[0]));
    // if a replica is error, then the left should be 1 leader and 1 standby.
    errorStateInstanceMap.put(HelixState.LEADER, Arrays.asList(instances[1]));
    errorStateInstanceMap.put(HelixState.STANDBY, Arrays.asList(instances[2]));
    healthyStateInstanceMap.put(HelixState.LEADER, Arrays.asList(instances[0]));
    healthyStateInstanceMap.put(HelixState.STANDBY, Arrays.asList(instances[1], instances[2]));

    Partition errorPartition0 = new Partition(0, errorStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    Partition errorPartition1 = new Partition(1, errorStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    Partition healthyPartition2 = new Partition(2, healthyStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    PartitionAssignment partitionAssignment1 = new PartitionAssignment(resourceName, PARTITION_COUNT);
    partitionAssignment1.addPartition(errorPartition0);
    partitionAssignment1.addPartition(errorPartition1);
    partitionAssignment1.addPartition(healthyPartition2);
    // Mock a post reset assignment where one of the partition remains in error state and the other one recovers
    Partition healthyPartition1 = new Partition(1, healthyStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    PartitionAssignment partitionAssignment2 = new PartitionAssignment(resourceName, PARTITION_COUNT);
    partitionAssignment2.addPartition(errorPartition0);
    partitionAssignment2.addPartition(healthyPartition1);
    partitionAssignment2.addPartition(healthyPartition2);
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(resourceName, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    doReturn(Arrays.asList(store)).when(readOnlyStoreRepository).getAllStores();
    when(routingDataRepository.getPartitionAssignments(resourceName)).thenReturn(partitionAssignment1)
        .thenReturn(partitionAssignment2);
    when(pushMonitor.getOfflinePushOrThrow(resourceName)).thenReturn(offlinePushStatus);
    ErrorPartitionResetTask errorPartitionResetTask = getErrorPartitionResetTask(clusterName);
    errorPartitionResetExecutorService.submit(errorPartitionResetTask);

    // Verify the reset is called for the error partitions
    verify(helixAdminClient, timeout(VERIFY_TIMEOUT).times(1)).resetPartition(
        clusterName,
        instances[0].getNodeId(),
        resourceName,
        Arrays.asList(
            HelixUtils.getPartitionName(resourceName, errorPartition0.getId()),
            HelixUtils.getPartitionName(resourceName, errorPartition1.getId())));
    // Make sure we have went through at least 3 cycles before proceeding with the verifications.
    verify(readOnlyStoreRepository, timeout(VERIFY_TIMEOUT).times(3)).getAllStores();
    verify(helixAdminClient, never())
        .resetPartition(eq(clusterName), eq(instances[1].getNodeId()), eq(resourceName), anyList());
    verify(helixAdminClient, never())
        .resetPartition(eq(clusterName), eq(instances[2].getNodeId()), eq(resourceName), anyList());
    Assert.assertEquals(
        metricsRepository
            .getMetric(String.format(".%s--current_version_error_partition_reset_attempt.Total", clusterName))
            .value(),
        2.);
    Assert
        .assertEquals(
            metricsRepository
                .getMetric(
                    String.format(".%s--current_version_error_partition_reset_attempt_errored.Count", clusterName))
                .value(),
            0.);
    Assert
        .assertEquals(
            metricsRepository
                .getMetric(
                    String.format(".%s--current_version_error_partition_recovered_from_reset.Total", clusterName))
                .value(),
            1.);
    Assert.assertEquals(
        metricsRepository
            .getMetric(
                String.format(".%s--current_version_error_partition_unrecoverable_from_reset.Total", clusterName))
            .value(),
        1.);

    errorPartitionResetTask.close();
  }

  @Test
  public void testErrorPartitionResetOnExcessErrorReplicas() {
    String clusterName = Utils.getUniqueString("testCluster");
    // Setup a store where one of its partitions has excess error replicas.
    Store store = getStoreWithCurrentVersion();
    String resourceName = store.getVersion(store.getCurrentVersion()).get().kafkaTopicName();

    EnumMap<HelixState, List<Instance>> helixStateToInstancesMap = new EnumMap<>(HelixState.class);
    helixStateToInstancesMap.put(HelixState.ERROR, Arrays.asList(instances[0], instances[1]));
    helixStateToInstancesMap.put(HelixState.LEADER, Arrays.asList(instances[2]));
    helixStateToInstancesMap.put(HelixState.STANDBY, Arrays.asList(instances[3], instances[4]));
    Partition excessErrorPartition0 = new Partition(0, helixStateToInstancesMap, new EnumMap<>(ExecutionStatus.class));

    EnumMap<HelixState, List<Instance>> healthyStateInstanceMap = new EnumMap<>(HelixState.class);
    healthyStateInstanceMap.put(HelixState.LEADER, Arrays.asList(instances[0]));
    healthyStateInstanceMap.put(HelixState.STANDBY, Arrays.asList(instances[1], instances[2]));
    Partition healthyPartition1 = new Partition(1, healthyStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    Partition healthyPartition2 = new Partition(2, healthyStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    PartitionAssignment partitionAssignment1 = new PartitionAssignment(resourceName, PARTITION_COUNT);
    partitionAssignment1.addPartition(excessErrorPartition0);
    partitionAssignment1.addPartition(healthyPartition1);
    partitionAssignment1.addPartition(healthyPartition2);
    PartitionAssignment partitionAssignment2 = new PartitionAssignment(resourceName, PARTITION_COUNT);
    partitionAssignment2.addPartition(new Partition(0, healthyStateInstanceMap, new EnumMap<>(ExecutionStatus.class)));
    partitionAssignment2.addPartition(new Partition(1, healthyStateInstanceMap, new EnumMap<>(ExecutionStatus.class)));
    partitionAssignment2.addPartition(new Partition(2, healthyStateInstanceMap, new EnumMap<>(ExecutionStatus.class)));
    doReturn(Arrays.asList(store)).when(readOnlyStoreRepository).getAllStores();
    when(routingDataRepository.getPartitionAssignments(resourceName)).thenReturn(partitionAssignment1)
        .thenReturn(partitionAssignment2);
    ErrorPartitionResetTask errorPartitionResetTask = getErrorPartitionResetTask(clusterName);
    errorPartitionResetExecutorService.submit(errorPartitionResetTask);

    // Verify the reset is called for the two error replicas.
    verify(helixAdminClient, timeout(VERIFY_TIMEOUT).times(1)).resetPartition(
        clusterName,
        instances[0].getNodeId(),
        resourceName,
        Arrays.asList(HelixUtils.getPartitionName(resourceName, excessErrorPartition0.getId())));
    verify(helixAdminClient, timeout(VERIFY_TIMEOUT).times(1)).resetPartition(
        clusterName,
        instances[1].getNodeId(),
        resourceName,
        Arrays.asList(HelixUtils.getPartitionName(resourceName, excessErrorPartition0.getId())));
    verify(helixAdminClient, never())
        .resetPartition(eq(clusterName), eq(instances[2].getNodeId()), eq(resourceName), anyList());
    verify(helixAdminClient, never())
        .resetPartition(eq(clusterName), eq(instances[3].getNodeId()), eq(resourceName), anyList());
    verify(helixAdminClient, never())
        .resetPartition(eq(clusterName), eq(instances[4].getNodeId()), eq(resourceName), anyList());
    errorPartitionResetTask.close();
  }

  private ErrorPartitionResetTask getErrorPartitionResetTask(String clusterName) {
    return new ErrorPartitionResetTask(
        clusterName,
        helixAdminClient,
        readOnlyStoreRepository,
        routingDataRepository,
        pushMonitor,
        metricsRepository,
        ERROR_PARTITION_RESET_LIMIT,
        PROCESSING_CYCLE_DELAY);
  }

  private Store getStoreWithCurrentVersion() {
    Store store = TestUtils.getRandomStore();
    store.addVersion(new VersionImpl(store.getName(), 1, "", PARTITION_COUNT));
    store.setCurrentVersion(1);
    return store;
  }
}
