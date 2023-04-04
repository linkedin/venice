package com.linkedin.venice.listener;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.ChannelHandlerContext;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * This test ensures that the ReadQuotaEnforcementHandler will throttle over-quota requests
 */
public class ReadQuotaEnforcementHandlerTest {
  long nodeCapacity;
  String thisNodeId;
  Clock clock;
  long currentTime;
  ReadOnlyStoreRepository storeRepository;
  HelixCustomizedViewOfflinePushRepository customizedViewRepository;
  ReadQuotaEnforcementHandler quotaEnforcer;
  AggServerQuotaUsageStats stats;

  @BeforeMethod
  public void setUp() {
    nodeCapacity = 10; // rcu per second that a single node can support
    thisNodeId = "node1";
    clock = mock(Clock.class);
    currentTime = 0;
    doReturn(currentTime).when(clock).millis();
    storeRepository = mock(ReadOnlyStoreRepository.class);
    customizedViewRepository = mock(HelixCustomizedViewOfflinePushRepository.class);
    stats = mock(AggServerQuotaUsageStats.class);
    quotaEnforcer = new ReadQuotaEnforcementHandler(
        nodeCapacity,
        storeRepository,
        CompletableFuture.completedFuture(customizedViewRepository),
        thisNodeId,
        stats,
        clock);
  }

  /**
   * Test enforcement of the node-level capacity when there is no store-level quota
   */
  @Test
  public void testQuotaEnforcementHandlerAtNodeLevel() {

    runTest(
        "dummyStore_v1",
        nodeCapacity * 5 * 10, // default multiple is 5, times 10 second interval
        nodeCapacity * 10, // default refill interval is 10 seconds
        MILLISECONDS.convert(10, SECONDS)); // default 10 second interval
  }

  /**
   * Test the case when there is a store-level quota which is less than the node-level capacity
   */
  @Test
  public void testQuotaEnforcementAtStoreLevel() {
    String storeName = Utils.getUniqueString("store");
    String topic = Version.composeKafkaTopic(storeName, 1);

    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();

    Partition partition = mock(Partition.class);
    doReturn(0).when(partition).getId();

    List<ReplicaState> replicaStates = new ArrayList<>();
    ReplicaState thisReplicaState = mock(ReplicaState.class);
    doReturn(thisInstance.getNodeId()).when(thisReplicaState).getParticipantId();
    doReturn(ExecutionStatus.COMPLETED.name()).when(thisReplicaState).getVenicePushStatus();
    replicaStates.add(thisReplicaState);
    when(customizedViewRepository.getReplicaStates(topic, partition.getId())).thenReturn(replicaStates);

    PartitionAssignment pa = mock(PartitionAssignment.class);
    doReturn(topic).when(pa).getTopic();
    doReturn(Collections.singletonList(partition)).when(pa).getAllPartitions();

    long storeReadQuota = 5; // rcu per second
    Store store = mock(Store.class);
    doReturn(storeReadQuota).when(store).getReadQuotaInCU();
    doReturn(store).when(storeRepository).getStore(any());

    quotaEnforcer.onExternalViewChange(pa);

    runTest(topic, storeReadQuota * 5 * 10, storeReadQuota * 10, 10000);
  }

  /**
   * Tests the case when there are two nodes serving the partition, this node should only
   * support half of the allocated quota
   */
  @Test
  public void testQuotaEnforcementAtStoreLevelWithMultipleNodes() {
    String storeName = Utils.getUniqueString("store");
    String topic = Version.composeKafkaTopic(storeName, 1);

    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();
    Instance otherInstance = mock(Instance.class);
    doReturn("otherNodeId").when(otherInstance).getNodeId();

    Partition partition = mock(Partition.class);
    doReturn(0).when(partition).getId();

    List<ReplicaState> replicaStates = new ArrayList<>();
    ReplicaState thisReplicaState = mock(ReplicaState.class);
    doReturn(thisInstance.getNodeId()).when(thisReplicaState).getParticipantId();
    doReturn(ExecutionStatus.COMPLETED.name()).when(thisReplicaState).getVenicePushStatus();
    replicaStates.add(thisReplicaState);
    ReplicaState otherReplicaState = mock(ReplicaState.class);
    doReturn(otherInstance.getNodeId()).when(otherReplicaState).getParticipantId();
    doReturn(ExecutionStatus.COMPLETED.name()).when(otherReplicaState).getVenicePushStatus();
    replicaStates.add(otherReplicaState);
    when(customizedViewRepository.getReplicaStates(topic, partition.getId())).thenReturn(replicaStates);

    PartitionAssignment pa = mock(PartitionAssignment.class);
    doReturn(topic).when(pa).getTopic();
    doReturn(Collections.singletonList(partition)).when(pa).getAllPartitions();
    doReturn(pa).when(customizedViewRepository).getPartitionAssignments(topic);

    long storeReadQuota = 6; // rcu per second, only half will be supported on this node
    Store store = mock(Store.class);
    doReturn(storeReadQuota).when(store).getReadQuotaInCU();
    doReturn(store).when(storeRepository).getStore(any());

    quotaEnforcer.onCustomizedViewChange(pa);

    runTest(topic, storeReadQuota / 2 * 5 * 10, storeReadQuota / 2 * 10, 10000);
  }

  /**
   * After appropriate setup, this test ensures we can read the initial capacity of the TokenBucket, cannot read
   * beyond that, then increments time to allow for a bucket refill, and again ensures we can read the amount that was
   * refilled but not beyond that.
   *
   * @param resourceName Store-version kafka topic
   * @param capacity  Maximum number of reads that can be made at once, this is the max bucket capacity
   * @param refillAmount  How many tokens are refilled in the bucket during each interval
   * @param refillTimeMs  The length of the refill interval
   */
  void runTest(String resourceName, long capacity, long refillAmount, long refillTimeMs) {
    AtomicInteger allowed = new AtomicInteger(0);
    AtomicInteger blocked = new AtomicInteger(0);

    RouterRequest request = mock(RouterRequest.class);
    doReturn(resourceName).when(request).getResourceName();
    doReturn(RequestType.SINGLE_GET).when(request).getRequestType();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    doAnswer((a) -> {
      blocked.incrementAndGet();
      return null;
    }).when(ctx).writeAndFlush(any());
    doAnswer((a) -> {
      allowed.incrementAndGet();
      return null;
    }).when(ctx).fireChannelRead(any());
    for (int i = 0; i < capacity; i++) {
      quotaEnforcer.channelRead0(ctx, request);
    }
    assertEquals(allowed.get(), capacity, "Made " + capacity + " reads, and all should have been allowed");
    assertEquals(blocked.get(), 0, "Didn't exceed " + capacity + " reads, but " + blocked.get() + " were throttled");
    quotaEnforcer.channelRead0(ctx, request);
    assertEquals(blocked.get(), 1, "After reading capacity of " + capacity + " next read should have been blocked");

    allowed.set(0);
    blocked.set(0);

    // allow one refill
    currentTime = currentTime + refillTimeMs + 1; // refill checks if past refill time, hence +1
    doReturn(currentTime).when(clock).millis();

    for (int i = 0; i < refillAmount; i++) {
      quotaEnforcer.channelRead0(ctx, request);
    }
    assertEquals(
        allowed.get(),
        refillAmount,
        "Made " + refillAmount + " reads after refill, and all should have been allowed");
    assertEquals(blocked.get(), 0, "After refill, reads should not be throttled");
    quotaEnforcer.channelRead0(ctx, request);
    assertEquals(
        blocked.get(),
        1,
        "After exhausting refill of " + refillAmount + " next read should have been blocked");
  }
}
