package com.linkedin.venice.listener;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.grpc.GrpcErrorCodes;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.listener.grpc.GrpcHandlerContext;
import com.linkedin.venice.listener.grpc.GrpcHandlerPipeline;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
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

  @DataProvider(name = "Test-Quota-EnforcementHandler-Enable-Grpc")
  public Object[][] testQuotaEnforcementHandlerEnableGrpc() {
    return new Object[][] { { true }, { false } };
  }

  /**
   * Test enforcement of the node-level capacity when there is no store-level quota
   */
  @Test
  public void testQuotaEnforcementHandlerAtNodeLevel() {
    Store store = mock(Store.class);
    doReturn(true).when(store).isStorageNodeReadQuotaEnabled();
    doReturn(store).when(storeRepository).getStore(any());

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
    doReturn(true).when(thisReplicaState).isReadyToServe();
    replicaStates.add(thisReplicaState);
    when(customizedViewRepository.getReplicaStates(topic, partition.getId())).thenReturn(replicaStates);

    PartitionAssignment pa = mock(PartitionAssignment.class);
    doReturn(topic).when(pa).getTopic();
    doReturn(Collections.singletonList(partition)).when(pa).getAllPartitions();

    long storeReadQuota = 5; // rcu per second
    Store store = mock(Store.class);
    doReturn(true).when(store).isStorageNodeReadQuotaEnabled();
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
    doReturn(true).when(thisReplicaState).isReadyToServe();
    replicaStates.add(thisReplicaState);
    ReplicaState otherReplicaState = mock(ReplicaState.class);
    doReturn(otherInstance.getNodeId()).when(otherReplicaState).getParticipantId();
    doReturn(true).when(otherReplicaState).isReadyToServe();
    replicaStates.add(otherReplicaState);
    when(customizedViewRepository.getReplicaStates(topic, partition.getId())).thenReturn(replicaStates);

    PartitionAssignment pa = mock(PartitionAssignment.class);
    doReturn(topic).when(pa).getTopic();
    doReturn(Collections.singletonList(partition)).when(pa).getAllPartitions();
    doReturn(pa).when(customizedViewRepository).getPartitionAssignments(topic);

    long storeReadQuota = 6; // rcu per second, only half will be supported on this node
    Store store = mock(Store.class);
    doReturn(true).when(store).isStorageNodeReadQuotaEnabled();
    doReturn(storeReadQuota).when(store).getReadQuotaInCU();
    doReturn(store).when(storeRepository).getStore(any());

    quotaEnforcer.onCustomizedViewChange(pa);

    runTest(topic, storeReadQuota / 2 * 5 * 10, storeReadQuota / 2 * 10, 10000);
  }

  /**
   * Test requests with invalid resource names are rejected with BAD_REQUEST
   */
  @Test
  public void testInvalidResourceNames() {
    String invalidStoreName = "store_dne";
    RouterRequest request = mock(RouterRequest.class);
    doReturn(invalidStoreName).when(request).getStoreName();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    quotaEnforcer.channelRead0(ctx, request);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(responseCaptor.capture());
    Object response = responseCaptor.getValue();
    assertTrue(response instanceof HttpShortcutResponse);
    assertEquals(((HttpShortcutResponse) response).getStatus(), HttpResponseStatus.BAD_REQUEST);
  }

  @Test
  public void testInvalidResourceNameGrpcEnabled() {
    String invalidStoreName = "store_dne";
    GrpcHandlerContext ctx = mock(GrpcHandlerContext.class);
    GrpcHandlerPipeline pipeline = mock(GrpcHandlerPipeline.class);
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();

    RouterRequest request = mock(RouterRequest.class);
    doReturn(invalidStoreName).when(request).getStoreName();
    doReturn(request).when(ctx).getRouterRequest();
    doReturn(builder).when(ctx).getVeniceServerResponseBuilder();

    quotaEnforcer.grpcRead(ctx, pipeline);
    assertEquals(builder.getErrorCode(), GrpcErrorCodes.BAD_REQUEST);
    assertNotNull(builder.getErrorMessage());
  }

  @DataProvider(name = "Enable-Grpc-Test-Boolean")
  public Object[][] enableGrpcTestBoolean() {
    return new Object[][] { { false }, { true } };
  }

  /**
   * Test enforcement of storage node read quota is only enabled for specific stores
   */
  @Test(dataProvider = "Enable-Grpc-Test-Boolean")
  public void testStoreLevelStorageNodeReadQuotaEnabled(boolean grpcEnabled) {
    String quotaEnabledStoreName = Utils.getUniqueString("quotaEnabled");
    String quotaDisabledStoreName = Utils.getUniqueString("quotaDisabled");

    String quotaEnabledTopic = Version.composeKafkaTopic(quotaEnabledStoreName, 1);
    String quotaDisabledTopic = Version.composeKafkaTopic(quotaDisabledStoreName, 1);

    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();

    Partition partition = mock(Partition.class);
    doReturn(0).when(partition).getId();

    List<ReplicaState> replicaStates = new ArrayList<>();
    ReplicaState thisReplicaState = mock(ReplicaState.class);
    doReturn(thisInstance.getNodeId()).when(thisReplicaState).getParticipantId();
    doReturn(true).when(thisReplicaState).isReadyToServe();
    replicaStates.add(thisReplicaState);
    when(customizedViewRepository.getReplicaStates(quotaEnabledTopic, partition.getId())).thenReturn(replicaStates);

    PartitionAssignment pa = mock(PartitionAssignment.class);
    doReturn(quotaEnabledTopic).when(pa).getTopic();
    doReturn(Collections.singletonList(partition)).when(pa).getAllPartitions();

    long storeReadQuota = 1;
    Store store = mock(Store.class);
    Store quotaDisabledStore = mock(Store.class);
    doReturn(true).when(store).isStorageNodeReadQuotaEnabled();
    doReturn(storeReadQuota).when(store).getReadQuotaInCU();
    doReturn(store).when(storeRepository).getStore(eq(quotaEnabledStoreName));
    doReturn(storeReadQuota).when(quotaDisabledStore).getReadQuotaInCU();
    doReturn(quotaDisabledStore).when(storeRepository).getStore(eq(quotaDisabledStoreName));

    quotaEnforcer.onExternalViewChange(pa);

    AtomicInteger allowed = new AtomicInteger(0);
    AtomicInteger blocked = new AtomicInteger(0);

    RouterRequest request = mock(RouterRequest.class);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    GrpcHandlerContext grpcCtx = mock(GrpcHandlerContext.class);
    GrpcHandlerPipeline pipeline = mock(GrpcHandlerPipeline.class);
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    setUpRequestMocks(ctx, request, allowed, blocked, quotaEnabledTopic);
    setUpGrpcMocks(grpcCtx, builder, pipeline, allowed, blocked, request);
    long capacity = storeReadQuota * 5 * 10;
    for (int i = 0; i < capacity; i++) {
      if (!grpcEnabled) {
        quotaEnforcer.channelRead0(ctx, request);
      } else {
        quotaEnforcer.grpcRead(grpcCtx, pipeline);
      }
    }
    assertEquals(allowed.get(), capacity);
    assertEquals(blocked.get(), 0);
    if (!grpcEnabled) {
      quotaEnforcer.channelRead0(ctx, request);
    } else {
      quotaEnforcer.grpcRead(grpcCtx, pipeline);
    }
    assertEquals(blocked.get(), 1);

    allowed.set(0);
    blocked.set(0);
    RouterRequest quotaDisabledRequest = mock(RouterRequest.class);
    ChannelHandlerContext quotaDisabledCtx = mock(ChannelHandlerContext.class);
    GrpcHandlerContext grpcDisabledCtx = mock(GrpcHandlerContext.class);
    VeniceServerResponse.Builder quotaDisabledBuilder = VeniceServerResponse.newBuilder();
    setUpRequestMocks(quotaDisabledCtx, quotaDisabledRequest, allowed, blocked, quotaDisabledTopic);
    setUpGrpcMocks(grpcDisabledCtx, quotaDisabledBuilder, pipeline, allowed, blocked, quotaDisabledRequest);

    for (int i = 0; i < capacity * 2; i++) {
      if (!grpcEnabled) {
        quotaEnforcer.channelRead0(quotaDisabledCtx, quotaDisabledRequest);
      } else {
        quotaEnforcer.grpcRead(grpcDisabledCtx, pipeline);
      }
    }
    // Store that have storage node read quota disabled should not be blocked
    assertEquals(allowed.get(), capacity * 2);
    assertEquals(blocked.get(), 0);
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
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    setUpRequestMocks(ctx, request, allowed, blocked, resourceName);
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

  void setUpRequestMocks(
      ChannelHandlerContext ctx,
      RouterRequest request,
      AtomicInteger allowed,
      AtomicInteger blocked,
      String resourceName) {
    doReturn(Version.parseStoreFromKafkaTopicName(resourceName)).when(request).getStoreName();
    doReturn(resourceName).when(request).getResourceName();
    doReturn(RequestType.SINGLE_GET).when(request).getRequestType();
    doAnswer((a) -> {
      blocked.incrementAndGet();
      return null;
    }).when(ctx).writeAndFlush(any());
    doAnswer((a) -> {
      allowed.incrementAndGet();
      return null;
    }).when(ctx).fireChannelRead(any());
  }

  void setUpGrpcMocks(
      GrpcHandlerContext grpcCtx,
      VeniceServerResponse.Builder builder,
      GrpcHandlerPipeline pipeline,
      AtomicInteger allowed,
      AtomicInteger blocked,
      RouterRequest request) {
    doReturn(request).when(grpcCtx).getRouterRequest();
    doReturn(builder).when(grpcCtx).getVeniceServerResponseBuilder();
    doAnswer((a) -> {
      allowed.incrementAndGet();
      return null;
    }).when(pipeline).processRequest(any());
    doAnswer((a) -> {
      blocked.incrementAndGet();
      return null;
    }).when(grpcCtx).setError();
  }
}
