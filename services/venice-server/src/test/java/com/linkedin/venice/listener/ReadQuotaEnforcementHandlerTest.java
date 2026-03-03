package com.linkedin.venice.listener;

import static com.linkedin.venice.response.VeniceReadResponseStatus.BAD_REQUEST;
import static com.linkedin.venice.throttle.VeniceRateLimiter.RateLimiterType;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.listener.ReadQuotaEnforcementHandler.QuotaEnforcementResult;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.grpc.handlers.GrpcReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.stats.ServerReadQuotaUsageStats;
import com.linkedin.venice.throttle.GuavaRateLimiter;
import com.linkedin.venice.throttle.TokenBucket;
import com.linkedin.venice.throttle.VeniceRateLimiter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * This test ensures that the ReadQuotaEnforcementHandler will throttle over-quota requests
 */
public class ReadQuotaEnforcementHandlerTest {
  private long nodeCapacity;
  private String thisNodeId;
  private Clock clock;
  private long currentTime;
  private ReadOnlyStoreRepository storeRepository;
  private HelixCustomizedViewOfflinePushRepository customizedViewRepository;
  private ReadQuotaEnforcementHandler quotaEnforcementHandler;
  private GrpcReadQuotaEnforcementHandler grpcQuotaEnforcementHandler;
  private AggServerQuotaUsageStats mockAggStats;

  private MetricsRepository metricsRepository;
  private RouterRequest routerRequest;
  private VeniceServerGrpcHandler mockNextHandler;
  private VeniceServerConfig serverConfig;
  private StorageEngineRepository storageEngineRepository;

  @BeforeMethod
  public void setUp() {
    nodeCapacity = 10; // rcu per second that a single node can support
    thisNodeId = "node1";
    clock = mock(Clock.class);
    currentTime = 0;
    doReturn(currentTime).when(clock).millis();
    storeRepository = mock(ReadOnlyStoreRepository.class);
    customizedViewRepository = mock(HelixCustomizedViewOfflinePushRepository.class);
    mockAggStats = mock(AggServerQuotaUsageStats.class);
    metricsRepository = new MetricsRepository();
    serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.getQuotaEnforcementIntervalInMs()).thenReturn(10000);
    when(serverConfig.getQuotaEnforcementCapacityMultiple()).thenReturn(5);
    doReturn(nodeCapacity).when(serverConfig).getNodeCapacityInRcu();
    routerRequest = mock(RouterRequest.class);
    // Reset the handlers to ensure we don't accidentally use handlers initialized in previous test
    quotaEnforcementHandler = null;
    grpcQuotaEnforcementHandler = null;
    storageEngineRepository = mock(StorageEngineRepository.class);
  }

  private void initializeQuotaEnforcementHandlers() {
    initializeQuotaEnforcementHandlers(mockAggStats);
  }

  private void initializeQuotaEnforcementHandlers(AggServerQuotaUsageStats aggStats) {
    quotaEnforcementHandler = new ReadQuotaEnforcementHandler(
        serverConfig,
        storeRepository,
        CompletableFuture.completedFuture(customizedViewRepository),
        storageEngineRepository,
        thisNodeId,
        aggStats,
        clock);
    grpcQuotaEnforcementHandler = new GrpcReadQuotaEnforcementHandler(quotaEnforcementHandler);
    mockNextHandler = mock(VeniceServerGrpcHandler.class);
    grpcQuotaEnforcementHandler.addNextHandler(mockNextHandler);
    doNothing().when(mockNextHandler).processRequest(any());
    TestUtils.waitForNonDeterministicAssertion(10, SECONDS, () -> assertTrue(quotaEnforcementHandler.isInitialized()));
  }

  @DataProvider(name = "Test-Quota-EnforcementHandler-Enable-Grpc")
  public Object[][] testQuotaEnforcementHandlerEnableGrpc() {
    return new Object[][] { { true }, { false } };
  }

  @Test
  public void testEnforceQuotaWhenResourceIsNotFound() {
    String storeName = "test_store";
    when(routerRequest.getStoreName()).thenReturn(storeName);
    when(storeRepository.getStore(storeName)).thenReturn(null);
    initializeQuotaEnforcementHandlers();
    assertEquals(quotaEnforcementHandler.enforceQuota(routerRequest), QuotaEnforcementResult.BAD_REQUEST);

    verify(mockAggStats, never()).recordAllowed(eq(storeName), anyInt(), eq(1L));
    verify(mockAggStats, never()).recordRejected(eq(storeName), anyInt(), eq(1L));

    // for Netty handler
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    quotaEnforcementHandler.channelRead0(ctx, routerRequest);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(responseCaptor.capture());
    Object response = responseCaptor.getValue();
    assertTrue(response instanceof HttpShortcutResponse);
    HttpShortcutResponse shortcutResponse = (HttpShortcutResponse) response;
    assertEquals(shortcutResponse.getStatus(), HttpResponseStatus.BAD_REQUEST);
    assertNotNull(shortcutResponse.getMessage());

    // for gRPC handler
    GrpcRequestContext grpcCtx = mock(GrpcRequestContext.class);
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    when(grpcCtx.getRouterRequest()).thenReturn(routerRequest);
    when(grpcCtx.getVeniceServerResponseBuilder()).thenReturn(builder);
    grpcQuotaEnforcementHandler.processRequest(grpcCtx);
    assertEquals(builder.getErrorCode(), BAD_REQUEST.getCode());
    assertNotNull(builder.getErrorMessage());
  }

  @Test
  public void testEnforceQuotaDuringInitialization() {
    initializeQuotaEnforcementHandlers();
    quotaEnforcementHandler.setInitializedVolatile(false);
    assertFalse(quotaEnforcementHandler.isInitialized());
    String storeName = "test_store";
    Store store = mock(Store.class);
    when(routerRequest.getStoreName()).thenReturn(storeName);
    when(storeRepository.getStore(storeName)).thenReturn(store);
    assertEquals(quotaEnforcementHandler.enforceQuota(routerRequest), QuotaEnforcementResult.ALLOWED);

    verify(mockAggStats, never()).recordAllowed(eq(storeName), anyInt(), eq(1L));
    verify(mockAggStats, never()).recordRejected(eq(storeName), anyInt(), eq(1L));

    // for Netty handler
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    quotaEnforcementHandler.channelRead0(ctx, routerRequest);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).fireChannelRead(responseCaptor.capture());
    Object receivedRequest = responseCaptor.getValue();
    assertTrue(receivedRequest instanceof RouterRequest);
    assertEquals(receivedRequest, routerRequest);

    // for gRPC handler
    GrpcRequestContext grpcCtx = mock(GrpcRequestContext.class);
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    when(grpcCtx.getRouterRequest()).thenReturn(routerRequest);
    when(grpcCtx.getVeniceServerResponseBuilder()).thenReturn(builder);
    grpcQuotaEnforcementHandler.processRequest(grpcCtx);
    assertEquals(builder.getErrorCode(), 0);
    verify(mockNextHandler).processRequest(grpcCtx);
  }

  @Test
  public void testEnforceQuotaWhenStorageNodeReadQuotaIsDisabled() {
    String storeName = "test_store";
    Store store = mock(Store.class);
    when(store.isStorageNodeReadQuotaEnabled()).thenReturn(false);
    when(routerRequest.getStoreName()).thenReturn(storeName);
    when(storeRepository.getStore(storeName)).thenReturn(store);
    when(store.isStorageNodeReadQuotaEnabled()).thenReturn(false);
    initializeQuotaEnforcementHandlers();
    assertEquals(quotaEnforcementHandler.enforceQuota(routerRequest), QuotaEnforcementResult.ALLOWED);

    verify(mockAggStats, never()).recordAllowed(eq(storeName), anyInt(), eq(1L));
    verify(mockAggStats, never()).recordRejected(eq(storeName), anyInt(), eq(1L));

    // for Netty handler
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    quotaEnforcementHandler.channelRead0(ctx, routerRequest);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).fireChannelRead(responseCaptor.capture());
    Object receivedRequest = responseCaptor.getValue();
    assertTrue(receivedRequest instanceof RouterRequest);
    assertEquals(receivedRequest, routerRequest);

    // for gRPC handler
    GrpcRequestContext grpcCtx = mock(GrpcRequestContext.class);
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    when(grpcCtx.getRouterRequest()).thenReturn(routerRequest);
    when(grpcCtx.getVeniceServerResponseBuilder()).thenReturn(builder);
    grpcQuotaEnforcementHandler.processRequest(grpcCtx);
    assertEquals(builder.getErrorCode(), 0);
    verify(mockNextHandler).processRequest(grpcCtx);
  }

  @Test
  public void testQuotaEnforcementWhenStoreVersionQuotaIsExceeded() {
    // Case 1: If request is not a retry request, it should be rejected
    String storeName = "test_store";
    int version = 1;
    String resourceName = Version.composeKafkaTopic(storeName, version);
    Store store = mock(Store.class);
    when(routerRequest.getStoreName()).thenReturn(storeName);
    when(routerRequest.getResourceName()).thenReturn(resourceName);
    when(routerRequest.getRequestType()).thenReturn(RequestType.SINGLE_GET);
    when(routerRequest.isRetryRequest()).thenReturn(false);
    when(storeRepository.getStore(storeName)).thenReturn(store);
    when(store.isStorageNodeReadQuotaEnabled()).thenReturn(true);
    VeniceRateLimiter veniceRateLimiter = mock(VeniceRateLimiter.class);
    when(veniceRateLimiter.tryAcquirePermit(1)).thenReturn(false);
    initializeQuotaEnforcementHandlers();
    quotaEnforcementHandler.setStoreVersionRateLimiter(resourceName, veniceRateLimiter);
    assertEquals(quotaEnforcementHandler.enforceQuota(routerRequest), QuotaEnforcementResult.REJECTED);

    verify(mockAggStats).recordRejected(eq(storeName), eq(version), eq(1L));
    verify(mockAggStats, never()).recordAllowed(eq(storeName), anyInt(), eq(1L));

    // for Netty handler
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    quotaEnforcementHandler.channelRead0(ctx, routerRequest);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(responseCaptor.capture());
    Object response = responseCaptor.getValue();
    assertTrue(response instanceof HttpShortcutResponse);
    assertEquals(((HttpShortcutResponse) response).getStatus(), HttpResponseStatus.TOO_MANY_REQUESTS);

    // for gRPC handler
    GrpcRequestContext grpcCtx = mock(GrpcRequestContext.class);
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    when(grpcCtx.getRouterRequest()).thenReturn(routerRequest);
    when(grpcCtx.getVeniceServerResponseBuilder()).thenReturn(builder);
    grpcQuotaEnforcementHandler.processRequest(grpcCtx);
    assertEquals(builder.getErrorCode(), VeniceReadResponseStatus.TOO_MANY_REQUESTS.getCode());
    assertNotNull(builder.getErrorMessage());

    // Case 2: If request is a retry request, it should be allowed
    when(routerRequest.isRetryRequest()).thenReturn(true);
    assertEquals(quotaEnforcementHandler.enforceQuota(routerRequest), QuotaEnforcementResult.ALLOWED);
    verify(mockAggStats).recordAllowed(eq(storeName), eq(version), eq(1L));
  }

  @Test
  public void testQuotaEnforcementWhenServerIsOverCapacity() {
    String storeName = "test_store";
    int version = 1;
    String resourceName = Version.composeKafkaTopic(storeName, version);
    Store store = mock(Store.class);
    when(routerRequest.getStoreName()).thenReturn(storeName);
    when(routerRequest.getResourceName()).thenReturn(resourceName);
    when(routerRequest.getRequestType()).thenReturn(RequestType.SINGLE_GET);
    when(routerRequest.isRetryRequest()).thenReturn(false);
    when(storeRepository.getStore(storeName)).thenReturn(store);
    when(store.isStorageNodeReadQuotaEnabled()).thenReturn(true);
    initializeQuotaEnforcementHandlers();
    assertNull(quotaEnforcementHandler.getStoreVersionRateLimiter(resourceName));
    VeniceRateLimiter storageNodeRateLimiter = mock(VeniceRateLimiter.class);
    quotaEnforcementHandler.setStorageNodeRateLimiter(storageNodeRateLimiter);
    when(storageNodeRateLimiter.tryAcquirePermit(1)).thenReturn(false);
    assertEquals(quotaEnforcementHandler.enforceQuota(routerRequest), QuotaEnforcementResult.OVER_CAPACITY);

    verify(mockAggStats, never()).recordAllowed(eq(storeName), anyInt(), eq(1L));
    verify(mockAggStats, never()).recordRejected(eq(storeName), anyInt(), eq(1L));

    // for Netty handler
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    quotaEnforcementHandler.channelRead0(ctx, routerRequest);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(responseCaptor.capture());
    Object response = responseCaptor.getValue();
    assertTrue(response instanceof HttpShortcutResponse);
    assertSame(((HttpShortcutResponse) response).getStatus(), HttpResponseStatus.SERVICE_UNAVAILABLE);

    // for gRPC handler
    GrpcRequestContext grpcCtx = mock(GrpcRequestContext.class);
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    when(grpcCtx.getRouterRequest()).thenReturn(routerRequest);
    when(grpcCtx.getVeniceServerResponseBuilder()).thenReturn(builder);
    grpcQuotaEnforcementHandler.processRequest(grpcCtx);
    assertEquals(builder.getErrorCode(), VeniceReadResponseStatus.SERVICE_UNAVAILABLE.getCode());
    assertNotNull(builder.getErrorMessage());
  }

  // both storeVersion quota and server capacity are sufficient to allow the request
  @Test
  public void testQuotaEnforcement() {
    String storeName = "test_store";
    int version = 1;
    String resourceName = Version.composeKafkaTopic(storeName, version);
    Store store = mock(Store.class);
    when(routerRequest.getStoreName()).thenReturn(storeName);
    when(routerRequest.getResourceName()).thenReturn(resourceName);
    when(routerRequest.getRequestType()).thenReturn(RequestType.SINGLE_GET);
    when(routerRequest.isRetryRequest()).thenReturn(false);
    when(storeRepository.getStore(storeName)).thenReturn(store);
    when(store.isStorageNodeReadQuotaEnabled()).thenReturn(true);
    initializeQuotaEnforcementHandlers();
    VeniceRateLimiter veniceRateLimiter = mock(VeniceRateLimiter.class);
    when(veniceRateLimiter.tryAcquirePermit(1)).thenReturn(true);
    quotaEnforcementHandler.setStoreVersionRateLimiter(resourceName, veniceRateLimiter);
    VeniceRateLimiter storageNodeRateLimiter = mock(VeniceRateLimiter.class);
    when(storageNodeRateLimiter.tryAcquirePermit(1)).thenReturn(true);
    quotaEnforcementHandler.setStorageNodeRateLimiter(storageNodeRateLimiter);
    assertEquals(quotaEnforcementHandler.enforceQuota(routerRequest), QuotaEnforcementResult.ALLOWED);

    verify(mockAggStats).recordAllowed(eq(storeName), eq(version), eq(1L));
    verify(mockAggStats, never()).recordRejected(eq(storeName), anyInt(), eq(1L));

    // for Netty handler
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    quotaEnforcementHandler.channelRead0(ctx, routerRequest);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).fireChannelRead(responseCaptor.capture());
    Object receivedRequest = responseCaptor.getValue();
    assertTrue(receivedRequest instanceof RouterRequest);

    // for gRPC handler
    GrpcRequestContext grpcCtx = mock(GrpcRequestContext.class);
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    when(grpcCtx.getRouterRequest()).thenReturn(routerRequest);
    when(grpcCtx.getVeniceServerResponseBuilder()).thenReturn(builder);
    grpcQuotaEnforcementHandler.processRequest(grpcCtx);
    assertEquals(builder.getErrorCode(), 0);
    verify(mockNextHandler).processRequest(grpcCtx);
  }

  @Test
  public void testGetRCU() {
    for (RequestType requestType: RequestType.values()) {
      RouterRequest request = mock(RouterRequest.class);
      int keyCount = 10;
      doReturn(keyCount).when(request).getKeyCount();
      doReturn(requestType).when(request).getRequestType();

      int rcu = ReadQuotaEnforcementHandler.getRcu(request);
      if (requestType == RequestType.SINGLE_GET) {
        assertEquals(rcu, 1, "Single get rcu count should be 1");
      } else {
        assertEquals(rcu, keyCount, "Non-single get rcu count should be: " + keyCount);
      }
    }
  }

  @Test
  public void testInitWithPreExistingResource() {
    String storeName = "testStore";
    String topic = Version.composeKafkaTopic(storeName, 1);
    Version version = mock(Version.class);
    doReturn(topic).when(version).kafkaTopicName();
    Store store = setUpStoreMock(storeName, 1, Collections.singletonList(version), 100, true);
    doReturn(store).when(storeRepository).getStore(storeName);
    doReturn(Collections.singletonList(store)).when(storeRepository).getAllStores();
    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();
    Partition partition = setUpPartitionMock(topic, thisInstance, true, 0);
    doReturn(0).when(partition).getId();
    PartitionAssignment pa = setUpPartitionAssignmentMock(topic, Collections.singletonList(partition));
    doReturn(pa).when(customizedViewRepository).getPartitionAssignments(topic);
    // Do initialize the quota enforcement handler with a real stats for this test
    initializeQuotaEnforcementHandlers(new AggServerQuotaUsageStats(serverConfig.getClusterName(), metricsRepository));

    verify(storeRepository, atLeastOnce()).registerStoreDataChangedListener(quotaEnforcementHandler);
    verify(customizedViewRepository, atLeastOnce()).subscribeRoutingDataChange(topic, quotaEnforcementHandler);
    Assert.assertEquals(quotaEnforcementHandler.getStats().getStoreStats(storeName).getCurrentVersion(), 1);
    Assert.assertEquals(quotaEnforcementHandler.getStats().getStoreStats(storeName).getBackupVersion(), 0);
  }

  /**
   * Test enforcement of the node-level capacity when store-level quota is still allowing
   */
  @Test
  public void testQuotaEnforcementHandlerAtNodeLevel() {
    String storeName = "dummyStore";
    String topic = Version.composeKafkaTopic(storeName, 1);
    Version version = mock(Version.class);
    doReturn(topic).when(version).kafkaTopicName();
    Store store = setUpStoreMock(storeName, 1, Collections.singletonList(version), 1000000, true);
    doReturn(store).when(storeRepository).getStore(storeName);
    doReturn(Collections.singletonList(store)).when(storeRepository).getAllStores();

    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();
    Partition partition = setUpPartitionMock(topic, thisInstance, true, 0);
    doReturn(0).when(partition).getId();
    PartitionAssignment pa = setUpPartitionAssignmentMock(topic, Collections.singletonList(partition));
    doReturn(pa).when(customizedViewRepository).getPartitionAssignments(topic);
    initializeQuotaEnforcementHandlers();

    runTest(
        topic,
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

    Partition partition = setUpPartitionMock(topic, thisInstance, true, 0);
    doReturn(0).when(partition).getId();

    PartitionAssignment pa = setUpPartitionAssignmentMock(topic, Collections.singletonList(partition));

    long storeReadQuota = 5; // rcu per second
    Store store = mock(Store.class);
    doReturn(true).when(store).isStorageNodeReadQuotaEnabled();
    doReturn(storeReadQuota).when(store).getReadQuotaInCU();
    doReturn(store).when(storeRepository).getStore(any());

    initializeQuotaEnforcementHandlers();
    quotaEnforcementHandler.onCustomizedViewChange(pa);

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

    List<Instance> readyToServeReplicas = new ArrayList<>();
    readyToServeReplicas.add(thisInstance);
    readyToServeReplicas.add(otherInstance);
    doReturn(readyToServeReplicas).when(partition).getReadyToServeInstances();

    PartitionAssignment pa = mock(PartitionAssignment.class);
    doReturn(topic).when(pa).getTopic();
    doReturn(Collections.singletonList(partition)).when(pa).getAllPartitions();
    doReturn(pa).when(customizedViewRepository).getPartitionAssignments(topic);

    long storeReadQuota = 6; // rcu per second, only half will be supported on this node
    Store store = mock(Store.class);
    doReturn(true).when(store).isStorageNodeReadQuotaEnabled();
    doReturn(storeReadQuota).when(store).getReadQuotaInCU();
    doReturn(store).when(storeRepository).getStore(any());

    initializeQuotaEnforcementHandlers();
    quotaEnforcementHandler.onCustomizedViewChange(pa);

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
    initializeQuotaEnforcementHandlers();
    quotaEnforcementHandler.channelRead0(ctx, request);
    ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
    verify(ctx).writeAndFlush(responseCaptor.capture());
    Object response = responseCaptor.getValue();
    assertTrue(response instanceof HttpShortcutResponse);
    assertEquals(((HttpShortcutResponse) response).getStatus(), HttpResponseStatus.BAD_REQUEST);
  }

  @Test
  public void testReadQuotaOnVersionChange() {
    String storeName = Utils.getUniqueString("test-store");
    ServerReadQuotaUsageStats storeStats = mock(ServerReadQuotaUsageStats.class);
    doReturn(storeStats).when(mockAggStats).getStoreStats(storeName);
    doReturn(storeStats).when(mockAggStats).getNullableStoreStats(storeName);
    int currentVersion = 1;
    String topic = Version.composeKafkaTopic(storeName, currentVersion);
    String nextTopic = Version.composeKafkaTopic(storeName, currentVersion + 1);
    Version version = mock(Version.class);
    doReturn(topic).when(version).kafkaTopicName();
    Version nextVersion = mock(Version.class);
    doReturn(nextTopic).when(nextVersion).kafkaTopicName();
    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();

    Partition partition = setUpPartitionMock(topic, thisInstance, true, 0);
    PartitionAssignment pa = setUpPartitionAssignmentMock(topic, Collections.singletonList(partition));
    doReturn(pa).when(customizedViewRepository).getPartitionAssignments(topic);
    Partition nextPartition = setUpPartitionMock(nextTopic, thisInstance, true, 0);
    PartitionAssignment nextPa = setUpPartitionAssignmentMock(nextTopic, Collections.singletonList(nextPartition));
    doReturn(nextPa).when(customizedViewRepository).getPartitionAssignments(nextTopic);

    long storeReadQuota = 1;
    long newStoreReadQuota = storeReadQuota * 2;
    Store store = setUpStoreMock(storeName, currentVersion, Arrays.asList(version, nextVersion), storeReadQuota, true);
    Store storeAfterVersionBump =
        setUpStoreMock(storeName, currentVersion + 1, Arrays.asList(version, nextVersion), storeReadQuota, true);
    Store storeAfterQuotaBumpAndBackupDeletion =
        setUpStoreMock(storeName, currentVersion + 1, Collections.singletonList(nextVersion), newStoreReadQuota, true);
    // The store repository is called to initialize/update the token buckets, we need to make sure it doesn't return the
    // store state with higher quota prior to the test to verify quota change.
    // Get store is also called by getBucketForStore to update stats every time when handleStoreChanged is called.
    when(storeRepository.getStore(eq(storeName))).thenReturn(
        store,
        store,
        store,
        storeAfterVersionBump,
        storeAfterVersionBump,
        storeAfterQuotaBumpAndBackupDeletion);

    initializeQuotaEnforcementHandlers();
    quotaEnforcementHandler.handleStoreChanged(store);
    Assert.assertTrue(quotaEnforcementHandler.getActiveStoreVersions().contains(topic));
    Assert.assertTrue(quotaEnforcementHandler.getActiveStoreVersions().contains(nextTopic));

    quotaEnforcementHandler.handleStoreChanged(storeAfterVersionBump);
    int newCurrentVersion = currentVersion + 1;
    Assert.assertTrue(quotaEnforcementHandler.getActiveStoreVersions().contains(topic));
    Assert.assertTrue(quotaEnforcementHandler.getActiveStoreVersions().contains(nextTopic));
    verify(mockAggStats, atLeastOnce()).setCurrentVersion(eq(storeName), eq(newCurrentVersion));

    AtomicInteger allowed = new AtomicInteger(0);
    AtomicInteger blocked = new AtomicInteger(0);
    RouterRequest request = mock(RouterRequest.class);
    RouterRequest oldVersionRequest = mock(RouterRequest.class);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    setUpRequestMocks(ctx, request, allowed, blocked, nextTopic);
    setUpRequestMocks(ctx, oldVersionRequest, allowed, blocked, topic);
    long capacity = storeReadQuota * 5 * 10;
    for (int i = 0; i < capacity; i++) {
      quotaEnforcementHandler.channelRead0(ctx, request);
    }
    assertEquals(allowed.get(), capacity);
    assertEquals(blocked.get(), 0);
    verify(mockAggStats, times((int) capacity)).recordAllowed(eq(storeName), eq(newCurrentVersion), anyLong());
    verify(mockAggStats, never()).recordAllowedUnintentionally(eq(storeName), anyLong());
    verify(mockAggStats, never()).recordRejected(eq(storeName), anyInt(), anyLong());

    quotaEnforcementHandler.channelRead0(ctx, request);
    assertEquals(allowed.get(), capacity);
    assertEquals(blocked.get(), 1);
    verify(mockAggStats, times((int) capacity)).recordAllowed(eq(storeName), eq(newCurrentVersion), anyLong());
    verify(mockAggStats, never()).recordAllowedUnintentionally(eq(storeName), anyLong());
    verify(mockAggStats, times(1)).recordRejected(eq(storeName), eq(newCurrentVersion), anyLong());

    quotaEnforcementHandler.channelRead0(ctx, oldVersionRequest);
    assertEquals(allowed.get(), capacity + 1);
    assertEquals(blocked.get(), 1);
    verify(mockAggStats, atMostOnce()).recordAllowed(eq(storeName), eq(currentVersion), anyLong());
    verify(mockAggStats, never()).recordAllowedUnintentionally(eq(storeName), anyLong());
    verify(mockAggStats, never()).recordRejected(eq(storeName), eq(currentVersion), anyLong());

    quotaEnforcementHandler.handleStoreChanged(storeAfterQuotaBumpAndBackupDeletion);
    Assert.assertFalse(quotaEnforcementHandler.getActiveStoreVersions().contains(topic));
    Assert.assertTrue(quotaEnforcementHandler.getActiveStoreVersions().contains(nextTopic));
    verify(storeStats, atLeastOnce()).removeVersion(eq(currentVersion));
    long newCapacity = newStoreReadQuota * 5 * 10;
    for (int i = 0; i < newCapacity; i++) {
      quotaEnforcementHandler.channelRead0(ctx, request);
    }
    int expectedAllowedCount = (int) capacity + 1 + (int) newCapacity;
    assertEquals(allowed.get(), expectedAllowedCount);
    assertEquals(blocked.get(), 1);
    // one of the allowed request was towards the old current (backup) version
    verify(mockAggStats, times(expectedAllowedCount - 1))
        .recordAllowed(eq(storeName), eq(newCurrentVersion), anyLong());
    verify(mockAggStats, never()).recordAllowedUnintentionally(eq(storeName), anyLong());
    verify(mockAggStats, times(1)).recordRejected(eq(storeName), eq(newCurrentVersion), anyLong());
    quotaEnforcementHandler.onRoutingDataDeleted(nextTopic);
    verify(storeStats, atLeastOnce()).removeVersion(eq(currentVersion + 1));
  }

  @DataProvider(name = "Enable-Grpc-Test-Boolean")
  public Object[][] enableGrpcTestBoolean() {
    return new Object[][] { { false }, { true } };
  }

  /**
   * Test enforcement of storage node read quota is only enabled for specific stores
   */
  // @Test(dataProvider = "Enable-Grpc-Test-Boolean")
  @Test
  public void testStoreLevelStorageNodeReadQuotaEnabled() {
    String quotaEnabledStoreName = Utils.getUniqueString("quotaEnabled");
    String quotaDisabledStoreName = Utils.getUniqueString("quotaDisabled");

    String quotaEnabledTopic = Version.composeKafkaTopic(quotaEnabledStoreName, 1);
    String quotaDisabledTopic = Version.composeKafkaTopic(quotaDisabledStoreName, 1);

    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();

    Partition partition = setUpPartitionMock(quotaEnabledTopic, thisInstance, true, 0);
    PartitionAssignment pa = setUpPartitionAssignmentMock(quotaEnabledTopic, Collections.singletonList(partition));

    long storeReadQuota = 1;
    Store store = setUpStoreMock(quotaEnabledStoreName, 1, Collections.emptyList(), storeReadQuota, true);
    Store quotaDisabledStore =
        setUpStoreMock(quotaDisabledStoreName, 1, Collections.emptyList(), storeReadQuota, false);
    doReturn(store).when(storeRepository).getStore(eq(quotaEnabledStoreName));
    doReturn(quotaDisabledStore).when(storeRepository).getStore(eq(quotaDisabledStoreName));
    initializeQuotaEnforcementHandlers();

    quotaEnforcementHandler.onCustomizedViewChange(pa);

    AtomicInteger allowed = new AtomicInteger(0);
    AtomicInteger blocked = new AtomicInteger(0);

    RouterRequest request = mock(RouterRequest.class);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    setUpRequestMocks(ctx, request, allowed, blocked, quotaEnabledTopic);

    long capacity = storeReadQuota * 5 * 10;
    for (int i = 0; i < capacity; i++) {
      quotaEnforcementHandler.channelRead0(ctx, request);
    }
    assertEquals(allowed.get(), capacity);
    assertEquals(blocked.get(), 0);

    quotaEnforcementHandler.channelRead0(ctx, request);

    assertEquals(blocked.get(), 1);

    allowed.set(0);
    blocked.set(0);
    RouterRequest quotaDisabledRequest = mock(RouterRequest.class);
    ChannelHandlerContext quotaDisabledCtx = mock(ChannelHandlerContext.class);
    setUpRequestMocks(quotaDisabledCtx, quotaDisabledRequest, allowed, blocked, quotaDisabledTopic);

    for (int i = 0; i < capacity * 2; i++) {
      quotaEnforcementHandler.channelRead0(quotaDisabledCtx, quotaDisabledRequest);
    }
    // Store that have storage node read quota disabled should not be blocked
    assertEquals(allowed.get(), capacity * 2);
    assertEquals(blocked.get(), 0);
  }

  @Test
  public void testGetBucketForStore() {
    String storeName = "testStore";
    String topic = Version.composeKafkaTopic(storeName, 1);
    Version version = mock(Version.class);
    doReturn(topic).when(version).kafkaTopicName();
    Store store = setUpStoreMock(storeName, 1, Collections.singletonList(version), 100, true);
    doReturn(store).when(storeRepository).getStore(storeName);
    doReturn(Collections.singletonList(store)).when(storeRepository).getAllStores();

    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();
    Partition partition = setUpPartitionMock(topic, thisInstance, true, 0);
    doReturn(0).when(partition).getId();
    PartitionAssignment pa = setUpPartitionAssignmentMock(topic, Collections.singletonList(partition));
    doReturn(pa).when(customizedViewRepository).getPartitionAssignments(topic);
    initializeQuotaEnforcementHandlers();

    quotaEnforcementHandler.onCustomizedViewChange(pa);

    // Non-existent store should return "null" TokenBucket object
    VeniceRateLimiter bucketForInvalidStore = quotaEnforcementHandler.getStoreVersionRateLimiter("incorrect_store");
    assertNull(bucketForInvalidStore);
  }

  @Test
  public void testNodeResponsibility() {
    String storeName = Utils.getUniqueString("test-store");
    int currentVersion = 1;
    int nextVersionNumber = 2;
    String topic = Version.composeKafkaTopic(storeName, currentVersion);
    String nextTopic = Version.composeKafkaTopic(storeName, nextVersionNumber);
    Version version = mock(Version.class);
    doReturn(topic).when(version).kafkaTopicName();
    Version nextVersion = mock(Version.class);
    doReturn(nextTopic).when(nextVersion).kafkaTopicName();
    Store store = setUpStoreMock(storeName, 1, Arrays.asList(version, nextVersion), 10, true);
    doReturn(store).when(storeRepository).getStore(eq(storeName));
    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();
    Partition partition = setUpPartitionMock(topic, thisInstance, true, 0);
    PartitionAssignment pa = setUpPartitionAssignmentMock(topic, Collections.singletonList(partition));
    initializeQuotaEnforcementHandlers();
    quotaEnforcementHandler.onCustomizedViewChange(pa);
    verify(mockAggStats, atMostOnce()).setNodeQuotaResponsibility(eq(storeName), eq(currentVersion), eq(10L));
    Instance instance2 = mock(Instance.class);
    doReturn("node2").when(instance2).getNodeId();
    Partition nextVersionPartition = setUpPartitionMock(topic, Arrays.asList(thisInstance, instance2), true, 0);
    PartitionAssignment nextVersionPa =
        setUpPartitionAssignmentMock(nextTopic, Collections.singletonList(nextVersionPartition));
    quotaEnforcementHandler.onCustomizedViewChange(nextVersionPa);
    // Partition assignment change for the next version should not change store stats node responsibility
    verify(mockAggStats, never()).setNodeQuotaResponsibility(eq(storeName), eq(currentVersion), eq(5L));
    verify(mockAggStats, atMostOnce()).setNodeQuotaResponsibility(eq(storeName), eq(nextVersionNumber), eq(10L));
    Instance instance3 = mock(Instance.class);
    doReturn("node3").when(instance3).getNodeId();
    partition = setUpPartitionMock(topic, Arrays.asList(thisInstance, instance2, instance3), true, 0);
    pa = setUpPartitionAssignmentMock(topic, Collections.singletonList(partition));
    quotaEnforcementHandler.onCustomizedViewChange(pa);
    // Partition assignment change for the current version should change store stats node responsibility
    verify(mockAggStats, atMostOnce()).setNodeQuotaResponsibility(eq(storeName), eq(currentVersion), eq(4L));
    doReturn(nextVersionNumber).when(store).getCurrentVersion();
    doReturn(pa).when(customizedViewRepository).getPartitionAssignments(eq(topic));
    doReturn(nextVersionPa).when(customizedViewRepository).getPartitionAssignments(eq(nextTopic));
    quotaEnforcementHandler.handleStoreChanged(store);
    verify(mockAggStats, atLeastOnce()).setCurrentVersion(eq(storeName), eq(nextVersionNumber));
  }

  @Test
  public void testFailedToInitInStoreRepo() {
    String storeName = Utils.getUniqueString("test-store");
    String storeName2 = Utils.getUniqueString("test-store2");
    String topic = Version.composeKafkaTopic(storeName, 1);
    Version version = mock(Version.class);
    doReturn(topic).when(version).kafkaTopicName();
    Store store = setUpStoreMock(storeName, 1, Collections.singletonList(version), 100, true);
    Store quotaDisabledStore = setUpStoreMock(storeName2, 1, Collections.singletonList(version), 100, false);
    ReadOnlyStoreRepository mockStoreRepo = mock(ReadOnlyStoreRepository.class);
    doReturn(store).when(mockStoreRepo).getStore(storeName);
    doReturn(quotaDisabledStore).when(mockStoreRepo).getStore(storeName2);
    // Throw an exception to mimic failure during init
    doThrow(new VeniceException("Test failed to initialize read quota enforcement handler")).when(mockStoreRepo)
        .getAllStores();
    doReturn(RequestType.SINGLE_GET).when(routerRequest).getRequestType();
    doReturn(storeName).when(routerRequest).getStoreName();
    doReturn(topic).when(routerRequest).getResourceName();
    AggServerQuotaUsageStats stats = mock(AggServerQuotaUsageStats.class);
    ReadQuotaEnforcementHandler quotaEnforcer = new ReadQuotaEnforcementHandler(
        serverConfig,
        mockStoreRepo,
        CompletableFuture.completedFuture(customizedViewRepository),
        storageEngineRepository,
        thisNodeId,
        stats);
    // Ensure init() is invoked before proceeding with the test
    verify(mockStoreRepo, timeout(1000)).getAllStores();
    assertEquals(quotaEnforcer.enforceQuota(routerRequest), QuotaEnforcementResult.ALLOWED);
    verify(stats, times(1)).recordAllowedUnintentionally(storeName, 1);
    RouterRequest quotaDisabledRequest = mock(RouterRequest.class);
    doReturn(storeName2).when(quotaDisabledRequest).getStoreName();
    assertEquals(quotaEnforcer.enforceQuota(quotaDisabledRequest), QuotaEnforcementResult.ALLOWED);
    // Only storage node quota enabled requests should record allowed unintentionally when init fails
    verify(stats, times(1)).recordAllowedUnintentionally(storeName, 1);
  }

  @Test
  public void testFailedToInitInCVForIndividualResources() {
    AggServerQuotaUsageStats stats = mock(AggServerQuotaUsageStats.class);
    String storeName = Utils.getUniqueString("test-store");
    String storeName2 = Utils.getUniqueString("test-store2");
    String topic = Version.composeKafkaTopic(storeName, 1);
    String topic2 = Version.composeKafkaTopic(storeName2, 1);
    Version version = mock(Version.class);
    doReturn(topic).when(version).kafkaTopicName();
    Version version2 = mock(Version.class);
    doReturn(topic2).when(version2).kafkaTopicName();
    doReturn(5).when(version2).getPartitionCount();
    doReturn(VersionStatus.ONLINE).when(version2).getStatus();
    Store store = setUpStoreMock(storeName, 1, Collections.singletonList(version), 100, true);
    Store store2 = setUpStoreMock(storeName2, 1, Collections.singletonList(version2), 100, true);
    doReturn(version2).when(store2).getVersion(1);
    ReadOnlyStoreRepository mockStoreRepo = mock(ReadOnlyStoreRepository.class);
    doReturn(store).when(mockStoreRepo).getStore(storeName);
    doReturn(store2).when(mockStoreRepo).getStore(storeName2);
    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    storeList.add(store2);
    doReturn(storeList).when(mockStoreRepo).getAllStores();
    // An uncaught exception for store2 v1 resource shouldn't fail initialization of other stores
    doThrow(new VeniceException("Unexpected test exception")).when(customizedViewRepository)
        .getPartitionAssignments(topic2);
    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();
    Partition partition = setUpPartitionMock(topic, thisInstance, true, 0);
    PartitionAssignment pa = setUpPartitionAssignmentMock(topic, Collections.singletonList(partition));
    doReturn(pa).when(customizedViewRepository).getPartitionAssignments(topic);
    final ReadQuotaEnforcementHandler quotaEnforcer = new ReadQuotaEnforcementHandler(
        serverConfig,
        mockStoreRepo,
        CompletableFuture.completedFuture(customizedViewRepository),
        storageEngineRepository,
        thisNodeId,
        stats);
    // Ensure init() is completed before proceeding with the test
    TestUtils
        .waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> Assert.assertTrue(quotaEnforcer.isInitialized()));
    Assert.assertNull(quotaEnforcer.getStoreVersionRateLimiter(topic2));
    Assert.assertNotNull(quotaEnforcer.getStoreVersionRateLimiter(topic));
    Assert.assertEquals(quotaEnforcer.getStoreVersionRateLimiter(topic).getQuota(), 100L);

    // If failure is due to CV unavailability the fallback behavior should be based on storage engine partition count
    doReturn(true).when(serverConfig).isReadQuotaInitializationFallbackEnabled();
    doThrow(new VeniceNoHelixResourceException(topic2)).when(customizedViewRepository).getPartitionAssignments(topic2);
    StorageEngine mockStorageEngine = mock(StorageEngine.class);
    // Only 1 partition, partition 0 is assigned to this node based on storage engine
    doReturn(Collections.singleton(0)).when(mockStorageEngine).getPartitionIds();
    doReturn(mockStorageEngine).when(storageEngineRepository).getLocalStorageEngine(topic2);
    final ReadQuotaEnforcementHandler quotaEnforcerWithFallback = new ReadQuotaEnforcementHandler(
        serverConfig,
        mockStoreRepo,
        CompletableFuture.completedFuture(customizedViewRepository),
        storageEngineRepository,
        thisNodeId,
        stats);
    // Ensure init() is invoked before proceeding with the test
    TestUtils.waitForNonDeterministicAssertion(
        50000,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(quotaEnforcerWithFallback.isInitialized()));
    Assert.assertNotNull(quotaEnforcerWithFallback.getStoreVersionRateLimiter(topic));
    Assert.assertNotNull(quotaEnforcerWithFallback.getStoreVersionRateLimiter(topic2));
    Assert.assertEquals(quotaEnforcerWithFallback.getStoreVersionRateLimiter(topic).getQuota(), 100L);
    // The node responsibility should be 1/5 because the store has 5 partitions and the instance was assigned only 1
    // partition based on storage engine. 1/5 * 100 = 20.
    Assert.assertEquals(quotaEnforcerWithFallback.getStoreVersionRateLimiter(topic2).getQuota(), 20L);

    // If the fallback behavior config is disabled we should fail open.
    doReturn(false).when(serverConfig).isReadQuotaInitializationFallbackEnabled();
    AggServerQuotaUsageStats failOpenStats = mock(AggServerQuotaUsageStats.class);
    final ReadQuotaEnforcementHandler quotaEnforcerWithoutFallback = new ReadQuotaEnforcementHandler(
        serverConfig,
        mockStoreRepo,
        CompletableFuture.completedFuture(customizedViewRepository),
        storageEngineRepository,
        thisNodeId,
        failOpenStats);
    // Ensure init() is invoked before proceeding with the test
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(quotaEnforcerWithoutFallback.isInitialized()));
    Assert.assertNotNull(quotaEnforcerWithoutFallback.getStoreVersionRateLimiter(topic));
    Assert.assertNull(quotaEnforcerWithoutFallback.getStoreVersionRateLimiter(topic2));
    Assert.assertEquals(quotaEnforcerWithoutFallback.getStoreVersionRateLimiter(topic).getQuota(), 100L);
    doReturn(RequestType.SINGLE_GET).when(routerRequest).getRequestType();
    doReturn(storeName2).when(routerRequest).getStoreName();
    doReturn(topic2).when(routerRequest).getResourceName();
    assertEquals(quotaEnforcerWithoutFallback.enforceQuota(routerRequest), QuotaEnforcementResult.ALLOWED);
    verify(failOpenStats, times(1)).recordAllowedUnintentionally(storeName2, 1);
  }

  @Test
  public void testStoreQuotaChange() {
    AggServerQuotaUsageStats stats = mock(AggServerQuotaUsageStats.class);
    String storeName = Utils.getUniqueString("test-store");
    String topic = Version.composeKafkaTopic(storeName, 1);
    Version version = mock(Version.class);
    doReturn(topic).when(version).kafkaTopicName();
    Store store = setUpStoreMock(storeName, 1, Collections.singletonList(version), 100, true);
    ReadOnlyStoreRepository mockStoreRepo = mock(ReadOnlyStoreRepository.class);
    doReturn(store).when(mockStoreRepo).getStore(storeName);
    List<Store> storeList = Collections.singletonList(store);
    doReturn(storeList).when(mockStoreRepo).getAllStores();
    Instance thisInstance = mock(Instance.class);
    doReturn(thisNodeId).when(thisInstance).getNodeId();
    Partition partition = setUpPartitionMock(topic, thisInstance, true, 0);
    PartitionAssignment pa = setUpPartitionAssignmentMock(topic, Collections.singletonList(partition));
    doReturn(pa).when(customizedViewRepository).getPartitionAssignments(topic);
    final ReadQuotaEnforcementHandler quotaEnforcer = new ReadQuotaEnforcementHandler(
        serverConfig,
        mockStoreRepo,
        CompletableFuture.completedFuture(customizedViewRepository),
        storageEngineRepository,
        thisNodeId,
        stats);
    // Ensure init() is completed before proceeding with the test
    TestUtils
        .waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> Assert.assertTrue(quotaEnforcer.isInitialized()));
    Assert.assertNotNull(quotaEnforcer.getStoreVersionRateLimiter(topic));
    Assert.assertEquals(quotaEnforcer.getStoreVersionRateLimiter(topic).getQuota(), 100L);
    doReturn(200L).when(store).getReadQuotaInCU();
    quotaEnforcer.handleStoreChanged(store);
    Assert.assertEquals(quotaEnforcer.getStoreVersionRateLimiter(topic).getQuota(), 200L);
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
      quotaEnforcementHandler.channelRead0(ctx, request);
    }
    assertEquals(allowed.get(), capacity, "Made " + capacity + " reads, and all should have been allowed");
    assertEquals(blocked.get(), 0, "Didn't exceed " + capacity + " reads, but " + blocked.get() + " were throttled");
    quotaEnforcementHandler.channelRead0(ctx, request);
    assertEquals(blocked.get(), 1, "After reading capacity of " + capacity + " next read should have been blocked");

    allowed.set(0);
    blocked.set(0);

    // allow one refill
    currentTime = currentTime + refillTimeMs + 1; // refill checks if past refill time, hence +1
    doReturn(currentTime).when(clock).millis();

    for (int i = 0; i < refillAmount; i++) {
      quotaEnforcementHandler.channelRead0(ctx, request);
    }
    assertEquals(
        allowed.get(),
        refillAmount,
        "Made " + refillAmount + " reads after refill, and all should have been allowed");
    assertEquals(blocked.get(), 0, "After refill, reads should not be throttled");
    quotaEnforcementHandler.channelRead0(ctx, request);
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

  private PartitionAssignment setUpPartitionAssignmentMock(String topic, List<Partition> partitions) {
    PartitionAssignment pa = mock(PartitionAssignment.class);
    doReturn(topic).when(pa).getTopic();
    doReturn(partitions).when(pa).getAllPartitions();
    return pa;
  }

  private Partition setUpPartitionMock(String topic, Instance instance, boolean isReadyToServe, int partitionId) {
    Partition partition = mock(Partition.class);
    doReturn(partitionId).when(partition).getId();
    List<ReplicaState> replicaStates = new ArrayList<>();
    ReplicaState thisReplicaState = mock(ReplicaState.class);
    doReturn(instance.getNodeId()).when(thisReplicaState).getParticipantId();
    doReturn(isReadyToServe).when(thisReplicaState).isReadyToServe();
    if (isReadyToServe) {
      doReturn(Collections.singletonList(instance)).when(partition).getReadyToServeInstances();
    }
    replicaStates.add(thisReplicaState);
    when(customizedViewRepository.getReplicaStates(topic, partition.getId())).thenReturn(replicaStates);
    return partition;
  }

  private Partition setUpPartitionMock(
      String topic,
      List<Instance> instances,
      boolean isReadyToServe,
      int partitionId) {
    Partition partition = mock(Partition.class);
    doReturn(partitionId).when(partition).getId();
    List<ReplicaState> replicaStates = new ArrayList<>();
    for (Instance instance: instances) {
      ReplicaState thisReplicaState = mock(ReplicaState.class);
      doReturn(instance.getNodeId()).when(thisReplicaState).getParticipantId();
      doReturn(isReadyToServe).when(thisReplicaState).isReadyToServe();
      replicaStates.add(thisReplicaState);
    }
    if (isReadyToServe) {
      doReturn(instances).when(partition).getReadyToServeInstances();
    }
    when(customizedViewRepository.getReplicaStates(topic, partition.getId())).thenReturn(replicaStates);
    return partition;
  }

  private Store setUpStoreMock(
      String storeName,
      int currentVersion,
      List<Version> versionList,
      long readQuota,
      boolean readQuotaEnabled) {
    Store store = mock(Store.class);
    doReturn(storeName).when(store).getName();
    doReturn(currentVersion).when(store).getCurrentVersion();
    doReturn(versionList).when(store).getVersions();
    doReturn(readQuota).when(store).getReadQuotaInCU();
    doReturn(readQuotaEnabled).when(store).isStorageNodeReadQuotaEnabled();
    doReturn(VersionStatus.ONLINE).when(store).getVersionStatus(anyInt());
    return store;
  }

  @Test
  public void testGetRateLimiter() {
    String resourceName = "testStore_v1";
    long quotaInRcu = 10;
    double thisNodeQuotaResponsibility = 0.5;
    RateLimiterType rateLimiterType = RateLimiterType.EVENT_THROTTLER_WITH_SILENT_REJECTION;
    int quotaEnforcementIntervalInMs = 10000;
    int enforcementCapacityMultiple = 5;
    Clock clock = Clock.systemUTC();

    VeniceRateLimiter currentRateLimiter = mock(VeniceRateLimiter.class);
    when(currentRateLimiter.getQuota()).thenReturn((long) (quotaInRcu * thisNodeQuotaResponsibility));
    // check getRateLimiter returns the same rate limiter if the quota is the same as the current rate limiter
    VeniceRateLimiter rateLimiter = ReadQuotaEnforcementHandler.getRateLimiter(
        resourceName,
        quotaInRcu,
        thisNodeQuotaResponsibility,
        currentRateLimiter,
        rateLimiterType,
        quotaEnforcementIntervalInMs,
        enforcementCapacityMultiple,
        clock);
    assertEquals(rateLimiter, currentRateLimiter);

    // check getRateLimiter returns a new rate limiter if the quota is different from the current rate limiter
    VeniceRateLimiter newRateLimiter = ReadQuotaEnforcementHandler.getRateLimiter(
        resourceName,
        quotaInRcu + 1,
        thisNodeQuotaResponsibility,
        currentRateLimiter,
        rateLimiterType,
        quotaEnforcementIntervalInMs,
        enforcementCapacityMultiple,
        clock);
    assertNotEquals(newRateLimiter, currentRateLimiter);

    // check getRateLimiter returns a new rate limiter if the quota is the same as the current rate limiter but the
    // responsibility is different
    newRateLimiter = ReadQuotaEnforcementHandler.getRateLimiter(
        resourceName,
        quotaInRcu,
        thisNodeQuotaResponsibility + 0.1,
        currentRateLimiter,
        rateLimiterType,
        quotaEnforcementIntervalInMs,
        enforcementCapacityMultiple,
        clock);
    assertNotEquals(newRateLimiter, currentRateLimiter);

    // check returns GUAVA_RATE_LIMITER if the rate limiter type is GUAVA_RATE_LIMITER
    newRateLimiter = ReadQuotaEnforcementHandler.getRateLimiter(
        resourceName,
        quotaInRcu,
        thisNodeQuotaResponsibility,
        null,
        RateLimiterType.GUAVA_RATE_LIMITER,
        quotaEnforcementIntervalInMs,
        enforcementCapacityMultiple,
        clock);
    assertTrue(newRateLimiter instanceof GuavaRateLimiter);

    // check default rate limiter type is used if the rate limiter type is null
    newRateLimiter = ReadQuotaEnforcementHandler.getRateLimiter(
        resourceName,
        quotaInRcu,
        thisNodeQuotaResponsibility,
        null,
        null,
        quotaEnforcementIntervalInMs,
        enforcementCapacityMultiple,
        clock);
    assertTrue(newRateLimiter instanceof TokenBucket);
  }
}
