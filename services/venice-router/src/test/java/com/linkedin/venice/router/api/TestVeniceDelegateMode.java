package com.linkedin.venice.router.api;

import static com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy.HELIX_ASSISTED_ROUTING;
import static com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy.LEAST_LOADED_ROUTING;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.router.api.HostFinder;
import com.linkedin.alpini.router.api.HostHealthMonitor;
import com.linkedin.alpini.router.api.PartitionFinder;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.alpini.router.api.Scatter;
import com.linkedin.alpini.router.api.ScatterGatherRequest;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixInstanceConfigRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.api.routing.helix.HelixGroupSelectionStrategyEnum;
import com.linkedin.venice.router.api.routing.helix.HelixGroupSelector;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.HttpMethod;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpUriRequest;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestVeniceDelegateMode {
  private RetryManager retryManager;

  private VenicePath getVenicePath(
      String storeName,
      int version,
      String resourceName,
      RequestType requestType,
      List<RouterKey> keys) {
    return getVenicePath(storeName, version, resourceName, requestType, keys, Collections.emptySet());
  }

  private VenicePath getVenicePath(
      String storeName,
      int version,
      String resourceName,
      RequestType requestType,
      List<RouterKey> keys,
      Set<String> slowStorageNodeSet) {
    retryManager = mock(RetryManager.class);
    return new VenicePath(storeName, version, resourceName, false, -1, retryManager) {
      private final String ROUTER_REQUEST_VERSION =
          Integer.toString(ReadAvroProtocolDefinition.SINGLE_GET_ROUTER_REQUEST_V1.getProtocolVersion());

      @Override
      public RequestType getRequestType() {
        return requestType;
      }

      @Override
      public VenicePath substitutePartitionKey(RouterKey s) {
        return null;
      }

      @Override
      public VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s) {
        return null;
      }

      @Override
      public HttpUriRequest composeRouterRequestInternal(String storageNodeUri) {
        return null;
      }

      @Override
      public HttpMethod getHttpMethod() {
        if (requestType.equals(RequestType.SINGLE_GET)) {
          return HttpMethod.GET;
        } else {
          return HttpMethod.POST;
        }
      }

      @Override
      public byte[] getBody() {
        return null;
      }

      public String getVeniceApiVersionHeader() {
        return ROUTER_REQUEST_VERSION;
      }

      @Nonnull
      @Override
      public String getLocation() {
        return "fake_location";
      }

      public Collection<RouterKey> getPartitionKeys() {
        return keys;
      }

      @Override
      public boolean canRequestStorageNode(String nodeId) {
        return !slowStorageNodeSet.contains(nodeId);
      }
    };
  }

  private PartitionFinder<RouterKey> getPartitionFinder(Map<RouterKey, String> keyPartitionMap) {
    int inferredPartitionCount = keyPartitionMap.keySet()
        .stream()
        .max(Comparator.comparing(RouterKey::getPartitionId))
        .map(RouterKey::getPartitionId)
        .orElse(0) + 1;
    return getPartitionFinder(keyPartitionMap, inferredPartitionCount);
  }

  private PartitionFinder<RouterKey> getPartitionFinder(Map<RouterKey, String> keyPartitionMap, int partitionCount) {
    return new PartitionFinder<RouterKey>() {
      @Nonnull
      @Override
      public String findPartitionName(@Nonnull String resourceName, @Nonnull RouterKey partitionKey)
          throws RouterException {
        String partitionName = keyPartitionMap.get(partitionKey);
        if (partitionName != null) {
          return partitionName;
        }
        throw new VeniceException("Unknown partition key: " + partitionKey);
      }

      @Nonnull
      @Override
      public List<String> getAllPartitionNames(@Nonnull String resourceName) throws RouterException {
        Set<String> partitionSet = new HashSet(keyPartitionMap.values());
        return new ArrayList<>(partitionSet);
      }

      @Override
      public int getNumPartitions(@Nonnull String resourceName) throws RouterException {
        return partitionCount;
      }

      @Override
      public int findPartitionNumber(
          @Nonnull RouterKey partitionKey,
          int numPartitions,
          String storeName,
          int versionNumber) throws RouterException {
        String partitionName = findPartitionName(Version.composeKafkaTopic(storeName, versionNumber), partitionKey);
        return HelixUtils.getPartitionId(partitionName);
      }
    };
  }

  private HostFinder<Instance, VeniceRole> getHostFinder(Map<String, List<Instance>> partitionHostMap) {
    VeniceHostFinder veniceHostFinder = mock(VeniceHostFinder.class);
    ArgumentCaptor<String> resourceNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> partitionNumberCaptor = ArgumentCaptor.forClass(Integer.class);
    when(
        veniceHostFinder
            .findHosts(anyString(), resourceNameCaptor.capture(), anyString(), partitionNumberCaptor.capture(), any()))
                .then(invocation -> {
                  String partitionName =
                      HelixUtils.getPartitionName(resourceNameCaptor.getValue(), partitionNumberCaptor.getValue());
                  List<Instance> hosts = partitionHostMap.get(partitionName);
                  return hosts == null ? Collections.emptyList() : hosts;
                });
    return veniceHostFinder;
  }

  private HostHealthMonitor<Instance> getHostHealthMonitor() {
    return (hostName, partitionName) -> true;
  }

  private ReadRequestThrottler getReadRequestThrottle(boolean throttle) {
    ReadRequestThrottler throttler = mock(ReadRequestThrottler.class);
    doReturn(1).when(throttler).getReadCapacity();
    if (throttle) {
      doThrow(new QuotaExceededException("test", "10", "5")).when(throttler).mayThrottleRead(any(), anyInt());
    }

    return throttler;
  }

  private VenicePathParser getPathParser() {
    return mock(VenicePathParser.class);
  }

  @BeforeClass
  public void setUp() {
    RouterExceptionAndTrackingUtils.setRouterStats(
        new RouterStats<>(
            requestType -> new AggRouterHttpRequestStats(
                "test-cluster",
                new VeniceMetricsRepository(),
                requestType,
                mock(ReadOnlyStoreRepository.class),
                true)));
  }

  @AfterClass
  public void cleanUp() {
    RouterExceptionAndTrackingUtils.setRouterStats(null);
  }

  @Test
  public void testScatterWithSingleGet() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int version = 1;
    String resourceName = storeName + "_v" + version;
    RouterKey key = new RouterKey("key_1".getBytes());
    List<RouterKey> keys = new ArrayList<>();
    keys.add(key);
    VenicePath path = getVenicePath(storeName, version, resourceName, RequestType.SINGLE_GET, keys);
    Scatter<Instance, VenicePath, RouterKey> scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    String requestMethod = HttpMethod.GET.name();
    Map<RouterKey, String> keyPartitionMap = new HashMap<>();
    String partitionName = resourceName + "_1";
    keyPartitionMap.put(key, partitionName);
    PartitionFinder partitionFinder = getPartitionFinder(keyPartitionMap, 2);
    Instance instance1 = new Instance("host1_123", "host1", 123);
    Instance instance2 = new Instance("host2_123", "host2", 123);
    Instance instance3 = new Instance("host3_123", "host3", 123);
    List<Instance> instanceList = new ArrayList<>();
    instanceList.add(instance1);
    instanceList.add(instance2);
    instanceList.add(instance3);
    Map<String, List<Instance>> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put(partitionName, instanceList);
    HostFinder<Instance, VeniceRole> hostFinder = getHostFinder(partitionInstanceMap);
    HostHealthMonitor monitor = getHostHealthMonitor();
    ReadRequestThrottler throttler = getReadRequestThrottle(false);

    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(LEAST_LOADED_ROUTING).when(config).getMultiKeyRoutingStrategy();
    VeniceDelegateMode scatterMode =
        new VeniceDelegateMode(config, mock(RouterStats.class), mock(RouteHttpRequestStats.class));

    scatterMode.initReadRequestThrottler(throttler);

    Scatter<Instance, VenicePath, RouterKey> finalScatter = scatterMode
        .scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA);

    // Throttling for single-get request is not happening in VeniceDelegateMode
    verify(throttler, never()).mayThrottleRead(eq(storeName), eq(1));
    Collection<ScatterGatherRequest<Instance, RouterKey>> requests = finalScatter.getOnlineRequests();
    Assert.assertEquals(requests.size(), 1, "There should be only one online request since there is only one key");
    ScatterGatherRequest<Instance, RouterKey> request = requests.iterator().next();
    List<Instance> hosts = request.getHosts();
    Assert.assertEquals(hosts.size(), 1, "There should be only one chose host");
    Instance selectedHost = hosts.get(0);
    Assert.assertTrue(instanceList.contains(selectedHost));
    verify(retryManager, times(1)).recordRequest();
    verify(retryManager, never()).isRetryAllowed(anyInt());

    // Verify retry manager behavior for retry request
    path = getVenicePath(storeName, version, resourceName, RequestType.SINGLE_GET, keys);
    doReturn(true).when(retryManager).isRetryAllowed(anyInt());
    path.setRetryRequest();
    scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    RouterStats routerStats = mock(RouterStats.class);
    doReturn(mock(AggRouterHttpRequestStats.class)).when(routerStats).getStatsByType(any());
    scatterMode = new VeniceDelegateMode(config, routerStats, mock(RouteHttpRequestStats.class));
    scatterMode.initReadRequestThrottler(getReadRequestThrottle(false));
    scatterMode.scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA);
    verify(retryManager, never()).recordRequest();
    verify(retryManager, times(1)).isRetryAllowed(anyInt());
  }

  @Test(expectedExceptions = RouterException.class, expectedExceptionsMessageRegExp = ".*not available to serve request of type: SINGLE_GET")
  public void testScatterWithSingleGetWithNotAvailablePartition() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int version = 1;
    String resourceName = storeName + "_v" + version;
    RouterKey key = new RouterKey("key_1".getBytes());
    List<RouterKey> keys = new ArrayList<>();
    keys.add(key);
    VenicePath path = getVenicePath(storeName, version, resourceName, RequestType.SINGLE_GET, keys);
    Scatter<Instance, VenicePath, RouterKey> scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    String requestMethod = HttpMethod.GET.name();
    Map<RouterKey, String> keyPartitionMap = new HashMap<>();
    String partitionName = resourceName + "_1";
    keyPartitionMap.put(key, partitionName);
    PartitionFinder partitionFinder = getPartitionFinder(keyPartitionMap, 2);

    Map<String, List<Instance>> partitionInstanceMap = new HashMap<>();
    HostFinder<Instance, VeniceRole> hostFinder = getHostFinder(partitionInstanceMap);
    HostHealthMonitor monitor = getHostHealthMonitor();
    ReadRequestThrottler throttler = getReadRequestThrottle(false);
    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(LEAST_LOADED_ROUTING).when(config).getMultiKeyRoutingStrategy();

    VeniceDelegateMode scatterMode =
        new VeniceDelegateMode(config, mock(RouterStats.class), mock(RouteHttpRequestStats.class));
    scatterMode.initReadRequestThrottler(throttler);

    scatterMode.scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA);
  }

  @Test(expectedExceptions = RouterException.class, expectedExceptionsMessageRegExp = ".*not available to serve retry request of type: MULTI_GET")
  public void testLeastLoadedOnSlowHosts() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int version = 1;
    String resourceName = storeName + "_v" + version;
    RouterKey key1 = new RouterKey("key_1".getBytes());
    key1.setPartitionId(1);
    List<RouterKey> keys = new ArrayList<>();
    keys.add(key1);
    Instance instance1 = new Instance("host1_123", "host1", 123);
    Instance instance2 = new Instance("host2_123", "host2", 123);
    Set<String> slowStorageNodeSet = new HashSet<>();
    slowStorageNodeSet.add(instance1.getNodeId());
    slowStorageNodeSet.add(instance2.getNodeId());
    VenicePath path = getVenicePath(storeName, version, resourceName, RequestType.MULTI_GET, keys, slowStorageNodeSet);
    path.setRetryRequest();
    Scatter<Instance, VenicePath, RouterKey> scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    String requestMethod = HttpMethod.POST.name();

    Map<RouterKey, String> keyPartitionMap = new HashMap<>();
    String p1 = HelixUtils.getPartitionName(resourceName, 1);
    String p2 = HelixUtils.getPartitionName(resourceName, 2);
    String p3 = HelixUtils.getPartitionName(resourceName, 3);
    keyPartitionMap.put(key1, p1);
    PartitionFinder partitionFinder = getPartitionFinder(keyPartitionMap);
    ReadRequestThrottler throttler = getReadRequestThrottle(false);

    List<Instance> instanceListForP1 = new ArrayList<>();
    instanceListForP1.add(instance1);
    instanceListForP1.add(instance2);
    List<Instance> instanceListForP2 = new ArrayList<>();
    instanceListForP2.add(instance1);
    instanceListForP2.add(instance2);
    List<Instance> instanceListForP3 = new ArrayList<>();
    instanceListForP3.add(instance1);
    Map<String, List<Instance>> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put(p1, instanceListForP1);
    partitionInstanceMap.put(p2, instanceListForP2);
    partitionInstanceMap.put(p3, instanceListForP3);
    HostFinder<Instance, VeniceRole> hostFinder = getHostFinder(partitionInstanceMap);
    HostHealthMonitor monitor = getHostHealthMonitor();
    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(LEAST_LOADED_ROUTING).when(config).getMultiKeyRoutingStrategy();

    VeniceDelegateMode scatterMode = new VeniceDelegateMode(
        config,
        new RouterStats<>(
            requestType -> new AggRouterHttpRequestStats(
                "test-cluster",
                new VeniceMetricsRepository(),
                requestType,
                mock(ReadOnlyStoreRepository.class),
                true)),
        mock(RouteHttpRequestStats.class));
    scatterMode.initReadRequestThrottler(throttler);

    Scatter<Instance, VenicePath, RouterKey> finalScatter = scatterMode
        .scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA);

    Collection<ScatterGatherRequest<Instance, RouterKey>> requests = finalScatter.getOnlineRequests();
    Assert.assertEquals(requests.size(), 3);
  }

  /**
   * Once the scatter algo for multi-get gets changed in the future, the expectation in the following test should be
   * changed accordingly.
   *
   * @throws RouterException
   */
  @Test
  public void testScatterWithMultiGet() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int version = 1;
    String resourceName = storeName + "_v" + version;
    RouterKey key1 = new RouterKey("key_1".getBytes());
    key1.setPartitionId(1);
    RouterKey key2 = new RouterKey("key_2".getBytes());
    key2.setPartitionId(2);
    RouterKey key3 = new RouterKey("key_3".getBytes());
    key3.setPartitionId(3);
    RouterKey key4 = new RouterKey("key_4".getBytes());
    key4.setPartitionId(4);
    RouterKey key5 = new RouterKey("key_5".getBytes());
    key5.setPartitionId(5);
    RouterKey key6 = new RouterKey("key_6".getBytes());
    key6.setPartitionId(6);
    List<RouterKey> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);
    keys.add(key3);
    keys.add(key4);
    keys.add(key5);
    keys.add(key6);
    Instance instance1 = new Instance("host1_123", "host1", 123);
    Instance instance2 = new Instance("host2_123", "host2", 123);
    Instance instance3 = new Instance("host3_123", "host3", 123);
    Instance instance4 = new Instance("host4_123", "host4", 123);
    Instance instance5 = new Instance("host5_123", "host5", 123);

    VenicePath path = getVenicePath(storeName, version, resourceName, RequestType.MULTI_GET, keys);
    Scatter<Instance, VenicePath, RouterKey> scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    String requestMethod = HttpMethod.POST.name();

    Map<RouterKey, String> keyPartitionMap = new HashMap<>();
    String p1 = HelixUtils.getPartitionName(resourceName, 1);
    String p2 = HelixUtils.getPartitionName(resourceName, 2);
    String p3 = HelixUtils.getPartitionName(resourceName, 3);
    String p4 = HelixUtils.getPartitionName(resourceName, 4);
    String p5 = HelixUtils.getPartitionName(resourceName, 5);
    String p6 = HelixUtils.getPartitionName(resourceName, 6);
    keyPartitionMap.put(key1, p1);
    keyPartitionMap.put(key2, p2);
    keyPartitionMap.put(key3, p3);
    keyPartitionMap.put(key4, p4);
    keyPartitionMap.put(key5, p5);
    keyPartitionMap.put(key6, p6);
    PartitionFinder partitionFinder = getPartitionFinder(keyPartitionMap);

    List<Instance> instanceListForP1 = new ArrayList<>();
    instanceListForP1.add(instance1);
    instanceListForP1.add(instance2);
    List<Instance> instanceListForP2 = new ArrayList<>();
    instanceListForP2.add(instance1);
    instanceListForP2.add(instance2);
    List<Instance> instanceListForP3 = new ArrayList<>();
    instanceListForP3.add(instance1);
    instanceListForP3.add(instance3);
    List<Instance> instanceListForP4 = new ArrayList<>();
    instanceListForP4.add(instance1);
    instanceListForP4.add(instance3);
    List<Instance> instanceListForP5 = new ArrayList<>();
    instanceListForP5.add(instance2);
    instanceListForP5.add(instance4);
    List<Instance> instanceListForP6 = new ArrayList<>();
    instanceListForP6.add(instance5);
    instanceListForP6.add(instance3);
    Map<String, List<Instance>> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put(p1, instanceListForP1);
    partitionInstanceMap.put(p2, instanceListForP2);
    partitionInstanceMap.put(p3, instanceListForP3);
    partitionInstanceMap.put(p4, instanceListForP4);
    partitionInstanceMap.put(p5, instanceListForP5);
    partitionInstanceMap.put(p6, instanceListForP6);

    HostFinder<Instance, VeniceRole> hostFinder = getHostFinder(partitionInstanceMap);
    HostHealthMonitor monitor = getHostHealthMonitor();
    ReadRequestThrottler throttler = getReadRequestThrottle(false);
    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(LEAST_LOADED_ROUTING).when(config).getMultiKeyRoutingStrategy();

    VeniceDelegateMode scatterMode =
        new VeniceDelegateMode(config, mock(RouterStats.class), mock(RouteHttpRequestStats.class));
    scatterMode.initReadRequestThrottler(throttler);

    Scatter<Instance, VenicePath, RouterKey> finalScatter = scatterMode
        .scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA);

    Collection<ScatterGatherRequest<Instance, RouterKey>> requests = finalScatter.getOnlineRequests();
    Assert.assertEquals(requests.size(), 3);

    // Verify throttling
    verify(throttler).mayThrottleRead(storeName, 4);
    verify(throttler, times(2)).mayThrottleRead(eq(storeName), eq(1.0d));

    // each request should only have one 'Instance'
    requests.stream()
        .forEach(
            request -> Assert
                .assertEquals(request.getHosts().size(), 1, "There should be only one host for each request"));
    Set<Instance> instanceSet = new HashSet<>();
    requests.stream().forEach(request -> instanceSet.add(request.getHosts().get(0)));
    Assert.assertTrue(instanceSet.contains(instance1), "instance1 must be selected");
    Assert.assertTrue(
        instanceSet.contains(instance2) || instanceSet.contains(instance4),
        "One of instance2/instance4 should be selected");
    Assert.assertTrue(
        instanceSet.contains(instance3) || instanceSet.contains(instance5),
        "One of instance3/instance5 should be selected");
    verify(retryManager, times(3)).recordRequest();
    verify(retryManager, never()).isRetryAllowed(anyInt());

    // Verify retry manager behavior for retry request
    path = getVenicePath(storeName, version, resourceName, RequestType.MULTI_GET, keys);
    doReturn(true).when(retryManager).isRetryAllowed(anyInt());
    path.setRetryRequest();
    scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    RouterStats routerStats = mock(RouterStats.class);
    doReturn(mock(AggRouterHttpRequestStats.class)).when(routerStats).getStatsByType(any());
    scatterMode = new VeniceDelegateMode(config, routerStats, mock(RouteHttpRequestStats.class));
    scatterMode.initReadRequestThrottler(getReadRequestThrottle(false));
    scatterMode.scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA);
    verify(retryManager, never()).recordRequest();
    verify(retryManager, atLeastOnce()).isRetryAllowed(eq(3));
  }

  @Test
  public void testScatterWithStreamingMultiGet() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int version = 1;
    String resourceName = storeName + "_v" + version;
    RouterKey key1 = new RouterKey("key_1".getBytes());
    key1.setPartitionId(1);
    RouterKey key2 = new RouterKey("key_2".getBytes());
    key2.setPartitionId(2);
    RouterKey key3 = new RouterKey("key_3".getBytes());
    key3.setPartitionId(3);
    RouterKey key4 = new RouterKey("key_4".getBytes());
    key4.setPartitionId(4);
    RouterKey key5 = new RouterKey("key_5".getBytes());
    key5.setPartitionId(5);
    RouterKey key6 = new RouterKey("key_6".getBytes());
    key6.setPartitionId(6);
    List<RouterKey> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);
    keys.add(key3);
    keys.add(key4);
    keys.add(key5);
    keys.add(key6);
    VenicePath path = getVenicePath(storeName, version, resourceName, RequestType.MULTI_GET_STREAMING, keys);
    Scatter<Instance, VenicePath, RouterKey> scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    String requestMethod = HttpMethod.POST.name();

    Map<RouterKey, String> keyPartitionMap = new HashMap<>();
    String p1 = HelixUtils.getPartitionName(resourceName, 1);
    String p2 = HelixUtils.getPartitionName(resourceName, 2);
    String p3 = HelixUtils.getPartitionName(resourceName, 3);
    String p4 = HelixUtils.getPartitionName(resourceName, 4);
    String p5 = HelixUtils.getPartitionName(resourceName, 5);
    keyPartitionMap.put(key1, p1);
    keyPartitionMap.put(key2, p2);
    keyPartitionMap.put(key3, p3);
    keyPartitionMap.put(key4, p4);
    keyPartitionMap.put(key5, p5);
    PartitionFinder partitionFinder = getPartitionFinder(keyPartitionMap, 7);

    Instance instance1 = new Instance("host1_123", "host1", 123);
    Instance instance2 = new Instance("host2_123", "host2", 123);
    Instance instance3 = new Instance("host3_123", "host3", 123);
    Instance instance4 = new Instance("host4_123", "host4", 123);
    List<Instance> instanceListForP1 = new ArrayList<>();
    instanceListForP1.add(instance1);
    instanceListForP1.add(instance2);
    List<Instance> instanceListForP2 = new ArrayList<>();
    instanceListForP2.add(instance1);
    instanceListForP2.add(instance2);
    List<Instance> instanceListForP3 = new ArrayList<>();
    instanceListForP3.add(instance1);
    instanceListForP3.add(instance3);
    List<Instance> instanceListForP4 = new ArrayList<>();
    instanceListForP4.add(instance1);
    instanceListForP4.add(instance3);
    List<Instance> instanceListForP5 = new ArrayList<>();
    instanceListForP5.add(instance2);
    instanceListForP5.add(instance4);

    Map<String, List<Instance>> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put(p1, instanceListForP1);
    partitionInstanceMap.put(p2, instanceListForP2);
    partitionInstanceMap.put(p3, instanceListForP3);
    partitionInstanceMap.put(p4, instanceListForP4);
    partitionInstanceMap.put(p5, instanceListForP5);

    HostFinder<Instance, VeniceRole> hostFinder = getHostFinder(partitionInstanceMap);
    HostHealthMonitor monitor = getHostHealthMonitor();
    ReadRequestThrottler throttler = getReadRequestThrottle(false);

    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(LEAST_LOADED_ROUTING).when(config).getMultiKeyRoutingStrategy();

    VeniceDelegateMode scatterMode =
        new VeniceDelegateMode(config, mock(RouterStats.class), mock(RouteHttpRequestStats.class));
    scatterMode.initReadRequestThrottler(throttler);

    Scatter<Instance, VenicePath, RouterKey> finalScatter = scatterMode
        .scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA);

    Collection<ScatterGatherRequest<Instance, RouterKey>> requests = finalScatter.getOfflineRequests();
    Assert.assertEquals(requests.size(), 1);
  }

  @Test
  public void testScatterForMultiGetWithHelixAssistedRouting() throws RouterException {
    String storeName = Utils.getUniqueString("test_store");
    int version = 1;
    String resourceName = storeName + "_v" + version;
    RouterKey key1 = new RouterKey("key_1".getBytes());
    key1.setPartitionId(1);
    RouterKey key2 = new RouterKey("key_2".getBytes());
    key2.setPartitionId(2);
    RouterKey key3 = new RouterKey("key_3".getBytes());
    key3.setPartitionId(3);
    RouterKey key4 = new RouterKey("key_4".getBytes());
    key4.setPartitionId(4);
    RouterKey key5 = new RouterKey("key_5".getBytes());
    key5.setPartitionId(5);
    RouterKey key6 = new RouterKey("key_6".getBytes());
    key6.setPartitionId(6);
    List<RouterKey> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);
    keys.add(key3);
    keys.add(key4);
    keys.add(key5);
    keys.add(key6);
    VenicePath path = getVenicePath(storeName, version, resourceName, RequestType.MULTI_GET, keys);
    Scatter<Instance, VenicePath, RouterKey> scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    String requestMethod = HttpMethod.POST.name();

    Map<RouterKey, String> keyPartitionMap = new HashMap<>();
    String p1 = HelixUtils.getPartitionName(resourceName, 1);
    String p2 = HelixUtils.getPartitionName(resourceName, 2);
    String p3 = HelixUtils.getPartitionName(resourceName, 3);
    String p4 = HelixUtils.getPartitionName(resourceName, 4);
    String p5 = HelixUtils.getPartitionName(resourceName, 5);
    String p6 = HelixUtils.getPartitionName(resourceName, 6);
    keyPartitionMap.put(key1, p1);
    keyPartitionMap.put(key2, p2);
    keyPartitionMap.put(key3, p3);
    keyPartitionMap.put(key4, p4);
    keyPartitionMap.put(key5, p5);
    keyPartitionMap.put(key6, p6);
    PartitionFinder partitionFinder = getPartitionFinder(keyPartitionMap);

    Instance instance1 = new Instance("host1_123", "host1", 123);
    Instance instance2 = new Instance("host2_123", "host2", 123);
    Instance instance3 = new Instance("host3_123", "host3", 123);
    Instance instance4 = new Instance("host4_123", "host4", 123);
    List<Instance> instanceListForP1 = new ArrayList<>();
    instanceListForP1.add(instance1);
    instanceListForP1.add(instance3);
    List<Instance> instanceListForP2 = new ArrayList<>();
    instanceListForP2.add(instance1);
    instanceListForP2.add(instance3);
    List<Instance> instanceListForP3 = new ArrayList<>();
    instanceListForP3.add(instance1);
    instanceListForP3.add(instance3);
    List<Instance> instanceListForP4 = new ArrayList<>();
    instanceListForP4.add(instance2);
    instanceListForP4.add(instance4);
    List<Instance> instanceListForP5 = new ArrayList<>();
    instanceListForP5.add(instance2);
    instanceListForP5.add(instance4);
    List<Instance> instanceListForP6 = new ArrayList<>();
    instanceListForP6.add(instance2);
    instanceListForP6.add(instance4);
    Map<String, List<Instance>> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put(p1, instanceListForP1);
    partitionInstanceMap.put(p2, instanceListForP2);
    partitionInstanceMap.put(p3, instanceListForP3);
    partitionInstanceMap.put(p4, instanceListForP4);
    partitionInstanceMap.put(p5, instanceListForP5);
    partitionInstanceMap.put(p6, instanceListForP6);

    HostFinder<Instance, VeniceRole> hostFinder = getHostFinder(partitionInstanceMap);
    HostHealthMonitor monitor = getHostHealthMonitor();
    ReadRequestThrottler throttler = getReadRequestThrottle(false);
    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(HELIX_ASSISTED_ROUTING).when(config).getMultiKeyRoutingStrategy();

    VeniceDelegateMode scatterMode =
        new VeniceDelegateMode(config, mock(RouterStats.class), mock(RouteHttpRequestStats.class));
    scatterMode.initReadRequestThrottler(throttler);

    HelixInstanceConfigRepository helixInstanceConfigRepository = mock(HelixInstanceConfigRepository.class);
    // Two groups
    doReturn(2).when(helixInstanceConfigRepository).getGroupCount();
    doReturn(0).when(helixInstanceConfigRepository).getInstanceGroupId(instance1.getNodeId());
    doReturn(0).when(helixInstanceConfigRepository).getInstanceGroupId(instance2.getNodeId());
    doReturn(1).when(helixInstanceConfigRepository).getInstanceGroupId(instance3.getNodeId());
    doReturn(1).when(helixInstanceConfigRepository).getInstanceGroupId(instance4.getNodeId());

    HelixGroupSelector helixGroupSelector = new HelixGroupSelector(
        new VeniceMetricsRepository(),
        helixInstanceConfigRepository,
        HelixGroupSelectionStrategyEnum.ROUND_ROBIN,
        mock(TimeoutProcessor.class));
    scatterMode.initHelixGroupSelector(helixGroupSelector);

    Scatter<Instance, VenicePath, RouterKey> finalScatter = scatterMode
        .scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA);

    Collection<ScatterGatherRequest<Instance, RouterKey>> requests = finalScatter.getOnlineRequests();
    Assert.assertEquals(requests.size(), 2);

    // each request should only have one 'Instance'
    requests.stream()
        .forEach(
            request -> Assert
                .assertEquals(request.getHosts().size(), 1, "There should be only one host for each request"));
    Set<Instance> instanceSet = new HashSet<>();
    requests.stream().forEach(request -> instanceSet.add(request.getHosts().get(0)));
    Assert.assertTrue(instanceSet.contains(instance1) && instanceSet.contains(instance2));

    // The second request should pick up another group
    scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    finalScatter = scatterMode
        .scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA);

    requests = finalScatter.getOnlineRequests();
    Assert.assertEquals(requests.size(), 2);

    // each request should only have one 'Instance'
    requests.stream()
        .forEach(
            request -> Assert
                .assertEquals(request.getHosts().size(), 1, "There should be only one host for each request"));
    instanceSet.clear();
    requests.stream().forEach(request -> instanceSet.add(request.getHosts().get(0)));
    Assert.assertTrue(instanceSet.contains(instance1) && instanceSet.contains(instance2));

    // Test the scenario that all the replicas for a given partition are slow
    // for partition 1, both instance1 and instance3 are slow
    Set<String> slowStorageNodeSet = new HashSet<>();
    slowStorageNodeSet.add(instance1.getNodeId());
    slowStorageNodeSet.add(instance3.getNodeId());
    VenicePath pathForAllSlowReplicas =
        getVenicePath(storeName, version, resourceName, RequestType.MULTI_GET_STREAMING, keys, slowStorageNodeSet);
    scatter = new Scatter(pathForAllSlowReplicas, getPathParser(), VeniceRole.REPLICA);
    finalScatter = scatterMode
        .scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA);

    requests = finalScatter.getOnlineRequests();
    Assert.assertEquals(requests.size(), 1);
  }
}
