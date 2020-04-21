package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.misc.Metrics;
import com.linkedin.ddsstorage.router.api.HostFinder;
import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.ddsstorage.router.api.PartitionFinder;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpUriRequest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestVeniceDelegateMode {

  private VenicePath getVenicePath(String resourceName, RequestType requestType, List<RouterKey> keys) {
    return new VenicePath(resourceName, false, -1) {
      private final String ROUTER_REQUEST_VERSION = Integer.toString(
          ReadAvroProtocolDefinition.SINGLE_GET_ROUTER_REQUEST_V1.getProtocolVersion());
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
      public ByteBuf getRequestBody() {
        return Unpooled.EMPTY_BUFFER;
      }

      public String getVeniceApiVersionHeader() {
        return ROUTER_REQUEST_VERSION;
      }

      @Nonnull
      @Override
      public String getLocation() {
        return null;
      }

      public Collection<RouterKey> getPartitionKeys() {
        return keys;
      }
    };
  }

  private PartitionFinder<RouterKey> getPartitionFinder(Map<RouterKey, String> keyPartitionMap) {
    return new PartitionFinder<RouterKey>() {
      @Nonnull
      @Override
      public String findPartitionName(@Nonnull String resourceName, @Nonnull RouterKey partitionKey)
          throws RouterException {
        if (keyPartitionMap.containsKey(partitionKey)) {
          return keyPartitionMap.get(partitionKey);
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
        return getAllPartitionNames(resourceName).size();
      }
    };
  }

  private HostFinder<Instance, VeniceRole> getHostFinder(Map<String, List<Instance>> partitionHostMap, boolean sticky) {
    return new HostFinder<Instance, VeniceRole>() {
      @Nonnull
      @Override
      public List<Instance> findHosts(@Nonnull String requestMethod, @Nonnull String resourceName,
          @Nonnull String partitionName, @Nonnull HostHealthMonitor<Instance> hostHealthMonitor,
          @Nonnull VeniceRole roles) throws RouterException {
        if (partitionHostMap.containsKey(partitionName)) {
          if (sticky) {
            List<Instance> hosts = new ArrayList<>();
            hosts.add(partitionHostMap.get(partitionName).get(0));
            return hosts;
          }
          return partitionHostMap.get(partitionName);
        }
        return Collections.EMPTY_LIST;
      }

      @Nonnull
      @Override
      public Collection<Instance> findAllHosts(VeniceRole roles) throws RouterException {
        Set<Instance> instanceSet = new HashSet<>();
        partitionHostMap.values().stream().forEach( value -> instanceSet.addAll(value));
        return new ArrayList<>(instanceSet);
      }
    };
  }

  private HostHealthMonitor<Instance> getHostHealthMonitor() {
    return (hostName, partitionName) -> true;
  }

  private ReadRequestThrottler getReadRequestThrottle(boolean throttle) {
    ReadRequestThrottler throttler = mock(ReadRequestThrottler.class);
    doReturn(1).when(throttler).getReadCapacity();
    if (throttle) {
      doThrow(new QuotaExceededException("test", "10", "5")).when(throttler).mayThrottleRead(any(), anyInt(), any());
    }

    return throttler;
  }

  private VenicePathParser getPathParser() {
    return mock(VenicePathParser.class);
  }

  @BeforeClass
  public void setup() {
    RouterExceptionAndTrackingUtils.setRouterStats(new RouterStats<>( requestType -> new AggRouterHttpRequestStats(new MetricsRepository(), requestType)));
  }

  @AfterClass
  public void tearDown() {
    RouterExceptionAndTrackingUtils.setRouterStats(null);
  }

  @Test
  public void testScatterWithSingleGet() throws RouterException {
    String storeName = TestUtils.getUniqueString("test_store");
    String resourceName = storeName + "_v1";
    RouterKey key = new RouterKey("key_1".getBytes());
    List<RouterKey> keys = new ArrayList<>();
    keys.add(key);
    VenicePath path = getVenicePath(resourceName, RequestType.SINGLE_GET, keys);
    Scatter<Instance, VenicePath, RouterKey> scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    String requestMethod = HttpMethod.GET.name();
    Map<RouterKey, String> keyPartitionMap = new HashMap<>();
    String partitionName = "p1";
    keyPartitionMap.put(key, partitionName);
    PartitionFinder partitionFinder = getPartitionFinder(keyPartitionMap);
    Instance instance1 = new Instance("host1_123", "host1", 123);
    Instance instance2 = new Instance("host2_123", "host2", 123);
    Instance instance3 = new Instance("host3_123", "host3", 123);
    List<Instance> instanceList = new ArrayList<>();
    instanceList.add(instance1);
    instanceList.add(instance2);
    instanceList.add(instance3);
    Map<String, List<Instance>> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put(partitionName, instanceList);
    HostFinder<Instance, VeniceRole> hostFinder = getHostFinder(partitionInstanceMap, false);
    HostHealthMonitor monitor = getHostHealthMonitor();
    ReadRequestThrottler throttler = getReadRequestThrottle(false);

    VeniceDelegateMode scatterMode = new VeniceDelegateMode(
        new VeniceDelegateModeConfig()
        .withStickyRoutingEnabledForMultiGet(false)
        .withStickyRoutingEnabledForSingleGet(false),
        mock(RouterStats.class)
    );
    scatterMode.initReadRequestThrottler(throttler);

    Scatter<Instance, VenicePath, RouterKey> finalScatter = scatterMode.scatter(scatter, requestMethod, resourceName,
        partitionFinder, hostFinder, monitor, VeniceRole.REPLICA, new Metrics());

    // Throttling for single-get request is not happening in VeniceDelegateMode
    verify(throttler, never()).mayThrottleRead(eq(storeName), eq(1), any());
    Collection<ScatterGatherRequest<Instance, RouterKey>> requests = finalScatter.getOnlineRequests();
    Assert.assertEquals(requests.size(), 1, "There should be only one online request since there is only one key");
    ScatterGatherRequest<Instance, RouterKey> request = requests.iterator().next();
    List<Instance> hosts = request.getHosts();
    Assert.assertEquals(hosts.size(), 1, "There should be only one chose host");
    Instance selectedHost = hosts.get(0);
    Assert.assertTrue(instanceList.contains(selectedHost));

    // Test with sticky routing
    hostFinder = getHostFinder(partitionInstanceMap, true);
    scatterMode = new VeniceDelegateMode(
        new VeniceDelegateModeConfig()
        .withStickyRoutingEnabledForSingleGet(true)
        .withStickyRoutingEnabledForMultiGet(true),
        mock(RouterStats.class)
    );
    scatterMode.initReadRequestThrottler(throttler);
    scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    finalScatter = scatterMode.scatter(scatter, requestMethod, resourceName,
        partitionFinder, hostFinder, monitor, VeniceRole.REPLICA, new Metrics());
    requests = finalScatter.getOnlineRequests();
    Assert.assertEquals(requests.size(), 1, "There should be only one online request since there is only one key");
    request = requests.iterator().next();
    hosts = request.getHosts();
    Assert.assertEquals(hosts.size(), 1, "There should be only one chose host");
    selectedHost = hosts.get(0);
    Assert.assertEquals(instanceList.get(0), selectedHost, "Sticky routing should select: " + instanceList.get(0));
  }

  @Test (expectedExceptions = RouterException.class, expectedExceptionsMessageRegExp = ".*not available for store.*")
  public void testScatterWithSingleGetWithNotAvailablePartition() throws RouterException {
    String storeName = TestUtils.getUniqueString("test_store");
    String resourceName = storeName + "_v1";
    RouterKey key = new RouterKey("key_1".getBytes());
    List<RouterKey> keys = new ArrayList<>();
    keys.add(key);
    VenicePath path = getVenicePath(resourceName, RequestType.SINGLE_GET, keys);
    Scatter<Instance, VenicePath, RouterKey> scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    String requestMethod = HttpMethod.GET.name();
    Map<RouterKey, String> keyPartitionMap = new HashMap<>();
    String partitionName = "p1";
    keyPartitionMap.put(key, partitionName);
    PartitionFinder partitionFinder = getPartitionFinder(keyPartitionMap);

    Map<String, List<Instance>> partitionInstanceMap = new HashMap<>();
    HostFinder<Instance, VeniceRole> hostFinder = getHostFinder(partitionInstanceMap, false);
    HostHealthMonitor monitor = getHostHealthMonitor();
    ReadRequestThrottler throttler = getReadRequestThrottle(false);

    VeniceDelegateMode scatterMode = new VeniceDelegateMode(
        new VeniceDelegateModeConfig()
        .withStickyRoutingEnabledForSingleGet(false)
        .withStickyRoutingEnabledForMultiGet(false),
        mock(RouterStats.class)
    );
    scatterMode.initReadRequestThrottler(throttler);

    scatterMode.scatter(scatter, requestMethod, resourceName,
        partitionFinder, hostFinder, monitor, VeniceRole.REPLICA, new Metrics());
  }

  /**
   * Once the scatter algo for multi-get gets changed in the future, the expectation in the following test should be
   * changed accordingly.
   *
   * @throws RouterException
   */
  @Test
  public void testScatterWithMultiGet() throws RouterException {
    String storeName = TestUtils.getUniqueString("test_store");
    String resourceName = storeName + "_v1";
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
    VenicePath path = getVenicePath(resourceName, RequestType.MULTI_GET, keys);
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
    Instance instance5 = new Instance("host5_123", "host5", 123);
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

    HostFinder<Instance, VeniceRole> hostFinder = getHostFinder(partitionInstanceMap, false);
    HostHealthMonitor monitor = getHostHealthMonitor();
    ReadRequestThrottler throttler = getReadRequestThrottle(false);

    VeniceDelegateMode scatterMode = new VeniceDelegateMode(
        new VeniceDelegateModeConfig()
        .withStickyRoutingEnabledForSingleGet(false)
        .withStickyRoutingEnabledForMultiGet(false),
        mock(RouterStats.class)
    );
    scatterMode.initReadRequestThrottler(throttler);

    Scatter<Instance, VenicePath, RouterKey> finalScatter =
        scatterMode.scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA, new Metrics());

    Collection<ScatterGatherRequest<Instance, RouterKey>> requests = finalScatter.getOnlineRequests();
    Assert.assertEquals(requests.size(), 3);

    // Verify throttling
    verify(throttler).mayThrottleRead(storeName, 4, Optional.of(instance1.getNodeId()));
    verify(throttler, times(2)).mayThrottleRead(eq(storeName), eq(1.0d), any());

    // each request should only have one 'Instance'
    requests.stream().forEach(request -> Assert.assertEquals(request.getHosts().size(), 1,
        "There should be only one host for each request"));
    Set<Instance> instanceSet = new HashSet<>();
    requests.stream().forEach(request -> instanceSet.add(request.getHosts().get(0)));
    Assert.assertTrue(instanceSet.contains(instance1), "instance1 must be selected");
    Assert.assertTrue(instanceSet.contains(instance2) || instanceSet.contains(instance4),
        "One of instance2/instance4 should be selected");
    Assert.assertTrue(instanceSet.contains(instance3) || instanceSet.contains(instance5),
        "One of instance3/instance5 should be selected");

    // test sticky routing
    scatter = new Scatter(path, getPathParser(), VeniceRole.REPLICA);
    hostFinder = getHostFinder(partitionInstanceMap, true);
    scatterMode = new VeniceDelegateMode(
        new VeniceDelegateModeConfig()
        .withStickyRoutingEnabledForSingleGet(true)
        .withStickyRoutingEnabledForMultiGet(true),
        mock(RouterStats.class)
    );
    scatterMode.initReadRequestThrottler(throttler);
    finalScatter =
        scatterMode.scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA, new Metrics());
    requests = finalScatter.getOnlineRequests();
    Assert.assertEquals(requests.size(), 3);
    requests.stream().forEach(request -> {
      Assert.assertEquals(request.getHosts().size(), 1,
          "There should be only one host for each request");
      Instance host = request.getHosts().get(0);
      SortedSet partitionKeys = request.getPartitionKeys();
      Set<String> partitionNames = request.getPartitionsNames();
      if (host.equals(instance1)) {
        Assert.assertEquals(partitionKeys.size(), 4);
        Assert.assertTrue(partitionKeys.contains(key1));
        Assert.assertTrue(partitionKeys.contains(key2));
        Assert.assertTrue(partitionKeys.contains(key3));
        Assert.assertTrue(partitionKeys.contains(key4));
        Assert.assertEquals(partitionNames.size(), 4);
        Assert.assertTrue(partitionNames.contains(p1));
        Assert.assertTrue(partitionNames.contains(p2));
        Assert.assertTrue(partitionNames.contains(p3));
        Assert.assertTrue(partitionNames.contains(p4));
      } else if (host.equals(instance2)) {
        Assert.assertEquals(partitionKeys.size(), 1);
        Assert.assertTrue(partitionKeys.contains(key5));
        Assert.assertEquals(partitionNames.size(), 1);
        Assert.assertTrue(partitionNames.contains(p5));;
      } else if (host.equals(instance5)) {
        Assert.assertEquals(partitionKeys.size(), 1);
        Assert.assertTrue(partitionKeys.contains(key6));
        Assert.assertEquals(partitionNames.size(), 1);
        Assert.assertTrue(partitionNames.contains(p6));;
      } else {
        Assert.fail("Instance: " + host + " shouldn't be selected");
      }
    });
  }

  @Test
  public void testScatterWitStreamingMultiGet() throws RouterException {
    String storeName = TestUtils.getUniqueString("test_store");
    String resourceName = storeName + "_v1";
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
    VenicePath path = getVenicePath(resourceName, RequestType.MULTI_GET_STREAMING, keys);
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
    PartitionFinder partitionFinder = getPartitionFinder(keyPartitionMap);

    Instance instance1 = new Instance("host1_123", "host1", 123);
    Instance instance2 = new Instance("host2_123", "host2", 123);
    Instance instance3 = new Instance("host3_123", "host3", 123);
    Instance instance4 = new Instance("host4_123", "host4", 123);
    Instance instance5 = new Instance("host5_123", "host5", 123);
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

    HostFinder<Instance, VeniceRole> hostFinder = getHostFinder(partitionInstanceMap, false);
    HostHealthMonitor monitor = getHostHealthMonitor();
    ReadRequestThrottler throttler = getReadRequestThrottle(false);

    VeniceDelegateMode scatterMode = new VeniceDelegateMode(
        new VeniceDelegateModeConfig()
            .withStickyRoutingEnabledForSingleGet(false)
            .withStickyRoutingEnabledForMultiGet(true),
        mock(RouterStats.class)
    );
    scatterMode.initReadRequestThrottler(throttler);

    Scatter<Instance, VenicePath, RouterKey> finalScatter =
        scatterMode.scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, monitor, VeniceRole.REPLICA, new Metrics());

    Collection<ScatterGatherRequest<Instance, RouterKey>> requests = finalScatter.getOfflineRequests();
    Assert.assertEquals(requests.size(), 1);
  }

  @Test
  public void testRequestNotChoosingSlowHost() throws RouterException {
    byte[] keyBytes = {'a', 'b', 'c'};
    RouterKey key = new RouterKey(keyBytes);
    List<Instance> hosts = new ArrayList<>(3);
    hosts.add(Instance.fromNodeId("host1_8435"));
    hosts.add(Instance.fromNodeId("host2_8435"));
    hosts.add(Instance.fromNodeId("host3_8435"));

    Instance firstPickHost = VeniceDelegateMode.chooseHostByKey(key, hosts);
    VenicePath path = mock(VenicePath.class);

    // Test VeniceDelegateMode should choose the first pick host when no host is slow
    doReturn(true).when(path).canRequestStorageNode(any());

    Instance chosenHost = VeniceDelegateMode.avoidSlowHost(path, key, hosts);
    Assert.assertEquals(chosenHost, firstPickHost);


    // Test VeniceDelegateMode should choose the fast host when the first pick host and some other hosts are slow
    Instance fastHost = null;
    int fastHostNumber = hosts.size() - 1;
    // first pick host can not be the fast host
    doReturn(false).when(path).canRequestStorageNode(firstPickHost.getNodeId());
    for (Instance host: hosts) {
      if (host.equals(firstPickHost)) {
        continue;
      }
      if (fastHostNumber > 1) {
        doReturn(false).when(path).canRequestStorageNode(host.getNodeId());
        --fastHostNumber;
      } else {
        fastHost = host;
      }
    }

    chosenHost = VeniceDelegateMode.avoidSlowHost(path, key, hosts);
    Assert.assertEquals(chosenHost, fastHost);
  }
}
