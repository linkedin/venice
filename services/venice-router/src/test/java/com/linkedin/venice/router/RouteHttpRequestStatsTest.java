package com.linkedin.venice.router;

import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RoutingComputationMode;
import com.linkedin.venice.router.api.VeniceDelegateMode;
import com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


public class RouteHttpRequestStatsTest {
  private MockTehutiReporter reporter;
  private RouteHttpRequestStats stats;
  private RouterHttpRequestStats routerHttpRequestStats;
  private InFlightRequestStat inFlightRequestStat;
  private MetricsRepository metricsRepository;
  private VeniceDelegateMode delegateMode;
  private Method selectLeastLoadedHostMethod;

  @BeforeSuite
  public void setUp() {
    metricsRepository = MetricsRepositoryUtils.createSingleThreadedVeniceMetricsRepository();
    reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    VeniceRouterConfig mockConfig = mock(VeniceRouterConfig.class);
    doReturn(1).when(mockConfig).getRouterInFlightMetricWindowSeconds();
    inFlightRequestStat = new InFlightRequestStat(mockConfig);
    Sensor totalInflightRequestSensor = inFlightRequestStat.getTotalInflightRequestSensor();
    stats = new RouteHttpRequestStats(metricsRepository, mock(StorageNodeClient.class));
    routerHttpRequestStats = new RouterHttpRequestStats(
        metricsRepository,
        "test-store",
        "test-cluster",
        RequestType.SINGLE_GET,
        mock(ScatterGatherStats.class),
        false,
        totalInflightRequestSensor);
  }

  @BeforeMethod
  public void setUpDelegateMode() throws Exception {
    // Create mock dependencies for VeniceDelegateMode
    VeniceRouterConfig mockConfig = mock(VeniceRouterConfig.class);
    doReturn(VeniceMultiKeyRoutingStrategy.LEAST_LOADED_ROUTING).when(mockConfig).getMultiKeyRoutingStrategy();
    doReturn(RoutingComputationMode.SEQUENTIAL).when(mockConfig).getRoutingComputationMode();
    doReturn(100).when(mockConfig).getParallelRoutingChunkSize();

    RouterStats<AggRouterHttpRequestStats> mockRouterStats = mock(RouterStats.class);
    AggRouterHttpRequestStats mockAggStats = mock(AggRouterHttpRequestStats.class);
    doReturn(mockAggStats).when(mockRouterStats).getStatsByType(any());

    delegateMode = new VeniceDelegateMode(mockConfig, mockRouterStats, stats);

    // Get the private method using reflection
    selectLeastLoadedHostMethod =
        VeniceDelegateMode.class.getDeclaredMethod("selectLeastLoadedHost", List.class, VenicePath.class);
    selectLeastLoadedHostMethod.setAccessible(true);
  }

  @Test
  public void routerMetricsTest() {
    stats.recordPendingRequest("my_host1");
    stats.recordPendingRequest("my_host2");

    Assert.assertEquals(stats.getPendingRequestCount("my_host1"), 1);

    stats.recordPendingRequest("my_host1");
    Assert.assertEquals(stats.getPendingRequestCount("my_host1"), 2);

    Assert.assertEquals(metricsRepository.metrics().get(".my_host1--pending_request_count.Max").value(), 2.0);
    Assert.assertEquals(metricsRepository.metrics().get(".my_host2--pending_request_count.Max").value(), 1.0);

    stats.recordFinishedRequest("my_host1");
    stats.recordFinishedRequest("my_host2");

    Assert.assertEquals(stats.getPendingRequestCount("my_host1"), 1);
    Assert.assertEquals(stats.getPendingRequestCount("my_host2"), 0);
  }

  @Test
  public void routerInFlightMetricTest() {
    routerHttpRequestStats.recordIncomingRequest();
    Assert.assertTrue(inFlightRequestStat.getInFlightRequestRate() > 0.0);
    // After waiting for metric time window, it wil be reset back to 0
    waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
      Assert.assertEquals(inFlightRequestStat.getInFlightRequestRate(), 0.0);
    });
  }

  @Test
  public void testRecordHostConsidered() {
    String hostName = "my_host1";
    stats.recordHostConsidered(hostName);
    String metricName = "." + hostName + "--host_considered_for_least_loaded_routing.OccurrenceRate";
    Assert.assertNotNull(metricsRepository.metrics().get(metricName), "Host considered metric should exist");
    Assert.assertTrue(
        metricsRepository.metrics().get(metricName).value() > 0,
        "Host considered rate should be greater than 0");
  }

  @Test
  public void testRecordHostSelected() {
    String hostName = "my_host1";
    stats.recordHostSelected(hostName);
    String metricName = "." + hostName + "--host_selected_by_least_loaded_routing.OccurrenceRate";
    Assert.assertNotNull(metricsRepository.metrics().get(metricName), "Host selected metric should exist");
    Assert.assertTrue(
        metricsRepository.metrics().get(metricName).value() > 0,
        "Host selected rate should be greater than 0");
  }

  @Test
  public void testSelectLeastLoadedHostWithBusyHosts() throws Exception {
    // three hosts with different load levels
    Instance busyHost = createMockInstance("busy_host");
    Instance moderateHost = createMockInstance("moderate_host");
    Instance idleHost = createMockInstance("idle_host");

    // Simulate pending requests
    for (int i = 0; i < 10; i++) {
      stats.recordPendingRequest(busyHost.getNodeId());
    }
    for (int i = 0; i < 5; i++) {
      stats.recordPendingRequest(moderateHost.getNodeId());
    }
    stats.recordPendingRequest(idleHost.getNodeId());

    List<Instance> hosts = new ArrayList<>();
    hosts.add(busyHost);
    hosts.add(moderateHost);
    hosts.add(idleHost);

    // Mock VenicePath
    VenicePath mockPath = mock(VenicePath.class);
    doReturn(true).when(mockPath).canRequestStorageNode(anyString());
    doReturn("test-store").when(mockPath).getStoreName();
    doReturn(false).when(mockPath).isRetryRequest();

    Instance selectedHost = (Instance) selectLeastLoadedHostMethod.invoke(delegateMode, hosts, mockPath);

    // Then - verify idle host was selected
    Assert
        .assertEquals(selectedHost.getNodeId(), idleHost.getNodeId(), "Idle host should be selected by actual method");

    // Verify metrics were recorded by the actual method
    String busyConsideredMetric =
        "." + busyHost.getNodeId() + "--host_considered_for_least_loaded_routing.OccurrenceRate";
    String moderateConsideredMetric =
        "." + moderateHost.getNodeId() + "--host_considered_for_least_loaded_routing.OccurrenceRate";
    String idleConsideredMetric =
        "." + idleHost.getNodeId() + "--host_considered_for_least_loaded_routing.OccurrenceRate";

    Assert.assertTrue(
        metricsRepository.metrics().get(busyConsideredMetric).value() > 0,
        "Busy host should be considered");
    Assert.assertTrue(
        metricsRepository.metrics().get(moderateConsideredMetric).value() > 0,
        "Moderate host should be considered");
    Assert.assertTrue(
        metricsRepository.metrics().get(idleConsideredMetric).value() > 0,
        "Idle host should be considered");

    // Verify only idle host was selected
    String idleSelectedMetric = "." + idleHost.getNodeId() + "--host_selected_by_least_loaded_routing.OccurrenceRate";
    Assert.assertTrue(metricsRepository.metrics().get(idleSelectedMetric).value() > 0, "Idle host should be selected");
    String busySelectedMetric = "." + busyHost.getNodeId() + "--host_selected_by_least_loaded_routing.OccurrenceRate";
    String moderateSelectedMetric =
        "." + moderateHost.getNodeId() + "--host_selected_by_least_loaded_routing.OccurrenceRate";
    Assert.assertEquals(
        metricsRepository.metrics().get(busySelectedMetric).value(),
        0.0,
        "Busy host should not be selected");
    Assert.assertEquals(
        metricsRepository.metrics().get(moderateSelectedMetric).value(),
        0.0,
        "Moderate host should not be selected");
  }

  @Test
  public void testActualSelectLeastLoadedHostWithConsistentlyBusyHost() throws Exception {
    // three hosts where one is always overloaded
    Instance overloadedHost = createMockInstance("overloaded_host");
    Instance normalHost1 = createMockInstance("normal_host_1");
    Instance normalHost2 = createMockInstance("normal_host_2");

    // Make overloaded host have high pending requests
    for (int i = 0; i < 100; i++) {
      stats.recordPendingRequest(overloadedHost.getNodeId());
    }

    List<Instance> hosts = new ArrayList<>();
    hosts.add(overloadedHost);
    hosts.add(normalHost1);
    hosts.add(normalHost2);

    VenicePath mockPath = mock(VenicePath.class);
    doReturn(true).when(mockPath).canRequestStorageNode(anyString());
    doReturn("test-store").when(mockPath).getStoreName();
    doReturn(false).when(mockPath).isRetryRequest();

    // simulate 20 requests
    for (int i = 0; i < 20; i++) {
      List<Instance> hostsCopy = new ArrayList<>(hosts);
      Instance selected = (Instance) selectLeastLoadedHostMethod.invoke(delegateMode, hostsCopy, mockPath);

      // The selected host shouldn't be the overloaded one
      Assert.assertNotEquals(
          selected.getNodeId(),
          overloadedHost.getNodeId(),
          "Overloaded host should never be selected");

      // Simulate some load on normal hosts
      stats.recordPendingRequest(selected.getNodeId());
    }

    // overloaded host should have high consideration but zero selection
    String overloadedConsideredMetric =
        "." + overloadedHost.getNodeId() + "--host_considered_for_least_loaded_routing.OccurrenceRate";
    String overloadedSelectedMetric =
        "." + overloadedHost.getNodeId() + "--host_selected_by_least_loaded_routing.OccurrenceRate";

    Assert.assertTrue(
        metricsRepository.metrics().get(overloadedConsideredMetric).value() > 0,
        "Overloaded host should be considered many times");
    Assert.assertEquals(
        metricsRepository.metrics().get(overloadedSelectedMetric).value(),
        0.0,
        "Overloaded host should never be selected");

    // Normal hosts should have both consideration and selection
    for (Instance host: new Instance[] { normalHost1, normalHost2 }) {
      String consideredMetric = "." + host.getNodeId() + "--host_considered_for_least_loaded_routing.OccurrenceRate";
      String selectedMetric = "." + host.getNodeId() + "--host_selected_by_least_loaded_routing.OccurrenceRate";

      Assert.assertTrue(metricsRepository.metrics().get(consideredMetric).value() > 0);
      Assert.assertTrue(metricsRepository.metrics().get(selectedMetric).value() > 0);
    }
  }

  // Helper method to create mock Instance
  private Instance createMockInstance(String nodeId) {
    Instance instance = mock(Instance.class);
    doReturn(nodeId).when(instance).getNodeId();
    doReturn("host").when(instance).getHost();
    doReturn(1234).when(instance).getPort();
    return instance;
  }
}
