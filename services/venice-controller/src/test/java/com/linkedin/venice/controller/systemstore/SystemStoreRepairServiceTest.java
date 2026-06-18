package com.linkedin.venice.controller.systemstore;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SystemStoreRepairServiceTest {
  /**
   * Public test-only SystemStoreHealthChecker with the documented {@code (VeniceControllerMultiClusterConfig)}
   * constructor so it can be loaded reflectively by {@link SystemStoreRepairService#loadHealthChecker()}.
   */
  public static class TestOverrideChecker implements SystemStoreHealthChecker {
    private final VeniceControllerMultiClusterConfig config;

    public TestOverrideChecker(VeniceControllerMultiClusterConfig config) {
      this.config = config;
    }

    public VeniceControllerMultiClusterConfig getConfig() {
      return config;
    }

    @Override
    public Map<String, HealthCheckResult> checkHealth(String clusterName, Set<String> systemStoreNames) {
      return Collections.emptyMap();
    }
  }

  /**
   * Override whose constructor throws — distinct from a class-not-found failure, but it hits the same broad catch
   * in {@link SystemStoreRepairService#loadHealthChecker()} and must also fall back to the heartbeat checker.
   */
  public static class ThrowingConstructorChecker implements SystemStoreHealthChecker {
    public ThrowingConstructorChecker(VeniceControllerMultiClusterConfig config) {
      throw new RuntimeException("constructor boom");
    }

    @Override
    public Map<String, HealthCheckResult> checkHealth(String clusterName, Set<String> systemStoreNames) {
      return Collections.emptyMap();
    }
  }

  /**
   * Tracks whether {@code close()} ran via a static flag, because the checker is created reflectively inside
   * {@link SystemStoreRepairService#loadHealthChecker()} and the test has no other reference to the instance.
   */
  private static final AtomicBoolean CLOSE_TRACKING_CHECKER_CLOSED = new AtomicBoolean(false);

  public static class CloseTrackingChecker implements SystemStoreHealthChecker {
    public CloseTrackingChecker(VeniceControllerMultiClusterConfig config) {
    }

    @Override
    public Map<String, HealthCheckResult> checkHealth(String clusterName, Set<String> systemStoreNames) {
      return Collections.emptyMap();
    }

    @Override
    public void close() {
      CLOSE_TRACKING_CHECKER_CLOSED.set(true);
    }
  }

  @Test
  public void testInitialization() {
    VeniceControllerClusterConfig commonConfig = mock(VeniceControllerClusterConfig.class);
    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(commonConfig).when(multiClusterConfigs).getCommonConfig();
    doReturn("").when(commonConfig).getSystemStoreHealthCheckOverrideClassName();
    Set<String> clusterSet = new HashSet<>();
    clusterSet.add("venice-1");
    clusterSet.add("venice-2");
    doReturn(clusterSet).when(multiClusterConfigs).getClusters();

    VeniceControllerClusterConfig v1Config = mock(VeniceControllerClusterConfig.class);
    VeniceControllerClusterConfig v2Config = mock(VeniceControllerClusterConfig.class);
    doReturn(true).when(v2Config).isParentSystemStoreRepairServiceEnabled();
    doReturn(v1Config).when(multiClusterConfigs).getControllerConfig("venice-1");
    doReturn(v2Config).when(multiClusterConfigs).getControllerConfig("venice-2");

    SystemStoreRepairService systemStoreRepairService =
        new SystemStoreRepairService(mock(VeniceParentHelixAdmin.class), multiClusterConfigs, new MetricsRepository());
    Assert.assertFalse(systemStoreRepairService.getClusterToSystemStoreHealthCheckStatsMap().containsKey("venice-1"));
    Assert.assertTrue(systemStoreRepairService.getClusterToSystemStoreHealthCheckStatsMap().containsKey("venice-2"));
  }

  @Test
  public void testStartCreatesHeartbeatChecker() {
    VeniceControllerClusterConfig commonConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(600).when(commonConfig).getParentSystemStoreHeartbeatCheckWaitTimeSeconds();
    doReturn(1800).when(commonConfig).getParentSystemStoreRepairCheckIntervalSeconds();
    doReturn(30).when(commonConfig).getParentSystemStoreVersionRefreshThresholdInDays();
    doReturn(50).when(commonConfig).getSystemStoreRepairMaxPerRound();
    doReturn("").when(commonConfig).getSystemStoreHealthCheckOverrideClassName();

    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(commonConfig).when(multiClusterConfigs).getCommonConfig();
    doReturn(new HashSet<>()).when(multiClusterConfigs).getClusters();

    VeniceParentHelixAdmin parentAdmin = mock(VeniceParentHelixAdmin.class);
    SystemStoreRepairService service =
        new SystemStoreRepairService(parentAdmin, multiClusterConfigs, new MetricsRepository());
    service.startInner();

    Assert.assertNotNull(service.getHealthChecker());
    Assert.assertTrue(service.getHealthChecker() instanceof HeartbeatBasedSystemStoreHealthChecker);

    service.stopInner();
  }

  @Test
  public void testStartWithValidOverrideClassName() {
    VeniceControllerClusterConfig commonConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(600).when(commonConfig).getParentSystemStoreHeartbeatCheckWaitTimeSeconds();
    doReturn(1800).when(commonConfig).getParentSystemStoreRepairCheckIntervalSeconds();
    doReturn(30).when(commonConfig).getParentSystemStoreVersionRefreshThresholdInDays();
    doReturn(50).when(commonConfig).getSystemStoreRepairMaxPerRound();
    doReturn(TestOverrideChecker.class.getName()).when(commonConfig).getSystemStoreHealthCheckOverrideClassName();

    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(commonConfig).when(multiClusterConfigs).getCommonConfig();
    doReturn(new HashSet<>()).when(multiClusterConfigs).getClusters();

    VeniceParentHelixAdmin parentAdmin = mock(VeniceParentHelixAdmin.class);
    SystemStoreRepairService service =
        new SystemStoreRepairService(parentAdmin, multiClusterConfigs, new MetricsRepository());
    service.startInner();

    // The reflective override path must successfully instantiate the configured class via the documented
    // (VeniceControllerMultiClusterConfig) constructor and use it instead of the default heartbeat checker.
    SystemStoreHealthChecker loaded = service.getHealthChecker();
    Assert.assertNotNull(loaded);
    Assert.assertTrue(
        loaded instanceof TestOverrideChecker,
        "expected TestOverrideChecker to be loaded, got " + loaded.getClass().getName());
    Assert.assertSame(((TestOverrideChecker) loaded).getConfig(), multiClusterConfigs);

    service.stopInner();
  }

  @Test
  public void testStartWithInvalidOverrideClassName() {
    VeniceControllerClusterConfig commonConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(600).when(commonConfig).getParentSystemStoreHeartbeatCheckWaitTimeSeconds();
    doReturn(1800).when(commonConfig).getParentSystemStoreRepairCheckIntervalSeconds();
    doReturn(30).when(commonConfig).getParentSystemStoreVersionRefreshThresholdInDays();
    doReturn(50).when(commonConfig).getSystemStoreRepairMaxPerRound();
    doReturn("com.nonexistent.FakeChecker").when(commonConfig).getSystemStoreHealthCheckOverrideClassName();

    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(commonConfig).when(multiClusterConfigs).getCommonConfig();
    doReturn(new HashSet<>()).when(multiClusterConfigs).getClusters();

    VeniceParentHelixAdmin parentAdmin = mock(VeniceParentHelixAdmin.class);
    SystemStoreRepairService service =
        new SystemStoreRepairService(parentAdmin, multiClusterConfigs, new MetricsRepository());
    service.startInner();

    // Should fall back to heartbeat checker when override class cannot be loaded
    Assert.assertNotNull(service.getHealthChecker());
    Assert.assertTrue(service.getHealthChecker() instanceof HeartbeatBasedSystemStoreHealthChecker);

    service.stopInner();
  }

  @Test
  public void testStartWithOverrideConstructorThrowing() {
    VeniceControllerClusterConfig commonConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(600).when(commonConfig).getParentSystemStoreHeartbeatCheckWaitTimeSeconds();
    doReturn(1800).when(commonConfig).getParentSystemStoreRepairCheckIntervalSeconds();
    doReturn(30).when(commonConfig).getParentSystemStoreVersionRefreshThresholdInDays();
    doReturn(50).when(commonConfig).getSystemStoreRepairMaxPerRound();
    doReturn(ThrowingConstructorChecker.class.getName()).when(commonConfig)
        .getSystemStoreHealthCheckOverrideClassName();

    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(commonConfig).when(multiClusterConfigs).getCommonConfig();
    doReturn(new HashSet<>()).when(multiClusterConfigs).getClusters();

    VeniceParentHelixAdmin parentAdmin = mock(VeniceParentHelixAdmin.class);
    SystemStoreRepairService service =
        new SystemStoreRepairService(parentAdmin, multiClusterConfigs, new MetricsRepository());
    service.startInner();

    // An override that loads but whose constructor throws must fall back to the heartbeat checker, just like a
    // class-not-found failure, rather than crashing controller startup.
    Assert.assertNotNull(service.getHealthChecker());
    Assert.assertTrue(service.getHealthChecker() instanceof HeartbeatBasedSystemStoreHealthChecker);

    service.stopInner();
  }

  @Test
  public void testStopInnerClosesChecker() {
    CLOSE_TRACKING_CHECKER_CLOSED.set(false);

    VeniceControllerClusterConfig commonConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(600).when(commonConfig).getParentSystemStoreHeartbeatCheckWaitTimeSeconds();
    doReturn(1800).when(commonConfig).getParentSystemStoreRepairCheckIntervalSeconds();
    doReturn(30).when(commonConfig).getParentSystemStoreVersionRefreshThresholdInDays();
    doReturn(50).when(commonConfig).getSystemStoreRepairMaxPerRound();
    doReturn(CloseTrackingChecker.class.getName()).when(commonConfig).getSystemStoreHealthCheckOverrideClassName();

    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(commonConfig).when(multiClusterConfigs).getCommonConfig();
    doReturn(new HashSet<>()).when(multiClusterConfigs).getClusters();

    VeniceParentHelixAdmin parentAdmin = mock(VeniceParentHelixAdmin.class);
    SystemStoreRepairService service =
        new SystemStoreRepairService(parentAdmin, multiClusterConfigs, new MetricsRepository());
    service.startInner();
    Assert.assertTrue(service.getHealthChecker() instanceof CloseTrackingChecker);

    service.stopInner();

    // stopInner() must close() the loaded checker so a custom checker can release its resources; a regression that
    // skips close() would leak silently.
    Assert.assertTrue(CLOSE_TRACKING_CHECKER_CLOSED.get(), "stopInner() should close the loaded health checker");
  }
}
