package com.linkedin.venice.controller;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestVeniceHelixResources {
  private ZkServerWrapper zkServer;

  @BeforeClass
  public void setUp() {
    zkServer = ServiceFactory.getZkServer();
  }

  @AfterClass
  public void cleanUp() {
    zkServer.close();
  }

  private HelixVeniceClusterResources getVeniceHelixResources(String cluster) {
    return getVeniceHelixResources(cluster, mock(VeniceControllerClusterConfig.class));
  }

  private HelixVeniceClusterResources getVeniceHelixResources(String cluster, VeniceControllerClusterConfig config) {
    ZkClient zkClient = ZkClientFactory.newZkClient(zkServer.getAddress());
    ZKHelixManager controller =
        new ZKHelixManager(cluster, "localhost_1234", InstanceType.CONTROLLER, zkServer.getAddress());
    ZKHelixAdmin admin = new ZKHelixAdmin(zkServer.getAddress());
    admin.addCluster(cluster);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    doReturn(mock(MetaStoreWriter.class)).when(veniceHelixAdmin).getMetaStoreWriter();
    doReturn(mock(HelixReadOnlyZKSharedSystemStoreRepository.class)).when(veniceHelixAdmin)
        .getReadOnlyZKSharedSystemStoreRepository();
    doReturn(mock(HelixReadOnlyZKSharedSchemaRepository.class)).when(veniceHelixAdmin)
        .getReadOnlyZKSharedSchemaRepository();

    doReturn(Collections.emptyList()).when(veniceHelixAdmin).getAllStores(cluster);

    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    when(controllerConfig.getDaVinciPushStatusScanThreadNumber()).thenReturn(4);
    when(controllerConfig.getDaVinciPushStatusScanIntervalInSeconds()).thenReturn(5);
    when(controllerConfig.isDaVinciPushStatusEnabled()).thenReturn(true);
    when(controllerConfig.getOffLineJobWaitTimeInMilliseconds()).thenReturn(120000L);
    when(controllerConfig.isDeadStoreEndpointEnabled()).thenReturn(true);
    when(controllerConfig.isPreFetchDeadStoreStatsEnabled()).thenReturn(true);
    when(controllerConfig.getDeadStoreStatsPreFetchRefreshIntervalInMs()).thenReturn(100L); // Must be Long

    // The spectator HelixManager is now constructed from config.getZkAddress() (previously zkClient.getServers()), so
    // the injected config must expose the test ZK address. Stubbing it here covers both the no-arg overload and
    // caller-supplied configs.
    when(config.getZkAddress()).thenReturn(zkServer.getAddress());

    return new HelixVeniceClusterResources(
        cluster,
        zkClient,
        new HelixAdapterSerializer(),
        new SafeHelixManager(controller),
        config,
        veniceHelixAdmin,
        new MetricsRepository(),
        mock(RealTimeTopicSwitcher.class),
        Optional.empty(),
        mock(HelixAdminClient.class),
        mock(VeniceVersionLifecycleEventManager.class));
  }

  /**
   * The ZK address {@link HelixVeniceClusterResources} selects for the spectator Helix manager, captured by
   * {@link CapturingHelixVeniceClusterResources} during construction (no real connection is opened).
   */
  private static final ThreadLocal<String> CAPTURED_SPECTATOR_ZK_ADDRESS = new ThreadLocal<>();

  /**
   * Subclass that intercepts {@link HelixVeniceClusterResources#getSpectatorManager(String, String)} to record the ZK
   * address chosen for the spectator manager, returning a stubbed manager instead of opening a real ZK connection.
   */
  private static class CapturingHelixVeniceClusterResources extends HelixVeniceClusterResources {
    CapturingHelixVeniceClusterResources(
        String clusterName,
        ZkClient zkClient,
        HelixAdapterSerializer adapterSerializer,
        SafeHelixManager helixManager,
        VeniceControllerClusterConfig config,
        VeniceHelixAdmin admin,
        MetricsRepository metricsRepository,
        RealTimeTopicSwitcher realTimeTopicSwitcher,
        Optional<DynamicAccessController> accessController,
        HelixAdminClient helixAdminClient,
        VeniceVersionLifecycleEventManager veniceVersionLifecycleEventManager) {
      super(
          clusterName,
          zkClient,
          adapterSerializer,
          helixManager,
          config,
          admin,
          metricsRepository,
          realTimeTopicSwitcher,
          accessController,
          helixAdminClient,
          veniceVersionLifecycleEventManager);
    }

    @Override
    SafeHelixManager getSpectatorManager(String clusterName, String zkAddress) {
      // Invoked from the super constructor; record the selected address without opening a real ZK connection.
      CAPTURED_SPECTATOR_ZK_ADDRESS.set(zkAddress);
      SafeHelixManager manager = mock(SafeHelixManager.class);
      // HelixExternalViewRepository's constructor reads the cluster name off the manager.
      when(manager.getClusterName()).thenReturn(clusterName);
      return manager;
    }
  }

  /**
   * Construct a {@link CapturingHelixVeniceClusterResources} and return the ZK address it selected for the spectator
   * Helix manager. {@code metadataZkAddress} backs the Venice-metadata {@link ZkClient} (must be a live test ZK);
   * {@code configHelixZkAddress} is what {@link VeniceControllerClusterConfig#getZkAddress()} reports.
   */
  private String captureSpectatorZkAddress(String cluster, String metadataZkAddress, String configHelixZkAddress) {
    ZkClient zkClient = ZkClientFactory.newZkClient(metadataZkAddress);
    ZKHelixManager controller =
        new ZKHelixManager(cluster, "localhost_1234", InstanceType.CONTROLLER, metadataZkAddress);
    ZKHelixAdmin admin = new ZKHelixAdmin(metadataZkAddress);
    admin.addCluster(cluster);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    doReturn(mock(MetaStoreWriter.class)).when(veniceHelixAdmin).getMetaStoreWriter();
    doReturn(mock(HelixReadOnlyZKSharedSystemStoreRepository.class)).when(veniceHelixAdmin)
        .getReadOnlyZKSharedSystemStoreRepository();
    doReturn(mock(HelixReadOnlyZKSharedSchemaRepository.class)).when(veniceHelixAdmin)
        .getReadOnlyZKSharedSchemaRepository();
    doReturn(Collections.emptyList()).when(veniceHelixAdmin).getAllStores(cluster);

    VeniceControllerClusterConfig config = mock(VeniceControllerClusterConfig.class);
    when(config.getZkAddress()).thenReturn(configHelixZkAddress);

    CAPTURED_SPECTATOR_ZK_ADDRESS.remove();
    new CapturingHelixVeniceClusterResources(
        cluster,
        zkClient,
        new HelixAdapterSerializer(),
        new SafeHelixManager(controller),
        config,
        veniceHelixAdmin,
        new MetricsRepository(),
        mock(RealTimeTopicSwitcher.class),
        Optional.empty(),
        mock(HelixAdminClient.class),
        mock(VeniceVersionLifecycleEventManager.class));
    String captured = CAPTURED_SPECTATOR_ZK_ADDRESS.get();
    CAPTURED_SPECTATOR_ZK_ADDRESS.remove();
    return captured;
  }

  @DataProvider(name = "spectatorZkAddressCases")
  public Object[][] spectatorZkAddressCases() {
    // name, configHelixZkAddress (null -> reuse the live metadata ZK address), expectMatchesMetadataAddress
    return new Object[][] { { "config-only-specified", "helix-zk-standalone.example.com:2181", false },
        { "both-specified-and-identical", null, true },
        { "both-specified-and-different", "helix-zk-different.example.com:2181", false } };
  }

  /**
   * Guards the ZK-client re-architecture: the spectator Helix manager must bind to the Helix ZK address from
   * {@link VeniceControllerClusterConfig#getZkAddress()}, independent of the Venice-metadata {@link ZkClient}. When the
   * metadata client is later repointed to a backup ensemble for HA, Helix-coordination reads must stay on the Helix ZK.
   * Covers the address-source combinations: config-only, both-identical (today's prod), and both-different (post-HA).
   */
  @Test(dataProvider = "spectatorZkAddressCases")
  public void testSpectatorManagerBindsToConfigZkAddress(
      String name,
      String configHelixZkAddress,
      boolean expectMatchesMetadataAddress) {
    String metadataZkAddress = zkServer.getAddress();
    String configZkAddress = configHelixZkAddress == null ? metadataZkAddress : configHelixZkAddress;

    String captured = captureSpectatorZkAddress("test-spectator-" + name, metadataZkAddress, configZkAddress);

    Assert.assertEquals(
        captured,
        configZkAddress,
        "[" + name + "] spectator manager must bind to the config Helix ZK address.");
    if (!expectMatchesMetadataAddress) {
      Assert.assertNotEquals(
          captured,
          metadataZkAddress,
          "[" + name + "] spectator manager must not fall back to the Venice-metadata zkClient address.");
    }
  }

  @Test
  public void testShutdownLock() throws Exception {
    final HelixVeniceClusterResources rs = getVeniceHelixResources("test");
    int[] value = new int[] { 0 };

    CountDownLatch thread2StartedLatch = new CountDownLatch(1);
    Thread thread2 = new Thread(() -> {
      thread2StartedLatch.countDown();
      try (AutoCloseableLock ignore2 = rs.lockForShutdown()) {
        value[0] = 2;
      }
    });

    try (AutoCloseableLock ignore1 = rs.getClusterLockManager().createStoreWriteLock("store")) {
      value[0] = 1;
      thread2.start();
      Assert.assertTrue(thread2StartedLatch.await(1, TimeUnit.SECONDS));
      Thread.sleep(500);
      Assert.assertEquals(
          value[0],
          1,
          "The lock is acquired by metadata operation, could not be updated by shutdown process.");
    } finally {
      thread2.join();
    }

    Assert.assertEquals(value[0], 2, "Shutdown process should already acquire the lock and modify the value.");
  }

  @Test
  public void testStartAndStopDeadStoreStatsPreFetchTask() throws Exception {
    HelixVeniceClusterResources resources = getVeniceHelixResources("test-dead-store");
    resources.startDeadStoreStatsPreFetchTask();
    Thread.sleep(300); // Let it run a few times
    resources.stopDeadStoreStatsPreFetchTask();
    Assert.assertTrue(true, "Dead store stats task started and stopped cleanly");
  }

  @Test
  public void testErrorPartitionResetTaskNotInitializedInParentController() {
    // Even when the auto reset limit is positive, the error partition reset task must not be initialized in the
    // parent controller.
    VeniceControllerClusterConfig parentConfig = mock(VeniceControllerClusterConfig.class);
    when(parentConfig.isParent()).thenReturn(true);
    when(parentConfig.getErrorPartitionAutoResetLimit()).thenReturn(1);
    HelixVeniceClusterResources parentResources = getVeniceHelixResources("test-parent-error-partition", parentConfig);
    Assert.assertNull(
        parentResources.getErrorPartitionResetTask(),
        "Error partition reset task should not be initialized in the parent controller.");
  }

  @Test
  public void testErrorPartitionResetTaskInitializedInChildControllerWhenLimitIsPositive() {
    VeniceControllerClusterConfig childConfig = mock(VeniceControllerClusterConfig.class);
    when(childConfig.isParent()).thenReturn(false);
    when(childConfig.getErrorPartitionAutoResetLimit()).thenReturn(1);
    HelixVeniceClusterResources childResources = getVeniceHelixResources("test-child-error-partition", childConfig);
    Assert.assertNotNull(
        childResources.getErrorPartitionResetTask(),
        "Error partition reset task should be initialized in the child controller when the limit is positive.");
  }

  @Test
  public void testErrorPartitionResetTaskNotInitializedInChildControllerWhenLimitIsZero() {
    VeniceControllerClusterConfig childConfig = mock(VeniceControllerClusterConfig.class);
    when(childConfig.isParent()).thenReturn(false);
    when(childConfig.getErrorPartitionAutoResetLimit()).thenReturn(0);
    HelixVeniceClusterResources childResources =
        getVeniceHelixResources("test-child-error-partition-disabled", childConfig);
    Assert.assertNull(
        childResources.getErrorPartitionResetTask(),
        "Error partition reset task should not be initialized when the limit is zero.");
  }

  @Test
  public void testIsSourceCluster() {
    String storeName = Utils.getUniqueString("test-is-source-store");
    String sourceCluster = "source-cluster";
    String nonSourceCluster = "non-source-cluster";
    HelixVeniceClusterResources resources = getVeniceHelixResources(sourceCluster);
    StoreConfig storeConfig = new StoreConfig(storeName);
    storeConfig.setCluster(sourceCluster);
    ZkStoreConfigAccessor mockStoreConfigAccessor = mock(ZkStoreConfigAccessor.class);
    doReturn(storeConfig).when(mockStoreConfigAccessor).getStoreConfig(storeName);
    resources.setStoreConfigAccessor(mockStoreConfigAccessor);
    Assert.assertTrue(resources.isSourceCluster(sourceCluster, storeName));
    Assert.assertFalse(resources.isSourceCluster(nonSourceCluster, storeName));
    Assert.assertThrows(IllegalArgumentException.class, () -> resources.isSourceCluster(null, storeName));
    Assert
        .assertThrows(VeniceNoStoreException.class, () -> resources.isSourceCluster(sourceCluster, "non-exist-store"));
  }
}
