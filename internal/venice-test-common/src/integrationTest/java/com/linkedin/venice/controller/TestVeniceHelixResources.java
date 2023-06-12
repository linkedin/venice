package com.linkedin.venice.controller;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
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
    return getVeniceHelixResources(cluster, new MetricsRepository());
  }

  private HelixVeniceClusterResources getVeniceHelixResources(String cluster, MetricsRepository metricsRepository) {
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
    VeniceControllerConfig controllerConfig = mock(VeniceControllerConfig.class);
    when(controllerConfig.getDaVinciPushStatusScanThreadNumber()).thenReturn(4);
    when(controllerConfig.getDaVinciPushStatusScanIntervalInSeconds()).thenReturn(5);
    when(controllerConfig.isDaVinciPushStatusEnabled()).thenReturn(true);
    when(controllerConfig.getOffLineJobWaitTimeInMilliseconds()).thenReturn(120000L);
    return new HelixVeniceClusterResources(
        cluster,
        zkClient,
        new HelixAdapterSerializer(),
        new SafeHelixManager(controller),
        mock(VeniceControllerConfig.class),
        veniceHelixAdmin,
        metricsRepository,
        mock(RealTimeTopicSwitcher.class),
        Optional.empty(),
        mock(HelixAdminClient.class));
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
    Assert.assertEquals(value[0], 2, "Shutdown process should already acquire the lock and modify tne value.");
  }
}
