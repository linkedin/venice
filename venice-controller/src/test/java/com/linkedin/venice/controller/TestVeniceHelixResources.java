package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;
import static org.mockito.Mockito.*;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceHelixResources {
  private static final Logger LOGGER = Logger.getLogger(TestVeniceHelixResources.class);

  private HelixVeniceClusterResources getVeniceHelixResources(String cluster) {
    return getVeniceHelixResources(cluster, new MetricsRepository());
  }

  private HelixVeniceClusterResources getVeniceHelixResources(String cluster, MetricsRepository metricsRepository) {
    ZkServerWrapper zk = ServiceFactory.getZkServer();
    ZkClient zkClient = ZkClientFactory.newZkClient(zk.getAddress());
    ZKHelixManager controller = new ZKHelixManager(cluster, "localhost_1234", InstanceType.CONTROLLER, zk.getAddress());
    ZKHelixAdmin admin = new ZKHelixAdmin(zk.getAddress());
    admin.addCluster(cluster);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    doReturn(mock(MetaStoreWriter.class)).when(veniceHelixAdmin).getMetaStoreWriter();
    doReturn(mock(HelixReadOnlyZKSharedSystemStoreRepository.class)).when(veniceHelixAdmin).getReadOnlyZKSharedSystemStoreRepository();
    doReturn(mock(HelixReadOnlyZKSharedSchemaRepository.class)).when(veniceHelixAdmin).getReadOnlyZKSharedSchemaRepository();
    return new HelixVeniceClusterResources(cluster, zkClient, new HelixAdapterSerializer(), new SafeHelixManager(controller),
        mock(VeniceControllerConfig.class), veniceHelixAdmin, metricsRepository, Optional.empty(),
        Optional.empty(), Optional.empty(), mock(MetadataStoreWriter.class), mock(HelixAdminClient.class));
  }

  @Test
  public void testShutdownLock()
      throws Exception {
    final HelixVeniceClusterResources rs = getVeniceHelixResources("test");
    int[] test = new int[]{0};
    try (AutoCloseableLock ignore1 = rs.getClusterLockManager().createStoreWriteLock("store")) {
      test[0] = 1;
      new Thread(() -> {
        try (AutoCloseableLock ignore2 = rs.lockForShutdown()) {
          test[0] = 2;
        }
      }).start();

      Thread.sleep(500);
      Assert.assertEquals(test[0], 1 , "The lock is acquired by metadata operation, could not be updated by shutdown process.");
    }
    Thread.sleep(500);
    Assert.assertEquals(test[0], 2 , "Shutdown process should already acquire the lock and modify tne value.");
  }
}
