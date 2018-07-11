package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.StoreCleaner;
import io.tehuti.metrics.MetricsRepository;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceHelixResources {
  @Test
  public void testShutdownLock()
      throws Exception {
    ZkServerWrapper zk = ServiceFactory.getZkServer();
    ZkClient zkClient = new ZkClient(zk.getAddress());
    String cluster = "test";
    ZKHelixManager controller = new ZKHelixManager(cluster, "localhost_1234", InstanceType.CONTROLLER, zk.getAddress());
    ZKHelixAdmin admin = new ZKHelixAdmin(zk.getAddress());
    admin.addCluster(cluster);
    final VeniceHelixResources rs =
        new VeniceHelixResources("test", zkClient, new HelixAdapterSerializer(),
           new SafeHelixManager(controller), Mockito.mock(VeniceControllerClusterConfig.class),
            Mockito.mock(StoreCleaner.class), new MetricsRepository());
    int[] test = new int[]{0};
    rs.lockForMetadataOperation();
    test[0] = 1;
    new Thread(() -> {
      rs.lockForShutdown();
      test[0] = 2;
      rs.unlockForShutdown();
    }).start();

    Thread.sleep(500);
    Assert.assertEquals(test[0], 1 , "The lock is acquired by metadata operation, could not be updated by shutdown process.");
    rs.unlockForMetadataOperation();
    Thread.sleep(500);
    Assert.assertEquals(test[0], 2 , "Shutdown process should already aqcuire the lock and modify tne value.");
  }


}
