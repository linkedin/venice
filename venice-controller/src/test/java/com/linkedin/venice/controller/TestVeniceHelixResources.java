package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.utils.concurrent.VeniceReentrantReadWriteLock;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import static org.mockito.Mockito.*;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceHelixResources {
  private static final Logger LOGGER = Logger.getLogger(TestVeniceHelixResources.class);

  private VeniceHelixResources getVeniceHelixResources(String cluster) {
    return getVeniceHelixResources(cluster, new MetricsRepository(), new VeniceReentrantReadWriteLock());
  }

  private VeniceHelixResources getVeniceHelixResources(String cluster, MetricsRepository metricsRepository, VeniceReentrantReadWriteLock lock) {
    ZkServerWrapper zk = ServiceFactory.getZkServer();
    ZkClient zkClient = new ZkClient(zk.getAddress());
    ZKHelixManager controller = new ZKHelixManager(cluster, "localhost_1234", InstanceType.CONTROLLER, zk.getAddress());
    ZKHelixAdmin admin = new ZKHelixAdmin(zk.getAddress());
    admin.addCluster(cluster);
    return new VeniceHelixResources(cluster, zkClient, new HelixAdapterSerializer(),
        new SafeHelixManager(controller), mock(VeniceControllerClusterConfig.class),
        mock(StoreCleaner.class), metricsRepository, lock);
  }

  @Test
  public void testShutdownLock()
      throws Exception {
    final VeniceHelixResources rs = getVeniceHelixResources("test");
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

  @Test
  public void testLockStats() {
    MetricsRepository mr = new MetricsRepository();
    String c1 = "cluster-1";
    String c2 = "cluster-2";
    String t = "total";
    VeniceReentrantReadWriteLock badLock = mock(VeniceReentrantReadWriteLock.class);
    VeniceHelixResources resources1 = getVeniceHelixResources(c1, mr, new VeniceReentrantReadWriteLock());
    VeniceHelixResources resources2 = getVeniceHelixResources(c2, mr, badLock);

    double c1FailedWriteAc = 0, c1FailedReadAc = 0, c1FailedWriteRe = 0,
        c1SuccessfulWriteAc = 0, c1SuccessfulReadAc = 0, c1SuccessfulWriteRe = 0,
        c2FailedWriteAc = 0, c2FailedReadAc = 0, c2FailedWriteRe = 0,
        c2SuccessfulWriteAc = 0, c2SuccessfulReadAc = 0, c2SuccessfulWriteRe = 0,
        tFailedWriteAc = 0, tFailedReadAc = 0, tFailedWriteRe = 0,
        tSuccessfulWriteAc = 0, tSuccessfulReadAc = 0, tSuccessfulWriteRe = 0;

    assertAllMetrics(mr, c1, c1FailedWriteAc, c1FailedReadAc, c1FailedWriteRe, c1SuccessfulWriteAc, c1SuccessfulReadAc, c1SuccessfulWriteRe);
    assertAllMetrics(mr, c2, c2FailedWriteAc, c2FailedReadAc, c2FailedWriteRe, c2SuccessfulWriteAc, c2SuccessfulReadAc, c2SuccessfulWriteRe);
    assertAllMetrics(mr, t,  tFailedWriteAc,  tFailedReadAc, tFailedWriteRe, tSuccessfulWriteAc, tSuccessfulReadAc, tSuccessfulWriteRe);

    resources1.lockForMetadataOperation();
    assertAllMetrics(mr, c1, c1FailedWriteAc, c1FailedReadAc, c1FailedWriteRe, c1SuccessfulWriteAc, ++c1SuccessfulReadAc, c1SuccessfulWriteRe);
    assertAllMetrics(mr, c2, c2FailedWriteAc, c2FailedReadAc, c2FailedWriteRe, c2SuccessfulWriteAc, c2SuccessfulReadAc, c2SuccessfulWriteRe);
    assertAllMetrics(mr, t,  tFailedWriteAc,  tFailedReadAc, tFailedWriteRe, tSuccessfulWriteAc, ++tSuccessfulReadAc, tSuccessfulWriteRe);

    ReentrantReadWriteLock workingLock = new ReentrantReadWriteLock();
    doReturn(workingLock.readLock()).when(badLock).readLock();
    resources2.lockForMetadataOperation();
    assertAllMetrics(mr, c1, c1FailedWriteAc, c1FailedReadAc, c1FailedWriteRe, c1SuccessfulWriteAc, c1SuccessfulReadAc, c1SuccessfulWriteRe);
    assertAllMetrics(mr, c2, c2FailedWriteAc, c2FailedReadAc, c2FailedWriteRe, c2SuccessfulWriteAc, ++c2SuccessfulReadAc, c2SuccessfulWriteRe);
    assertAllMetrics(mr, t,  tFailedWriteAc,  tFailedReadAc, tFailedWriteRe, tSuccessfulWriteAc, ++tSuccessfulReadAc, tSuccessfulWriteRe);

    resources1.unlockForMetadataOperation();
    resources2.unlockForMetadataOperation();
    resources1.lockForShutdown();

    assertAllMetrics(mr, c1, c1FailedWriteAc, c1FailedReadAc, c1FailedWriteRe, ++c1SuccessfulWriteAc, c1SuccessfulReadAc, c1SuccessfulWriteRe);
    assertAllMetrics(mr, c2, c2FailedWriteAc, c2FailedReadAc, c2FailedWriteRe, c2SuccessfulWriteAc, c2SuccessfulReadAc, c2SuccessfulWriteRe);
    assertAllMetrics(mr, t,  tFailedWriteAc,  tFailedReadAc, tFailedWriteRe, ++tSuccessfulWriteAc, tSuccessfulReadAc, tSuccessfulWriteRe);

    doThrow(new VeniceException()).when(badLock).writeLock();
    try {
      resources2.lockForShutdown();
      Assert.fail("Should have thrown! Mocking problem?");
    } catch (VeniceException e) {
      // Expected
    }

    assertAllMetrics(mr, c1, c1FailedWriteAc, c1FailedReadAc, c1FailedWriteRe, c1SuccessfulWriteAc, c1SuccessfulReadAc, c1SuccessfulWriteRe);
    assertAllMetrics(mr, c2, ++c2FailedWriteAc, c2FailedReadAc, c2FailedWriteRe, c2SuccessfulWriteAc, c2SuccessfulReadAc, c2SuccessfulWriteRe);
    assertAllMetrics(mr, t,  ++tFailedWriteAc,  tFailedReadAc, tFailedWriteRe, tSuccessfulWriteAc, tSuccessfulReadAc, tSuccessfulWriteRe);

    resources1.unlockForShutdown();

    assertAllMetrics(mr, c1, c1FailedWriteAc, c1FailedReadAc, c1FailedWriteRe, c1SuccessfulWriteAc, c1SuccessfulReadAc, ++c1SuccessfulWriteRe);
    assertAllMetrics(mr, c2, c2FailedWriteAc, c2FailedReadAc, c2FailedWriteRe, c2SuccessfulWriteAc, c2SuccessfulReadAc, c2SuccessfulWriteRe);
    assertAllMetrics(mr, t,  tFailedWriteAc,  tFailedReadAc, tFailedWriteRe, tSuccessfulWriteAc, tSuccessfulReadAc, ++tSuccessfulWriteRe);

    // Now the second resources will allow us to write lock, but not to unlock...
    doReturn(workingLock.writeLock()).when(badLock).writeLock();
    resources2.lockForShutdown();

    assertAllMetrics(mr, c1, c1FailedWriteAc, c1FailedReadAc, c1FailedWriteRe, c1SuccessfulWriteAc, c1SuccessfulReadAc, c1SuccessfulWriteRe);
    assertAllMetrics(mr, c2, c2FailedWriteAc, c2FailedReadAc, c2FailedWriteRe, ++c2SuccessfulWriteAc, c2SuccessfulReadAc, c2SuccessfulWriteRe);
    assertAllMetrics(mr, t,  tFailedWriteAc,  tFailedReadAc, tFailedWriteRe, ++tSuccessfulWriteAc, tSuccessfulReadAc, tSuccessfulWriteRe);

    doReturn(true).when(badLock).isWriteLockedByCurrentThread();
    resources2.unlockForShutdown();

    assertAllMetrics(mr, c1, c1FailedWriteAc, c1FailedReadAc, c1FailedWriteRe, c1SuccessfulWriteAc, c1SuccessfulReadAc, c1SuccessfulWriteRe);
    assertAllMetrics(mr, c2, c2FailedWriteAc, c2FailedReadAc, ++c2FailedWriteRe, c2SuccessfulWriteAc, c2SuccessfulReadAc, c2SuccessfulWriteRe);
    assertAllMetrics(mr, t,  tFailedWriteAc,  tFailedReadAc, ++tFailedWriteRe, tSuccessfulWriteAc, tSuccessfulReadAc, tSuccessfulWriteRe);
  }

  private void assertAllMetrics(
      MetricsRepository metricsRepository,
      String resource,
      double failedWriteAcquision,
      double failedReadAcquision,
      double failedWriteRelease,
      double successfulWriteAcquision,
      double successfulReadAcquision,
      double successfulWriteRelease) {
    assertMetric(metricsRepository, "." + resource + "--failed_write_lock_acquisition.Count", failedWriteAcquision);
    assertMetric(metricsRepository, "." + resource + "--failed_read_lock_acquisition.Count", failedReadAcquision);
    assertMetric(metricsRepository, "." + resource + "--failed_write_lock_release.Count", failedWriteRelease);
    assertMetric(metricsRepository, "." + resource + "--successful_write_lock_acquisition.Count", successfulWriteAcquision);
    assertMetric(metricsRepository, "." + resource + "--successful_read_lock_acquisition.Count", successfulReadAcquision);
    assertMetric(metricsRepository, "." + resource + "--successful_write_lock_release.Count", successfulWriteRelease);
  }

  private void assertMetric(MetricsRepository metricsRepository, String metricName, double expectedValue) {
    Metric metric = metricsRepository.getMetric(metricName);
    Assert.assertNotNull(metric, "Absent metric: " + metricName);
    Assert.assertEquals(metric.value(), expectedValue, "Wrong value for metric: " + metricName);
  }
}
