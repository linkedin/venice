package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.utils.concurrent.VeniceReentrantReadWriteLock;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
    ZkClient zkClient = ZkClientFactory.newZkClient(zk.getAddress());
    ZKHelixManager controller = new ZKHelixManager(cluster, "localhost_1234", InstanceType.CONTROLLER, zk.getAddress());
    ZKHelixAdmin admin = new ZKHelixAdmin(zk.getAddress());
    admin.addCluster(cluster);
    return new VeniceHelixResources(cluster, zkClient, new HelixAdapterSerializer(),
        new SafeHelixManager(controller), mock(VeniceControllerClusterConfig.class),
        mock(StoreCleaner.class), metricsRepository, lock, Optional.empty(), Optional.empty(), Optional.empty());
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
    Assert.assertEquals(test[0], 2 , "Shutdown process should already acquire the lock and modify tne value.");
  }

  @Test
  public void testLockStats() throws InterruptedException {
    MetricsRepository mr = new MetricsRepository();
    String c1 = "cluster-1";
    String c2 = "cluster-2";
    String t = "total";
    VeniceReentrantReadWriteLock badLock = mock(VeniceReentrantReadWriteLock.class);
    ReentrantReadWriteLock workingLock = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.WriteLock badWriteLock = mock(ReentrantReadWriteLock.WriteLock.class);
    when(badWriteLock.tryLock(anyLong(), any(TimeUnit.class))).thenReturn(false).thenReturn(true);
    doReturn(workingLock.readLock()).when(badLock).readLock();
    doReturn(badWriteLock).when(badLock).writeLock();
    VeniceHelixResources resources1 = getVeniceHelixResources(c1, mr, new VeniceReentrantReadWriteLock());
    VeniceHelixResources resources2 = getVeniceHelixResources(c2, mr, badLock);
    String writeLockPrefixC1 = VeniceHelixResources.class.getSimpleName() + "-" + c1 + "-writeLock";
    String readLockPrefixC1 = VeniceHelixResources.class.getSimpleName() + "-" + c1 + "-readLock";
    String writeLockPrefixC2 = VeniceHelixResources.class.getSimpleName() + "-" + c2 + "-writeLock";
    String readLockPrefixC2 = VeniceHelixResources.class.getSimpleName() + "-" + c2 + "-readLock";

    double c1FailedWriteAc = 0, c1FailedReadAc = 0,
        c1SuccessfulWriteAc = 0, c1SuccessfulReadAc = 0,
        c2FailedWriteAc = 0, c2FailedReadAc = 0,
        c2SuccessfulWriteAc = 0, c2SuccessfulReadAc = 0,
        tFailedReadWriteAc = 0,
        tSuccessfulReadWriteAc = 0;

    assertAllMetrics(mr, writeLockPrefixC1, readLockPrefixC1, c1FailedWriteAc, c1FailedReadAc, c1SuccessfulWriteAc, c1SuccessfulReadAc);
    assertAllMetrics(mr, writeLockPrefixC2, readLockPrefixC2, c2FailedWriteAc, c2FailedReadAc, c2SuccessfulWriteAc, c2SuccessfulReadAc);
    assertTotalMetrics(mr, t, tFailedReadWriteAc, tSuccessfulReadWriteAc);
    resources1.lockForMetadataOperation();
    assertAllMetrics(mr, writeLockPrefixC1, readLockPrefixC1, c1FailedWriteAc, c1FailedReadAc, c1SuccessfulWriteAc, ++c1SuccessfulReadAc);
    assertAllMetrics(mr, writeLockPrefixC2, readLockPrefixC2, c2FailedWriteAc, c2FailedReadAc, c2SuccessfulWriteAc, c2SuccessfulReadAc);
    assertTotalMetrics(mr, t, tFailedReadWriteAc, ++tSuccessfulReadWriteAc);

    doReturn(workingLock.readLock()).when(badLock).readLock();
    resources2.lockForMetadataOperation();
    assertAllMetrics(mr, writeLockPrefixC1, readLockPrefixC1, c1FailedWriteAc, c1FailedReadAc, c1SuccessfulWriteAc, c1SuccessfulReadAc);
    assertAllMetrics(mr, writeLockPrefixC2, readLockPrefixC2, c2FailedWriteAc, c2FailedReadAc, c2SuccessfulWriteAc, ++c2SuccessfulReadAc);
    assertTotalMetrics(mr, t, tFailedReadWriteAc, ++tSuccessfulReadWriteAc);

    resources1.unlockForMetadataOperation();
    resources2.unlockForMetadataOperation();
    resources1.lockForShutdown();
    resources1.unlockForShutdown();

    assertAllMetrics(mr, writeLockPrefixC1, readLockPrefixC1, c1FailedWriteAc, c1FailedReadAc, ++c1SuccessfulWriteAc, c1SuccessfulReadAc);
    assertAllMetrics(mr, writeLockPrefixC2, readLockPrefixC2, c2FailedWriteAc, c2FailedReadAc, c2SuccessfulWriteAc, c2SuccessfulReadAc);
    assertTotalMetrics(mr, t, tFailedReadWriteAc, ++tSuccessfulReadWriteAc);

    resources2.lockForShutdown();
    assertAllMetrics(mr, writeLockPrefixC1, readLockPrefixC1, c1FailedWriteAc, c1FailedReadAc, c1SuccessfulWriteAc, c1SuccessfulReadAc);
    assertAllMetrics(mr, writeLockPrefixC2, readLockPrefixC2, ++c2FailedWriteAc, c2FailedReadAc, ++c2SuccessfulWriteAc, c2SuccessfulReadAc);
    assertTotalMetrics(mr, t, ++tFailedReadWriteAc, ++tSuccessfulReadWriteAc);

    resources2.unlockForShutdown();
  }

  private void assertAllMetrics(
      MetricsRepository metricsRepository,
      String writeLockPrefix,
      String readLockPrefix,
      double failedWriteAcquisition,
      double failedReadAcquisition,
      double successfulWriteAcquisition,
      double successfulReadAcquisition) {
    assertMetric(metricsRepository, "." + writeLockPrefix + "--failed_lock_acquisition.Count", failedWriteAcquisition);
    assertMetric(metricsRepository, "." + readLockPrefix + "--failed_lock_acquisition.Count", failedReadAcquisition);
    assertMetric(metricsRepository, "." + writeLockPrefix + "--successful_lock_acquisition.Count", successfulWriteAcquisition);
    assertMetric(metricsRepository, "." + readLockPrefix + "--successful_lock_acquisition.Count", successfulReadAcquisition);
  }

  private void assertTotalMetrics(
      MetricsRepository metricsRepository,
      String totalPrefix,
      double totalFailedAcquisition,
      double totalSuccessfulAcquisition) {
    assertMetric(metricsRepository, "." + totalPrefix + "--failed_lock_acquisition.Count", totalFailedAcquisition);
    assertMetric(metricsRepository, "." + totalPrefix + "--successful_lock_acquisition.Count", totalSuccessfulAcquisition);
  }

  private void assertMetric(MetricsRepository metricsRepository, String metricName, double expectedValue) {
    Metric metric = metricsRepository.getMetric(metricName);
    Assert.assertNotNull(metric, "Absent metric: " + metricName);
    Assert.assertEquals(metric.value(), expectedValue, "Wrong value for metric: " + metricName);
  }
}
