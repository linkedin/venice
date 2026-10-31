package com.linkedin.venice.stats;

import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ServerReadQuotaUsageStatsTest {
  @DataProvider(name = "concurrentRateOperations")
  public Object[][] concurrentRateOperations() {
    return new Object[][] { { "recordAllowed", "recordRejected" }, { "recordAllowed", "kps" },
        { "recordAllowed", "qps" }, { "recordAllowed", "ratio" }, { "kps", "recordAllowed" }, { "kps", "qps" } };
  }

  @Test(dataProvider = "concurrentRateOperations", timeOut = 15 * Time.MS_PER_SECOND)
  public void testRateAccessSerializedAtWindowBoundary(String firstOperation, String secondOperation) throws Exception {
    CountDownLatch firstAccess = new CountDownLatch(1);
    CountDownLatch releaseFirstAccess = new CountDownLatch(1);
    AtomicReference<Thread> firstThread = new AtomicReference<>();
    AtomicReference<Thread> secondThread = new AtomicReference<>();
    TestMockTime time = new TestMockTime(0) {
      @Override
      public long getMilliseconds() {
        // Pause inside a rate operation, after it has acquired its per-version lock.
        if (Thread.currentThread() == firstThread.get()) {
          firstAccess.countDown();
          try {
            Assert.assertTrue(releaseFirstAccess.await(10, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
          }
        }
        return super.getMilliseconds();
      }
    };
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    ExecutorService service = Executors.newFixedThreadPool(2, new DaemonThreadFactory("quota-rate-window-test"));
    try {
      ServerReadQuotaUsageStats stats = new ServerReadQuotaUsageStats(metricsRepository, "test-store", time, null);
      stats.updateVersionInfo(1, 0);
      stats.setNodeQuotaResponsibility(1, 1000);
      stats.recordAllowed(1, 30);
      time.setTime(30_001);

      Future<?> first = service.submit(() -> {
        firstThread.set(Thread.currentThread());
        accessQuotaRate(stats, firstOperation);
      });
      Assert.assertTrue(firstAccess.await(5, TimeUnit.SECONDS));
      Future<?> second = service.submit(() -> {
        secondThread.set(Thread.currentThread());
        accessQuotaRate(stats, secondOperation);
      });
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Assert.assertNotNull(secondThread.get());
        Assert.assertTrue(second.isDone() || secondThread.get().getState() == Thread.State.BLOCKED);
      });
      Assert.assertFalse(second.isDone(), "Rate recording and measurement must not access the windows concurrently");

      releaseFirstAccess.countDown();
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);

      int recordings =
          1 + (firstOperation.startsWith("record") ? 1 : 0) + (secondOperation.startsWith("record") ? 1 : 0);
      Assert.assertEquals(stats.getVersionedRequestedKPS(1), recordings * 30d / 30.001, 0.000001);
      Assert.assertEquals(stats.getVersionedRequestedQPS(1), recordings / 30.001, 0.000001);
      Assert.assertEquals(stats.getReadQuotaUsageRatio(), recordings * 30d / 30.001 / 1000, 0.000001);

      time.setTime(120_001);
      Assert.assertEquals(stats.getVersionedRequestedKPS(1), 0d);
      Assert.assertEquals(stats.getVersionedRequestedQPS(1), 0d);
      Assert.assertEquals(stats.getReadQuotaUsageRatio(), 0d);
      stats.recordAllowed(1, 30);
      Assert.assertEquals(stats.getVersionedRequestedKPS(1), 1d);
    } finally {
      releaseFirstAccess.countDown();
      service.shutdownNow();
      try {
        Assert.assertTrue(service.awaitTermination(5, TimeUnit.SECONDS));
      } finally {
        metricsRepository.close();
      }
    }
  }

  private static void accessQuotaRate(ServerReadQuotaUsageStats stats, String operation) {
    switch (operation) {
      case "recordAllowed":
        stats.recordAllowed(1, 30);
        break;
      case "recordRejected":
        stats.recordRejected(1, 30);
        break;
      case "kps":
        stats.getVersionedRequestedKPS(1);
        break;
      case "qps":
        stats.getVersionedRequestedQPS(1);
        break;
      case "ratio":
        stats.getReadQuotaUsageRatio();
        break;
      default:
        throw new IllegalArgumentException(operation);
    }
  }

  @Test
  public void testGetReadQuotaUsageRatio() {
    // createSingleThreadedMetricsRepository builds a dedicated AsyncGaugeExecutor — close in finally
    // to release it. Tehuti MetricsRepository is not AutoCloseable.
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    try {
      String storeName = "test-store";
      ServerReadQuotaUsageStats stats =
          new ServerReadQuotaUsageStats(metricsRepository, storeName, new SystemTime(), null);
      stats.updateVersionInfo(1, 0);
      stats.setNodeQuotaResponsibility(1, 1000);
      stats.recordAllowed(1, 10000);
      double usageRatio = metricsRepository.getMetric(".test-store--quota_requested_usage_ratio.Gauge").value();
      Assert.assertEquals(usageRatio, (10000d / 30d) / 1000, 0.001);
      stats.setNodeQuotaResponsibility(2, 100);
      // Ensure quota usage don't just 2x when node responsibility changes on other versions
      Assert.assertTrue(
          metricsRepository.getMetric(".test-store--quota_requested_usage_ratio.Gauge").value() <= usageRatio);
    } finally {
      metricsRepository.close();
    }
  }

  @Test
  public void testGetReadQuotaMetricsWithNoVersionOrRecordings() {
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    try {
      String storeName = "test-store";
      int currentVersion = 3;
      int backupVersion = 2;
      ServerReadQuotaUsageStats stats =
          new ServerReadQuotaUsageStats(metricsRepository, storeName, new SystemTime(), null);
      // Stats shouldn't fail if the store don't have any versions yet
      Assert.assertEquals(stats.getVersionedRequestedQPS(backupVersion), 0d);
      Assert.assertEquals(stats.getVersionedRequestedQPS(currentVersion), 0d);
      Assert.assertEquals(stats.getVersionedRequestedKPS(backupVersion), 0d);
      Assert.assertEquals(stats.getVersionedRequestedKPS(currentVersion), 0d);
      Assert.assertEquals(stats.getReadQuotaUsageRatio(), Double.NaN);
      // Stats shouldn't fail if there are no recordings yet
      stats.updateVersionInfo(currentVersion, backupVersion);
      Assert.assertEquals(stats.getVersionedRequestedQPS(backupVersion), 0d);
      Assert.assertEquals(stats.getVersionedRequestedQPS(currentVersion), 0d);
      Assert.assertEquals(stats.getVersionedRequestedKPS(backupVersion), 0d);
      Assert.assertEquals(stats.getVersionedRequestedKPS(currentVersion), 0d);
      Assert.assertEquals(stats.getReadQuotaUsageRatio(), Double.NaN);
      // The replica receives some assignment and traffic for current version
      stats.setNodeQuotaResponsibility(currentVersion, 1000);
      stats.recordAllowed(currentVersion, 100);
      Assert.assertTrue(stats.getReadQuotaUsageRatio() > 0);
      Assert.assertTrue(stats.getVersionedRequestedQPS(currentVersion) > 0);
      Assert.assertTrue(stats.getVersionedRequestedKPS(currentVersion) > 0);
    } finally {
      metricsRepository.close();
    }
  }

  /**
   * A non-thread safe map like Int2ObjectOpenHashMap could cause the threads to busy loop inside the internal find
   * method when a race condition happens
   */
  @Test(timeOut = 10 * Time.MS_PER_SECOND)
  public void testVersionedStatsThreadSafe() throws ExecutionException, InterruptedException, TimeoutException {
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    ExecutorService service =
        Executors.newFixedThreadPool(100, new DaemonThreadFactory("ServerReadQuotaUsageStatsTest"));
    try {
      String storeName = "test-store";
      ServerReadQuotaUsageStats stats =
          new ServerReadQuotaUsageStats(metricsRepository, storeName, new SystemTime(), null);
      CompletableFuture[] completableFutures = new CompletableFuture[100];
      for (int j = 0; j < 100; j++) {
        completableFutures[j] = CompletableFuture.runAsync(() -> {
          for (int i = 0; i < 100000; i++) {
            stats.recordAllowed(i, 1);
          }
        }, service);
      }
      CompletableFuture.allOf(completableFutures).get(10, TimeUnit.SECONDS);
    } finally {
      service.shutdown();
      metricsRepository.close();
    }
  }
}
