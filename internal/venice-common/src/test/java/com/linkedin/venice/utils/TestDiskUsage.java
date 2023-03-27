package com.linkedin.venice.utils;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDiskUsage {
  @Test
  public void disUsageReportsFullWhenFull() throws IOException {
    String path = "./";
    FileStore disk = Files.getFileStore(Paths.get(path));
    long free = disk.getUsableSpace();
    long total = disk.getTotalSpace();
    double currentDiskUsage = (total - free) / (double) (total);

    // set full threshold to half of current usage, disk must report full
    DiskUsage usage = new DiskUsage(path, currentDiskUsage / 2);
    Assert.assertTrue(usage.isDiskFull(1), "Disk must report full: " + usage.getDiskStatus());

    // set full threshold to 100%, disk must not report full
    DiskUsage notFullUsage = new DiskUsage(path, 0.999);
    Assert.assertFalse(notFullUsage.isDiskFull(1), "Disk must not report full: " + notFullUsage.getDiskStatus());
  }

  @Test
  public void testDatabaseSizeLimiter() {
    AtomicLong databaseSize = new AtomicLong(0);

    String databasePath = "./";
    long databaseSizeLimit = 100;
    long measurementInterval = 10;
    Supplier<Long> databaseSizeSupplier = () -> databaseSize.get();

    DiskUsage.DatabaseSizeLimiter limiter =
        new DiskUsage.DatabaseSizeLimiter(databasePath, databaseSizeLimit, measurementInterval, databaseSizeSupplier);
    Assert.assertFalse(limiter.hitDatabaseSizeLimit(databaseSize.addAndGet(10)));
    Assert.assertFalse(limiter.hitDatabaseSizeLimit(databaseSize.addAndGet(90)));

    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(limiter.hitDatabaseSizeLimit(databaseSize.addAndGet(10)));
    });

    limiter.shutdown();
  }
}
