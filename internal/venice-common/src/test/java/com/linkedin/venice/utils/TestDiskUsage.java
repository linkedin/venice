package com.linkedin.venice.utils;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
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
}
