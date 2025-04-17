package com.linkedin.davinci.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import org.testng.annotations.Test;


public class DiskHealthCheckServiceTest {
  @Test
  public void testBasicFunctionality() {
    try (DiskHealthCheckService diskHealthCheckService = new DiskHealthCheckService(
        true,
        Duration.ofMillis(100).toMillis(),
        Duration.ofMillis(100).toMillis(),
        Utils.getUniqueTempPath(),
        Duration.ofSeconds(1).toMillis())) {
      diskHealthCheckService.start();

      assertTrue(diskHealthCheckService.isDiskHealthy());
      assertNull(diskHealthCheckService.getErrorMessage());
      assertTrue(diskHealthCheckService.getDiskHealthy());

      // Test disk becomes unhealthy
      String errorMessage = "I Want My Hat Back";
      diskHealthCheckService.setDiskUnhealthy(errorMessage);
      assertFalse(diskHealthCheckService.isDiskHealthy());
      assertEquals(diskHealthCheckService.getErrorMessage(), errorMessage);
      assertFalse(diskHealthCheckService.getDiskHealthy());

      // Disk becomes healthy again
      diskHealthCheckService.setDiskHealthy();
      assertTrue(diskHealthCheckService.isDiskHealthy());
      assertNull(diskHealthCheckService.getErrorMessage());
      assertTrue(diskHealthCheckService.getDiskHealthy());

      // Last status was updated a long time ago
      diskHealthCheckService.setLastStatusUpdateTimeInNS(System.nanoTime() - Duration.ofHours(1).toNanos());
      assertFalse(diskHealthCheckService.isDiskHealthy());
      assertTrue(diskHealthCheckService.getErrorMessage().startsWith("Status has not been updated for"));
      assertFalse(diskHealthCheckService.getDiskHealthy());
    }
  }

  @Test
  public void testDbDirectoryNotExist() throws IOException {
    String directory = Utils.getUniqueTempPath();
    assertTrue(new File(directory).mkdirs());

    // Test if directory does not exist
    testDbDirectoryError(directory + "/non_existent", "does not exist or is not a directory!");

    // Test if db directory points to a file
    Path filePath = Paths.get(directory + "/file_path");
    Files.createFile(filePath);
    testDbDirectoryError(filePath.toString(), "does not exist or is not a directory!");

    // No read permissions to input path
    File noRead = new File(directory + "/directory_path_no_read");
    assertTrue(noRead.mkdirs());
    assertTrue(noRead.setReadable(false));
    testDbDirectoryError(noRead.toString(), "No read/write permission for health check service in path");

    // No write permissions to input path
    File noWrite = new File(directory + "/directory_path_no_write");
    assertTrue(noWrite.mkdirs());
    assertTrue(noWrite.setWritable(false));
    testDbDirectoryError(noWrite.toString(), "No read/write permission for health check service in path");
  }

  private void testDbDirectoryError(String directory, String errorMessage) {
    try (DiskHealthCheckService diskHealthCheckService = new DiskHealthCheckService(
        true,
        Duration.ofMillis(100).toMillis(),
        Duration.ofMillis(100).toMillis(),
        directory,
        Duration.ofSeconds(1).toMillis())) {
      diskHealthCheckService.start();

      // Wait for the health check to run at least once
      Utils.sleep(500);

      assertFalse(diskHealthCheckService.isDiskHealthy());
      assertTrue(diskHealthCheckService.getErrorMessage().contains(errorMessage));
      assertFalse(diskHealthCheckService.getDiskHealthy());
    }
  }

  @Test
  public void testHealthCheckFileWrite() {
    String directory = Utils.getUniqueTempPath();
    assertTrue(new File(directory).mkdirs());

    try (DiskHealthCheckService diskHealthCheckService = new DiskHealthCheckService(
        true,
        Duration.ofMillis(100).toMillis(),
        Duration.ofMillis(100).toMillis(),
        directory,
        Duration.ofSeconds(1).toMillis())) {
      diskHealthCheckService.start();

      // Wait for the health check to run at least once
      Utils.sleep(500);

      assertTrue(diskHealthCheckService.isDiskHealthy());
      assertNull(diskHealthCheckService.getErrorMessage());
      assertTrue(diskHealthCheckService.getDiskHealthy());

      Path healthCheckFilePath = Paths.get(directory + "/" + DiskHealthCheckService.TMP_FILE_NAME);
      assertTrue(Files.exists(healthCheckFilePath));
      assertTrue(Files.isRegularFile(healthCheckFilePath));

      // Mimic text old
      String incorrectMessage = "I Want My Hat Back";
      diskHealthCheckService.getHealthCheckTask()
          .setHealthCheckDiskAccessor(new CorruptFileHealthCheckDiskAccessor(incorrectMessage));

      // Wait for the health check to run at least once
      Utils.sleep(500);

      assertFalse(diskHealthCheckService.isDiskHealthy());
      assertTrue(diskHealthCheckService.getErrorMessage().endsWith(incorrectMessage));
      assertFalse(diskHealthCheckService.getDiskHealthy());

      // Disk is healthy again
      diskHealthCheckService.getHealthCheckTask().resetHealthCheckDiskAccessor();

      // Wait for the health check to run at least once
      Utils.sleep(500);

      assertTrue(diskHealthCheckService.isDiskHealthy());
      assertNull(diskHealthCheckService.getErrorMessage());
      assertTrue(diskHealthCheckService.getDiskHealthy());
    }
  }

  private static class CorruptFileHealthCheckDiskAccessor extends DiskHealthCheckService.HealthCheckDiskAccessor {
    private final String message;

    private CorruptFileHealthCheckDiskAccessor(String message) {
      this.message = message;
    }

    // Mimic that data got corrupted after writing to disk
    @Override
    String validateHealthCheckFile(File file, String expectedMessage, int repeats) throws IOException {
      return message;
    }
  }
}
