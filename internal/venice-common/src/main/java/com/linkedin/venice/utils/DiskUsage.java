package com.linkedin.venice.utils;

import static com.linkedin.venice.utils.DiskUsage.DatabaseSizeLimiter.DEFAULT_DATABASE_SIZE_MEASURE_INTERVAL;
import static java.lang.Thread.currentThread;

import com.linkedin.venice.exceptions.VeniceException;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * We define a threshold where we consider the disk to be full.  For example, we might declare the disk full at 0.90 usage.
 * Each task that writes to the disk maintains an instance of this class.  That instance calculates a "reserve" of space
 * that the task is allowed to write without checking the disk again.  For example, a 3TB (or 3000GB) disk with a threshold
 * of 0.90 requires 300G to be free on the disk.  We calculate the reserve space as 0.001 of the required free space.  In this
 * case 300MB.  On every write, the task calls this object's #isDiskFull() method, passing the number of bytes written.
 * Once the task has written 300MB (or whatever the reserve is) then this object will reinspect the disk for it's actual
 * usage and reallocate a reserve.  If we have exceeded the disk full threshold, we report the disk is full.
 *
 * We calculate reserve space as 0.001 of required free space to allow 1000 concurrent tasks to write to disk without
 * risk of blowing past the disk full threshold and actually reaching 100% disk usage.
 *
 * We add another way to limit the total amount of data, which can be writen locally.
 * For DaVinci use cases, the database folder can share the same disk as other components, such as logging or system files, so
 * it is hard to limit the total size of the database by measuring the disk usage ratio, plus different DaVinci instances
 * might have different database path setup as well.
 * The database size is especially important when using DaVinci in-memory mode, and we would like to use this way to
 * limit the maximum data mapped to memory (not fully accurate).
 */
public class DiskUsage implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(DiskUsage.class);

  private final String diskPath;
  private final long totalSpaceBytes;
  private final long freeSpaceBytesRequired; // less than this means disk full (over threshold)
  private final FileStore disk;

  // this is 0.001 of freeSpaceBytesRequired so that it would take 1000 concurrent tasks to blow
  // past the the diskFullThreshold and actually exhaust the disk before we check again.
  private final long reserveSpaceBytes;

  private long freeSpaceBytes; // This doesn't stay up-to-date, but is refreshed periodically
  private final AtomicLong reserveSpaceBytesRemaining = new AtomicLong();

  private final DatabaseSizeLimiter databaseSizeLimiter;

  public DiskUsage(String path, double diskFullThreshold) {
    this(path, diskFullThreshold, -1, DEFAULT_DATABASE_SIZE_MEASURE_INTERVAL);
  }

  public DiskUsage(
      String path,
      double diskFullThreshold,
      long databaseSizeLimit,
      long databaseSizeMeasurementInterval) {
    if (databaseSizeMeasurementInterval <= 0) {
      throw new IllegalArgumentException(
          "Database size measurement interval should be positive, but got: " + databaseSizeMeasurementInterval);
    }
    if (diskFullThreshold < 0 || diskFullThreshold > 1) {
      throw new IllegalArgumentException(
          "Disk full threshold must be a double between 0 and 1. " + diskFullThreshold + " is not valid");
    }
    this.diskPath = path;
    if (databaseSizeLimit > 0) {
      this.databaseSizeLimiter = new DatabaseSizeLimiter(path, databaseSizeLimit, databaseSizeMeasurementInterval);
    } else {
      this.databaseSizeLimiter = null;
    }
    try {
      this.disk = Files.getFileStore(Paths.get(path));
      this.totalSpaceBytes = disk.getTotalSpace();
    } catch (IOException e) {
      throw new VeniceException("Invalid path for DiskUsage: " + path);
    }
    freeSpaceBytesRequired = (long) ((1 - diskFullThreshold) * totalSpaceBytes);
    reserveSpaceBytes = (long) (freeSpaceBytesRequired * 0.001);
    queryDiskStatusAndUpdate();
  }

  private synchronized void queryDiskStatusAndUpdate() {
    try {
      freeSpaceBytes = disk.getUsableSpace();
    } catch (IOException e) {
      LOGGER.error("Failed to get usable space for disk: {}", diskPath, e);
      freeSpaceBytes = totalSpaceBytes; // If we can't query status for some reason, then let the system continue
                                        // operating
    }
    reserveSpaceBytesRemaining.set(reserveSpaceBytes);
    if (freeSpaceBytes < freeSpaceBytesRequired) {
      reserveSpaceBytesRemaining.set(0);
    }
  }

  /**
   * Each time you write bytes to the disk, call this method.  It will determine an appropriate frequency to update its
   * view of the disk so this call will usually be very fast.  If the disk is beyond the threshold that was set at
   * construction then it will return true.  Otherwise it returns false.
   */
  public boolean isDiskFull(long bytesWritten) {
    if (databaseSizeLimiter != null && databaseSizeLimiter.hitDatabaseSizeLimit(bytesWritten)) {
      return true;
    }
    // Here we're using -1 to subtract the newly written data from the reserve space remaining.
    // If the reserve space remaining is still greater than 0, report disk not full. Otherwise,
    // check the actual state of the disk.
    if (reserveSpaceBytesRemaining.addAndGet(-1 * bytesWritten) > 0) {
      return false; // disk not full, don't need to actually check again
    } else { // we need to check actual disk usage
      queryDiskStatusAndUpdate();
      return freeSpaceBytes < freeSpaceBytesRequired; // If there is less free space than required, the disk is full
    }
  }

  public String getDiskStatus() {
    StringBuilder sb = new StringBuilder();
    if (databaseSizeLimiter != null) {
      sb.append(databaseSizeLimiter.getDatabaseStatus()).append(", ");
    }
    sb.append("Disk at path: ")
        .append(diskPath)
        .append(" has ")
        .append(totalSpaceBytes)
        .append(" total bytes and ")
        .append(freeSpaceBytes)
        .append(" bytes free. ")
        .append("Disk is marked 'full' when there are less than ")
        .append(freeSpaceBytesRequired)
        .append(" bytes free.");
    return sb.toString();
  }

  @Override
  public void close() throws IOException {
    if (databaseSizeLimiter != null) {
      databaseSizeLimiter.shutdown();
    }
  }

  /**
   * This class is used to measure whether the current database size exceeds the limit or not.
   * It will measure the size after writing a configured amount of bytes, and the measurement
   * won't be very accurate since the measurement is independent of the database write operation,
   * and it was designed in this way to minimize the overhead of this class in the write path.
   */
  public static class DatabaseSizeLimiter {
    private static final Logger LOGGER = LogManager.getLogger(DatabaseSizeLimiter.class);
    public static final long DEFAULT_DATABASE_SIZE_MEASURE_INTERVAL = 256 * 1024 * 1024; // 256MB

    private final String databasePath;
    private final long databaseSizeLimit;
    private final long databaseSizeMeasurementInterval;
    private final AtomicLong currentDatabaseSize = new AtomicLong();
    private final Supplier<Long> databaseSizeSupplier;
    private final AtomicLong accumulatedByteSize = new AtomicLong();
    private final AtomicBoolean hasOngoingMeasurement = new AtomicBoolean(false);
    private final ExecutorService databaseSizeMeasurementExecutor =
        Executors.newSingleThreadExecutor(new DefaultThreadFactory("Database_Size_Measurement"));
    private boolean exceedDatabaseSizeLimit = false;

    public DatabaseSizeLimiter(String databasePath, long databaseSizeLimit, long databaseSizeMeasurementInterval) {
      this(
          databasePath,
          databaseSizeLimit,
          databaseSizeMeasurementInterval,
          () -> FileUtils.sizeOf(new File(databasePath)));
    }

    // For testing
    public DatabaseSizeLimiter(
        String databasePath,
        long databaseSizeLimit,
        long databaseSizeMeasurementInterval,
        Supplier<Long> databaseSizeSupplier) {
      this.databasePath = databasePath;
      this.databaseSizeLimit = databaseSizeLimit;
      this.databaseSizeMeasurementInterval = databaseSizeMeasurementInterval;
      this.databaseSizeSupplier = databaseSizeSupplier;
    }

    public boolean hitDatabaseSizeLimit(long bytesWritten) {
      if (databaseSizeLimit <= 0) {
        return false;
      }
      if (exceedDatabaseSizeLimit) {
        return true;
      }
      long accumulatedBytes = accumulatedByteSize.addAndGet(bytesWritten);
      if (accumulatedBytes > databaseSizeMeasurementInterval && hasOngoingMeasurement.compareAndSet(false, true)) {
        /**
         * Reset the counter and submit a database measurement task.
         *
         * The {@link #accumulatedByteSize} won't be very accurate because of race condition, but it is fine since
         * we would like to minimize the calls to measure the database size, which is an expensive operation.
          */
        accumulatedByteSize.set(0);
        databaseSizeMeasurementExecutor.execute(() -> {
          try {
            long startTime = System.currentTimeMillis();
            long databaseSize = databaseSizeSupplier.get();
            currentDatabaseSize.set(databaseSize);
            if (databaseSize >= databaseSizeLimit) {
              exceedDatabaseSizeLimit = true;
            }
            LOGGER.info(
                "Measured database size: {} for path: {}, and it took {} ms",
                databaseSize,
                databasePath,
                LatencyUtils.getElapsedTimeInMs(startTime));
          } catch (Exception e) {
            LOGGER.error("Failed to measure the size of database path: {}", databasePath, e);
          } finally {
            hasOngoingMeasurement.set(false);
          }
        });
      }
      return exceedDatabaseSizeLimit;
    }

    public String getDatabaseStatus() {
      StringBuilder sb = new StringBuilder("Database path: ").append(databasePath)
          .append(", current database size: ")
          .append(currentDatabaseSize.get())
          .append("bytes")
          .append(", and limit: ")
          .append(databaseSizeLimit)
          .append("bytes");
      return sb.toString();
    }

    public void shutdown() {
      databaseSizeMeasurementExecutor.shutdown();
      try {
        if (!databaseSizeMeasurementExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          databaseSizeMeasurementExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        currentThread().interrupt();
      }
    }
  }
}
