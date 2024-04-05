package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
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
 */
public class DiskUsage {
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

  public DiskUsage(String path, double diskFullThreshold) {
    this.diskPath = path;
    if (diskFullThreshold < 0 || diskFullThreshold > 1) {
      throw new IllegalArgumentException(
          "Disk full threshold must be a double between 0 and 1. " + diskFullThreshold + " is not valid");
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
    return "Disk at path: " + diskPath + " has " + totalSpaceBytes + " total bytes and " + freeSpaceBytes
        + " bytes free.  " + "Disk is marked 'full' when there are less than " + freeSpaceBytesRequired
        + " bytes free.";
  }
}
