package com.linkedin.davinci.storage;

import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DiskHealthCheckService will wake up every 10 seconds by default and run a health check
 * in the disk by writing 64KB random data, read them back and verify the content; if there
 * is any error within the process, an in-memory state variable "diskHealthy" will be updated
 * to false; otherwise, "diskHealthy" will be kept as true.
 *
 * If there is a SSD failure, the disk operation could hang forever; in order to report such
 * kind of disk failure, there is a timeout mechanism inside the health status polling API;
 * a total timeout will be decided at the beginning:
 * totalTimeout = Math.max(30 seconds, health check interval + disk operation timeout)
 * we will keep track of the last update time for the in-memory health status variable, if
 * the in-memory status haven't been updated for more than the totalTimeout, we believe the
 * disk operation hang due to disk failure and start reporting unhealthy for this server.
 */
public class DiskHealthCheckService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(DiskHealthCheckService.class);

  private static final long HEALTH_CHECK_HARD_TIMEOUT = TimeUnit.SECONDS.toMillis(30); // 30 seconds

  private static final String TMP_FILE_NAME = ".health_check_file";
  private static final int TMP_FILE_SIZE_IN_BYTES = 64 * 1024; // 64KB

  // lock object protects diskHealthy and lastStatusUpdateTimeInNS.
  private final Lock lock = new ReentrantLock();
  private final long healthCheckIntervalMs;
  private final long healthCheckTimeoutMs;
  private final boolean serviceEnabled;
  private final String databasePath;
  private final long diskFailServerShutdownTimeMs;

  private boolean diskHealthy = true;
  private long lastStatusUpdateTimeInNS;
  private String errorMessage;
  private DiskHealthCheckTask healthCheckTask;
  private Thread runner;

  private void setDiskHealthy(boolean diskHealthy) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(lock)) {
      this.diskHealthy = diskHealthy;
    }
  }

  private boolean getDiskHealthy() {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(lock)) {
      return diskHealthy;
    }
  }

  private void setLastStatusUpdateTimeInNS(long lastStatusUpdateTimeInNS) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(lock)) {
      this.lastStatusUpdateTimeInNS = lastStatusUpdateTimeInNS;
    }
  }

  public DiskHealthCheckService(
      boolean serviceEnabled,
      long healthCheckIntervalMs,
      long diskOperationTimeout,
      String databasePath,
      long diskFailServerShutdownTimeMs) {
    this.serviceEnabled = serviceEnabled;
    this.healthCheckIntervalMs = healthCheckIntervalMs;
    this.healthCheckTimeoutMs = Math.max(HEALTH_CHECK_HARD_TIMEOUT, healthCheckIntervalMs + diskOperationTimeout);
    this.databasePath = databasePath;
    errorMessage = null;
    this.diskFailServerShutdownTimeMs = diskFailServerShutdownTimeMs;
  }

  @Override
  public boolean startInner() {
    if (!serviceEnabled) {
      // If the disk check feature is disabled, don't start the health check background thread
      healthCheckTask = null;
      return true;
    }
    setLastStatusUpdateTimeInNS(System.nanoTime());
    healthCheckTask = new DiskHealthCheckTask();
    runner = new Thread(healthCheckTask);
    runner.setName("Storage Disk Health Check Background Thread");
    runner.setDaemon(true);
    runner.start();

    return true;
  }

  public boolean isDiskHealthy() {
    if (!serviceEnabled) {
      // always return true if the feature is disabled.
      return true;
    }

    try (AutoCloseableLock ignore = AutoCloseableLock.of(lock)) {
      if (LatencyUtils.getElapsedTimeFromNSToMS(lastStatusUpdateTimeInNS) > healthCheckTimeoutMs) {
        /**
         * Disk operation hangs so the status has not been updated for {@link healthCheckTimeoutMs};
         * mark the host as unhealthy.
         */
        diskHealthy = false;
      }
      return diskHealthy;
    }
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public void stopInner() {
    if (healthCheckTask != null) {
      // No need to stop the task if it's not started at all.
      healthCheckTask.setStop();
      // interrupt the thread directly to avoid hanging
      runner.interrupt();
    }
  }

  private class DiskHealthCheckTask implements Runnable {
    private volatile boolean stop = false;

    protected void setStop() {
      stop = true;
    }

    private long unhealthyStartTime;

    @Override
    public void run() {
      while (!stop) {
        try {
          Thread.sleep(healthCheckIntervalMs);

          if (!getDiskHealthy()) {
            if (unhealthyStartTime != 0) {
              long duration = System.currentTimeMillis() - unhealthyStartTime;
              if (duration > diskFailServerShutdownTimeMs) {
                Utils.exit(
                    "Disk health service reported unhealthy disk for " + duration / Time.MS_PER_SECOND
                        + " seconds, STOPPING THE SERVER NOW!");
              }
            } else {
              unhealthyStartTime = System.currentTimeMillis();
            }
          } else {
            unhealthyStartTime = 0;
          }

          File databaseDir = new File(databasePath);
          if (!databaseDir.exists() || !databaseDir.isDirectory()) {
            errorMessage = databasePath + " does not exist or is not a directory!";
            setDiskHealthy(false);
            continue;
          }

          if (!(databaseDir.canRead() && databaseDir.canWrite())) {
            errorMessage = "No read/write permission for health check service in path " + databasePath;
            setDiskHealthy(false);
            continue;
          }

          // create a temporary file to test disk IO
          File tmpFile = new File(databaseDir, TMP_FILE_NAME);
          if (!tmpFile.exists()) {
            tmpFile.createNewFile();
          }
          // delete the temporary file at the end
          tmpFile.deleteOnExit();

          try (PrintWriter printWriter = new PrintWriter(tmpFile, "UTF-8")) {
            String message = String.valueOf(System.currentTimeMillis());
            // write 64KB data to the temporary file first
            int repeats = TMP_FILE_SIZE_IN_BYTES / message.length();
            for (int i = 0; i < repeats; i++) {
              printWriter.println(message);
            }
            printWriter.flush();

            // Check data in it.
            errorMessage = null;
            boolean fileReadableAndCorrect = true;
            try (BufferedReader br =
                new BufferedReader(new InputStreamReader(new FileInputStream(tmpFile), StandardCharsets.UTF_8))) {
              for (int i = 0; i < repeats; i++) {
                String newLine = br.readLine();
                if (!Objects.equals(newLine, message)) {
                  errorMessage =
                      "Content in health check file is different from what was written to it; expect message: "
                          + message + "; actual content: " + newLine;
                  fileReadableAndCorrect = false;
                  break;
                }
              }
            }

            try (AutoCloseableLock ignore = AutoCloseableLock.of(lock)) {
              // update the disk health status
              diskHealthy = fileReadableAndCorrect;
              lastStatusUpdateTimeInNS = System.nanoTime();
            }
          }
        } catch (InterruptedException e) {
          LOGGER.info("Disk check service thread shutting down (interrupted).");
        } catch (Exception ee) {
          LOGGER.error("Error while checking the disk health in server: ", ee);
          errorMessage = ee.getMessage();
          setDiskHealthy(false);
        }
      }
    }
  }
}
