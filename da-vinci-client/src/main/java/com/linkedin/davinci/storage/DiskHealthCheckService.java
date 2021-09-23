package com.linkedin.davinci.storage;

import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


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
  private static final Logger logger = Logger.getLogger(DiskHealthCheckService.class);

  private static final long HEALTH_CHECK_HARD_TIMEOUT = TimeUnit.SECONDS.toMillis(30); // 30 seconds

  private volatile boolean diskHealthy = true;
  private long healthCheckIntervalMs;
  private long healthCheckTimeoutMs;
  private long lastStatusUpdateTimeInNS;
  private boolean serviceEnabled;
  private String databasePath;
  private static String tmpFileName = ".health_check_file";
  private static int tmpFileSizeInBytes = 64 * 1024; // 64KB
  private String errorMessage;
  private DiskHealthCheckTask healthCheckTask;
  private Thread runner;
  private long diskFailServerShutdownTimeMs;

  public DiskHealthCheckService(boolean serviceEnabled, long healthCheckIntervalMs, long diskOperationTimeout,
      String databasePath, long diskFailServerShutdownTimeMs) {
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
    lastStatusUpdateTimeInNS = System.nanoTime();
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

    if (LatencyUtils.getLatencyInMS(lastStatusUpdateTimeInNS) > healthCheckTimeoutMs) {
      /**
       * Disk operation hangs so the status has not been updated for {@link healthCheckTimeoutMs};
       * mark the host as unhealthy.
       */
      diskHealthy = false;
    }
    return diskHealthy;
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
    protected void setStop(){
      stop = true;
    }
    private long unhealthyStartTime;

    @Override
    public void run() {
      while (!stop) {
        try {
          Thread.sleep(healthCheckIntervalMs);

          if (!diskHealthy) {
            if (unhealthyStartTime != 0) {
              long duration = System.currentTimeMillis() - unhealthyStartTime;
              if (duration > diskFailServerShutdownTimeMs) {
                Utils.exit("Disk health service reported unhealthy disk for " + duration / Time.MS_PER_SECOND
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
            diskHealthy = false;
            continue;
          }

          if (!(databaseDir.canRead() && databaseDir.canWrite())) {
            errorMessage = "No read/write permission for health check service in path " + databasePath;
            diskHealthy = false;
            continue;
          }

          // create a temporary file to test disk IO
          File tmpFile = new File(databaseDir, tmpFileName);
          if (!tmpFile.exists()) {
            tmpFile.createNewFile();
          }
          // delete the temporary file at the end
          tmpFile.deleteOnExit();

          PrintWriter printWriter = new PrintWriter(tmpFile, "UTF-8");
          String message = String.valueOf(System.currentTimeMillis());
          // write 64KB data to the temporary file first
          int repeats = tmpFileSizeInBytes / message.length();
          for (int i = 0; i < repeats; i++) {
            printWriter.println(message);
          }
          printWriter.flush();
          printWriter.close();

          // Check data in it.
          errorMessage = null;
          boolean fileReadableAndCorrect = true;
          BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(tmpFile), StandardCharsets.UTF_8));
          for (int i = 0; i < repeats; i++) {
            String newLine = br.readLine();
            if (!newLine.equals(message)) {
              errorMessage = "Content in health check file is different from what was written to it; expect message: "
                  + message + "; actual content: " + newLine;
              fileReadableAndCorrect = false;
              break;
            }
          }
          br.close();

          // update the disk health status
          diskHealthy = fileReadableAndCorrect;
          lastStatusUpdateTimeInNS = System.nanoTime();
        } catch (InterruptedException e) {
          logger.info("Disk check service thread shutting down", e);
        } catch (Exception ee) {
          logger.error("Error while checking the disk health in server: ", ee);
          errorMessage = ee.getMessage();
          diskHealthy = false;
        }
      }
    }
  }
}
