package com.linkedin.venice.storage;

import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import org.apache.log4j.Logger;


public class DiskHealthCheckService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(DiskHealthCheckService.class);

  private volatile boolean diskHealthy = true;
  private long healthCheckIntervalMs;
  private long lastStatusUpdateTimeInNS;
  private boolean serviceEnabled;
  private String databasePath;
  private static String tmpFileName = ".health_check_file";
  private static int tmpFileSizeInBytes = 64 * 1024; // 64KB
  private String errorMessage;
  private DiskHealthCheckTask healthCheckTask;
  private Thread runner;

  public DiskHealthCheckService(boolean serviceEnabled, long healthCheckIntervalMs, String databasePath) {
    this.serviceEnabled = serviceEnabled;
    this.healthCheckIntervalMs = healthCheckIntervalMs;
    this.databasePath = databasePath;
    errorMessage = null;
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

    if (LatencyUtils.getLatencyInMS(lastStatusUpdateTimeInNS) > 5l * healthCheckIntervalMs) {
      /**
       * Disk operation hangs so the status has not been updated for 5 health check cycles;
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

    @Override
    public void run() {
      while (!stop) {
        try {
          Thread.sleep(healthCheckIntervalMs);

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
            printWriter.print(message + "\n");
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
              errorMessage = "Content in health check file is different from what was written to it; expect message: " + message + "; actual content: " + newLine;
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
