package com.linkedin.venice;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a simple runnable which keeps fetching task from list and execute the assigned task. The task fetching
 * and progress tracking / checkpointing is thread-safe, so it can be run in parallel.
 */
public class BatchMaintenanceTaskRunner implements Runnable {
  private static final Logger LOGGER = LogManager.getLogger(BatchMaintenanceTaskRunner.class);
  private static final String TASK_LOG_PREFIX = "[**** TASK INFO ****]";

  private static final ReentrantLock LOCK = new ReentrantLock();
  private static final AtomicInteger INDEX = new AtomicInteger(-1);
  private final List<String> taskList;
  private final Function<String, Boolean> storeRunnable;
  private final Map<String, Boolean> progressMap;
  private final String checkpointFile;

  public BatchMaintenanceTaskRunner(
      Map<String, Boolean> progressMap,
      String checkpointFile,
      List<String> taskList,
      Function<String, Boolean> storeRunnable) {
    this.taskList = taskList;
    this.storeRunnable = storeRunnable;
    this.progressMap = progressMap;
    this.checkpointFile = checkpointFile;
  }

  @Override
  public void run() {
    while (true) {
      int fetchedTaskIndex = INDEX.incrementAndGet();
      if (fetchedTaskIndex >= taskList.size()) {
        LOGGER.info("Cannot find new store from queue, will exit.");
        break;
      }
      String store = taskList.get(fetchedTaskIndex);
      try {
        LOGGER.info("{} Running store job: {} for store: {}", TASK_LOG_PREFIX, fetchedTaskIndex + 1, store);
        boolean result = storeRunnable.apply(store);
        if (result) {
          LOGGER.info(
              "{} Complete store task for job: {}/{} store: {}",
              TASK_LOG_PREFIX,
              fetchedTaskIndex + 1,
              taskList.size(),
              store);
          progressMap.put(store, true);
        } else {
          LOGGER.info(
              "{} Failed store task for job: {}/{} store: {}",
              TASK_LOG_PREFIX,
              fetchedTaskIndex + 1,
              taskList.size(),
              store);
        }
        // Periodically update the checkpoint file.
        if ((fetchedTaskIndex % 100) == 0) {
          LOGGER.info("{} Preparing to checkpoint status at index {}", TASK_LOG_PREFIX, fetchedTaskIndex);
          checkpoint(checkpointFile);
        }
      } catch (Exception e) {
        LOGGER.info("{} Caught exception: {}. Will exit.", TASK_LOG_PREFIX, e.getMessage());
      }
    }
    // Perform one final checkpointing before existing the runnable.
    checkpoint(checkpointFile);
  }

  public void checkpoint(String checkpointFile) {
    try {
      LOCK.lock();
      LOGGER.info("Updating checkpoint...");

      List<String> status =
          progressMap.entrySet().stream().map(e -> e.getKey() + "," + e.getValue()).collect(Collectors.toList());
      Files.write(Paths.get(checkpointFile), status);
      LOGGER.info("Updated checkpoint...");

    } catch (IOException e) {
      throw new VeniceException(e);
    } finally {
      LOCK.unlock();
    }
  }
}
