package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Singleton class meant to keep track of subscription failures for ingestion tasks where the ingestion
 * task needs to keep running despite cluster failures.
 *
 * TODO: This class could stand to be refactored into an Actor class, but it seems we're short on time just now,
 * so we're going for old hat to start with AbstractVeniceService.
 */
public class RemoteIngestionRepairService extends AbstractVeniceService {
  private final Thread repairThread;
  private final Map<StoreIngestionTask, BlockingQueue<Runnable>> ingestionRepairTasks;
  // 30 minutes default sleep interval
  public static final int DEFAULT_REPAIR_THREAD_SLEEP_INTERVAL_SECONDS = 1800;
  private final int repairThreadSleepInterval;
  private static final Logger LOGGER = LogManager.getLogger(RemoteIngestionRepairService.class);

  public RemoteIngestionRepairService(int repairThreadSleepInterval) {
    // Create queue and polling thread
    repairThread = new IngestionRepairServiceThread();
    ingestionRepairTasks = new VeniceConcurrentHashMap<>();
    this.repairThreadSleepInterval = repairThreadSleepInterval;
  }

  @Override
  public boolean startInner() throws Exception {
    repairThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    repairThread.interrupt();
  }

  public void registerRepairTask(StoreIngestionTask storeIngestionTask, Runnable repairTask) {
    if (!repairThread.isAlive()) {
      // We probably do want to bubble an exception up as the implication here is that
      // an important repair task that we would certainly want to complete simply won't because
      // there is no running thread (as it has been killed). Though potentially, there might be an attempt
      // to queue a task while things are shutting down (and lead to exceptions). So to that end, we need to be
      // certain that shutdowns are well ordered (or calling classes have defensive measures for this case if it's
      // somehow
      // valid).
      throw new IllegalStateException("RemoteIngestionRepairService is no longer running!  Rejecting repair task!!");
    }
    BlockingQueue<Runnable> taskList =
        ingestionRepairTasks.computeIfAbsent(storeIngestionTask, s -> new LinkedBlockingDeque<>());
    taskList.offer(repairTask);
  }

  public void unregisterRepairTasksForStoreIngestionTask(StoreIngestionTask storeIngestionTask) {
    BlockingQueue<Runnable> taskList = ingestionRepairTasks.remove(storeIngestionTask);
    taskList.clear();
  }

  // For testing only
  /*package private */ Map<StoreIngestionTask, BlockingQueue<Runnable>> getIngestionRepairTasks() {
    return ingestionRepairTasks;
  }

  /*package private */ void pollRepairTasks() {
    for (Map.Entry<StoreIngestionTask, BlockingQueue<Runnable>> taskEntry: ingestionRepairTasks.entrySet()) {
      BlockingQueue<Runnable> taskQueue = taskEntry.getValue();
      // If there's nothing in the task queue, skip this iteration
      if (taskQueue.isEmpty()) {
        continue;
      }
      Runnable task = taskQueue.poll();
      try {
        task.run();
      } catch (Exception e) {
        LOGGER.error("Failed to repair partition for ingestion task for store: {}", taskEntry.getKey());
        taskQueue.add(task);
      }
    }
  }

  private class IngestionRepairServiceThread extends Thread {
    IngestionRepairServiceThread() {
      super("Ingestion-Repair-Service-Thread");
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        pollRepairTasks();
        try {
          TimeUnit.SECONDS.sleep(repairThreadSleepInterval);
        } catch (InterruptedException e) {
          // We've received an interrupt which is to be expected, so we'll just leave the loop and log
          break;
        }
      }
      LOGGER.info("RemoteIngestionRepairService thread interrupted!  Shutting down...");
    }
  }
}
