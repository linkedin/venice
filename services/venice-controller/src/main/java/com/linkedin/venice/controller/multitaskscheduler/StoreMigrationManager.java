package com.linkedin.venice.controller.multitaskscheduler;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreMigrationManager extends ScheduledTaskManager {
  // TODO: MigrationRecord will be persisted to ZooKeeper, and cleaned up when the migration is complete, in a future
  // pull request.
  private Map<String, MigrationRecord> migrationRecords;
  protected Map<String, StoreMigrationTask> migrationTasks;
  private final int maxRetryAttempts;
  protected final int delayInSeconds; // Default delay in seconds for scheduling the next step
  private final List<String> fabricList;
  private static final Logger LOGGER = LogManager.getLogger(StoreMigrationManager.class);

  public MigrationRecord getMigrationRecord(String storeName) {
    return migrationRecords.get(storeName);
  }

  @Override
  protected ScheduledExecutorService createExecutorService(int threadPoolSize) {
    return Executors
        .newScheduledThreadPool(threadPoolSize, new DaemonThreadFactory("`MultiTaskScheduler-StoreMigration"));
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  public static StoreMigrationManager createStoreMigrationManager(
      int threadPoolSize,
      int maxRetryAttempts,
      int delayInSeconds,
      List<String> fabricList) {
    StoreMigrationManager manager =
        new StoreMigrationManager(threadPoolSize, maxRetryAttempts, delayInSeconds, fabricList);
    LOGGER.info("StoreMigrationManager initialized with thread pool size: {}", threadPoolSize);
    LOGGER.info("Maximum retry attempts set to: {}", maxRetryAttempts);
    return manager;
  }

  /**
   * Constructor to initialize the StoreMigrationManager with the specified thread pool size and max retry attempts.
   * Only used for testing purpose, please use createStoreMigrationManager instead.
   * @param threadPoolSize
   * @param maxRetryAttempts
   */
  @Deprecated
  public StoreMigrationManager(int threadPoolSize, int maxRetryAttempts, int delayInSeconds, List<String> fabricList) {
    super(threadPoolSize);
    this.maxRetryAttempts = maxRetryAttempts;
    this.fabricList = fabricList;
    this.delayInSeconds = delayInSeconds;
    this.migrationRecords = new ConcurrentHashMap<>();
    this.migrationTasks = new ConcurrentHashMap<>();
  }

  public ScheduledFuture<?> scheduleMigration(
      String storeName,
      String sourceCluster,
      String destinationCluster,
      int currentStep) {
    // TODO fix null value
    return scheduleMigration(
        storeName,
        sourceCluster,
        destinationCluster,
        currentStep,
        0,
        true,
        null,
        null,
        null,
        null,
        Integer.MAX_VALUE);
  }

  public ScheduledFuture<?> scheduleMigration(
      String storeName,
      String sourceCluster,
      String destinationCluster,
      int currentStep,
      int delayInSeconds,
      boolean abortOnFailure,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      Map<String, ControllerClient> srcChildControllerClientMap,
      Map<String, ControllerClient> destChildControllerClientMap,
      int pauseAfterStep) {
    MigrationRecord record =
        new MigrationRecord.Builder(storeName, sourceCluster, destinationCluster).currentStep(currentStep)
            .abortOnFailure(abortOnFailure)
            .pauseAfter(pauseAfterStep)
            .build();
    migrationRecords.put(storeName, record);
    return scheduleNextStep(
        record,
        delayInSeconds,
        srcControllerClient,
        destControllerClient,
        srcChildControllerClientMap,
        destChildControllerClientMap);
  }

  public ScheduledFuture<?> scheduleNextStep(
      MigrationRecord record,
      int delayInSeconds,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      Map<String, ControllerClient> srcChildControllerClientMap,
      Map<String, ControllerClient> destChildControllerClientMap) {
    return super.scheduleNextStep(
        new StoreMigrationTask(
            record,
            this,
            srcControllerClient,
            destControllerClient,
            srcChildControllerClientMap,
            destChildControllerClientMap,
            this.fabricList),
        delayInSeconds);
  }

  public void pauseMigrationAfter(String storeName, MigrationRecord.Step pauseAfterStep) {
    MigrationRecord rec = migrationRecords.get(storeName);
    if (rec != null) {
      rec.setPauseAfter(pauseAfterStep);
      LOGGER.info("Migration for store {} was set to paused after step: {}", storeName, pauseAfterStep);
    } else {
      LOGGER.warn("No migration record found for store: {}", storeName);
    }
  }

  public void resumeMigration(String storeName) {
    MigrationRecord rec = migrationRecords.get(storeName);
    if (rec != null) {
      resumeMigration(storeName, rec.getCurrentStepEnum());
    } else {
      LOGGER.warn("No migration record found for store: {}", storeName);
    }
  }

  public void resumeMigration(String storeName, MigrationRecord.Step nextStep) {
    MigrationRecord rec = migrationRecords.get(storeName);
    if (rec != null) {
      if (!rec.isPaused()) {
        LOGGER.warn("Migration for store {} is not paused, cannot resume", storeName);
        return;
      }
      rec.setCurrentStep(nextStep);
      super.scheduleNextStep(migrationTasks.get(rec.getStoreName()), this.delayInSeconds);
      LOGGER.info("Migration for store {} was resumed at step: {}", storeName, nextStep);
    } else {
      LOGGER.warn("No migration record found for store: {}", storeName);
    }
  }

  public void cleanupMigrationRecord(String storeName) {
    migrationRecords.remove(storeName);
  }

  public int getMaxRetryAttempts() {
    return maxRetryAttempts;
  }
}
