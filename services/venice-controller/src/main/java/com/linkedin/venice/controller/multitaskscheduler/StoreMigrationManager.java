package com.linkedin.venice.controller.multitaskscheduler;

import com.linkedin.venice.utils.DaemonThreadFactory;
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
  private final int maxRetryAttempts;
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

  public static StoreMigrationManager createStoreMigrationManager(int threadPoolSize, int maxRetryAttempts) {
    StoreMigrationManager manager = new StoreMigrationManager(threadPoolSize, maxRetryAttempts);
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
  public StoreMigrationManager(int threadPoolSize, int maxRetryAttempts) {
    super(threadPoolSize);
    this.maxRetryAttempts = maxRetryAttempts;
    this.migrationRecords = new ConcurrentHashMap<>();
  }

  public ScheduledFuture<?> scheduleMigration(
      String storeName,
      String sourceCluster,
      String destinationCluster,
      int currentStep) {
    return scheduleMigration(storeName, sourceCluster, destinationCluster, currentStep, 0, true);
  }

  public ScheduledFuture<?> scheduleMigration(
      String storeName,
      String sourceCluster,
      String destinationCluster,
      int currentStep,
      int delayInSeconds,
      boolean abortOnFailure) {
    MigrationRecord record =
        new MigrationRecord.Builder(storeName, sourceCluster, destinationCluster).currentStep(currentStep)
            .abortOnFailure(abortOnFailure)
            .build();
    migrationRecords.put(storeName, record);
    return scheduleNextStep(record, delayInSeconds);
  }

  public ScheduledFuture<?> scheduleNextStep(MigrationRecord record, int delayInSeconds) {
    return super.scheduleNextStep(new StoreMigrationTask(record, this), delayInSeconds);
  }

  public void cleanupMigrationRecord(String storeName) {
    migrationRecords.remove(storeName);
  }

  public int getMaxRetryAttempts() {
    return maxRetryAttempts;
  }
}
