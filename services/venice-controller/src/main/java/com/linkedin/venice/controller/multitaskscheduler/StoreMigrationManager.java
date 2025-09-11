package com.linkedin.venice.controller.multitaskscheduler;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private final int delayInSeconds; // Default delay in seconds for scheduling the next step
  private final List<String> childFabricList;
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
      List<String> childFabricList) {
    StoreMigrationManager manager =
        new StoreMigrationManager(threadPoolSize, maxRetryAttempts, delayInSeconds, childFabricList);
    LOGGER.info(
        "StoreMigrationManager initialized (threadPoolSize={}, maxRetryAttempts={}, delayInSeconds={}, childFabricList={}).",
        threadPoolSize,
        maxRetryAttempts,
        delayInSeconds,
        childFabricList);
    return manager;
  }

  /**
   * Constructor to initialize the StoreMigrationManager with the specified thread pool size and max retry attempts.
   * Only used for testing purpose, please use createStoreMigrationManager instead.
   * @param threadPoolSize
   * @param maxRetryAttempts
   * @param delayInSeconds
   * @param childFabricList
   */
  @VisibleForTesting
  StoreMigrationManager(int threadPoolSize, int maxRetryAttempts, int delayInSeconds, List<String> childFabricList) {
    super(threadPoolSize);
    this.maxRetryAttempts = maxRetryAttempts;
    this.childFabricList = Collections.unmodifiableList(new ArrayList<>(childFabricList));
    this.delayInSeconds = delayInSeconds;
    this.migrationRecords = new ConcurrentHashMap<>();
    this.migrationTasks = new ConcurrentHashMap<>();
  }

  public ScheduledFuture<?> scheduleMigration(
      String storeName,
      String sourceCluster,
      String destinationCluster,
      int currentStep) {
    return scheduleMigration(
        storeName,
        sourceCluster,
        destinationCluster,
        currentStep,
        Integer.MAX_VALUE,
        true,
        null,
        null,
        Collections.emptyMap(),
        Collections.emptyMap());
  }

  public ScheduledFuture<?> scheduleMigration(
      String storeName,
      String sourceCluster,
      String destinationCluster,
      int currentStep,
      int pauseAfterStep,
      boolean abortOnFailure,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      Map<String, ControllerClient> srcChildControllerClientMap,
      Map<String, ControllerClient> destChildControllerClientMap) {
    Objects.requireNonNull(srcChildControllerClientMap, "The source child controller client map cannot be null");
    Objects.requireNonNull(destChildControllerClientMap, "The destination child controller client map cannot be null");
    if (srcChildControllerClientMap.size() != childFabricList.size()) {
      throw new IllegalArgumentException(
          "Source child controller client map size does not match the number of child fabrics: "
              + srcChildControllerClientMap.size() + " != " + childFabricList.size());
    }
    MigrationRecord migrationRecord =
        new MigrationRecord.Builder(storeName, sourceCluster, destinationCluster).currentStep(currentStep)
            .abortOnFailure(abortOnFailure)
            .pauseAfter(pauseAfterStep)
            .build();
    // Atomic insert only if absent
    MigrationRecord existingRecord = migrationRecords.putIfAbsent(storeName, migrationRecord);
    if (existingRecord != null) {
      LOGGER.error("A migration record already exists for store: {}, existing record: {}", storeName, existingRecord);
      throw new IllegalArgumentException("A migration record already exists for store: " + storeName);
    }
    return scheduleNextStep(
        migrationRecord,
        this.delayInSeconds,
        srcControllerClient,
        destControllerClient,
        srcChildControllerClientMap,
        destChildControllerClientMap);
  }

  public ScheduledFuture<?> scheduleNextStep(
      MigrationRecord migrationRecord,
      int delayInSeconds,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      Map<String, ControllerClient> srcChildControllerClientMap,
      Map<String, ControllerClient> destChildControllerClientMap) {
    return super.scheduleNextStep(
        new StoreMigrationTask(
            migrationRecord,
            this,
            srcControllerClient,
            destControllerClient,
            srcChildControllerClientMap,
            destChildControllerClientMap,
            this.childFabricList),
        delayInSeconds);
  }

  public void pauseMigrationAfter(String storeName, int pauseAfterStep) {
    MigrationRecord rec = migrationRecords.get(storeName);
    if (rec != null) {
      if (rec.isPaused()) {
        LOGGER.warn("Migration for store {} is already paused, cannot pause again", storeName);
        throw new IllegalStateException("Migration for store " + storeName + " is already paused, cannot pause again.");
      }
      rec.setPauseAfter(pauseAfterStep);
      if (rec.getCurrentStep() > pauseAfterStep) {
        LOGGER.warn(
            "Current step {} is greater than pause after step {} for store {}, can't pause store migration.",
            rec.getCurrentStep(),
            pauseAfterStep,
            storeName);
        throw new IllegalArgumentException(
            "Current step " + rec.getCurrentStep() + " is greater than pause after step " + pauseAfterStep
                + " for store " + storeName + ", can't pause store migration.");
      }
      LOGGER.info("Migration for store {} was set to paused after step: {}", storeName, pauseAfterStep);
    } else {
      LOGGER.warn("No migration record found for store: {} for pause store migration.", storeName);
      throw new IllegalArgumentException(
          "No migration record found for store:" + storeName + ", can't pause store migration after step : "
              + pauseAfterStep);
    }
  }

  /**
   * Resumes the migration for a specific store from current step
   * @param storeName
   */
  public void resumeMigration(String storeName) {
    MigrationRecord rec = migrationRecords.get(storeName);
    if (rec != null) {
      resumeMigration(storeName, rec.getCurrentStep());
    } else {
      LOGGER.warn("No migration record found for store: {}", storeName);
    }
    throw new IllegalArgumentException(
        "No migration record found for store: " + storeName + ", cannot resume migration.");
  }

  /**
   * Resumes the migration for a specific store from the next step.
   * @param storeName
   * @param nextStep
   */
  public void resumeMigration(String storeName, int nextStep) {
    MigrationRecord rec = migrationRecords.get(storeName);
    if (rec != null) {
      StoreMigrationTask migrationTask = migrationTasks.get(storeName);
      if (!rec.isPaused()) {
        LOGGER.warn("Migration for store {} is not paused, cannot resume MigrationRecord {}", storeName, rec);
        throw new IllegalStateException("Migration for store " + storeName + " is not paused, cannot resume.");
      }
      if (migrationTask == null) {
        LOGGER.warn("No migration task found for store: {}, cannot resume MigrationRecord {}", storeName, rec);
        throw new IllegalStateException("No migration task found for store: " + storeName + ", cannot resume.");
      }
      rec.setCurrentStep(nextStep);
      super.scheduleNextStep(migrationTask, this.delayInSeconds);
      LOGGER.info("Migration for store {} was resumed at step: {}", storeName, nextStep);
    } else {
      LOGGER.error("No migration record found for store: {}", storeName);
      throw new IllegalArgumentException(
          "No migration record found for store: " + storeName + ", cannot resume migration.");
    }
  }

  public void cleanupMigrationRecord(String storeName) {
    migrationRecords.remove(storeName);
  }

  public int getMaxRetryAttempts() {
    return maxRetryAttempts;
  }

  public int getDelayInSeconds() {
    return delayInSeconds;
  }
}
