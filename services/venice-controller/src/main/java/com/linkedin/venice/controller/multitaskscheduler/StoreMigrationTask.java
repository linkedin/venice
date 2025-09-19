package com.linkedin.venice.controller.multitaskscheduler;

import static com.linkedin.venice.controller.multitaskscheduler.MigrationRecord.Step;

import java.time.Duration;
import java.time.Instant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class StoreMigrationTask implements Runnable {
  private final MigrationRecord record;
  private final StoreMigrationManager manager;
  private Duration timeoutDuration = Duration.ofHours(24); // Default timeout duration
  private static final Logger LOGGER = LogManager.getLogger(StoreMigrationTask.class);

  public StoreMigrationTask(MigrationRecord record, StoreMigrationManager manager) {
    this.record = record;
    this.manager = manager;
  }

  @Override
  public void run() {
    try {
      switch (record.getCurrentStepEnum()) {
        case CHECK_DISK_SPACE:
          checkDiskSpace();
          break;
        case PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST:
          preCheckAndSubmitMigrationRequest();
          break;
        case VERIFY_MIGRATION_STATUS:
          verifyMigrationStatus();
          break;
        case UPDATE_CLUSTER_DISCOVERY:
          updateClusterDiscovery();
          break;
        case VERIFY_READ_REDIRECTION:
          verifyReadRedirection();
          break;
        case END_MIGRATION:
          endMigration();
          break;
        default:
          LOGGER.error(
              "Invalid migration step: {} of record {}. Please retry with a valid step between 0 and 5.",
              record.getCurrentStep(),
              record);
          manager.cleanupMigrationRecord(record.getStoreName());
      }
    } catch (Exception e) {
      LOGGER.error("Error occurred during migration for store: {}", record.getStoreName(), e);
      if (record.getAttempts() < manager.getMaxRetryAttempts()) {
        record.incrementAttempts();
        manager.scheduleNextStep(this, 60); // Reschedule for 1 minute later
      } else {
        handleMigrationFailure();
      }
    }
  }

  private void checkDiskSpace() {
    // Implement disk space check logic
    record.setCurrentStep(Step.PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST);
    record.resetAttempts();
    manager.scheduleNextStep(this, 0);
  }

  private void preCheckAndSubmitMigrationRequest() {
    // Implement pre-check and submission logic
    record.setCurrentStep(Step.VERIFY_MIGRATION_STATUS);
    record.resetAttempts();
    manager.scheduleNextStep(this, 0);
  }

  private void verifyMigrationStatus() {
    // Simulating the verification of migration status with a placeholder
    if (Instant.ofEpochMilli(-1).equals(record.getStoreMigrationStartTime())) {
      // If the migration start time is not set yet, set it to the current time
      record.setStoreMigrationStartTime(Instant.now());
    }

    boolean statusVerified = false; // Placeholder for actual status

    if (statusVerified) {
      record.setCurrentStep(Step.UPDATE_CLUSTER_DISCOVERY);
      record.resetAttempts();
      manager.scheduleNextStep(this, 0);
    } else if (isTimeout()) {
      handleMigrationFailure();
    } else {
      manager.scheduleNextStep(this, 60); // Reschedule for 1 minute later
    }
  }

  private void updateClusterDiscovery() {
    // Implement cluster discovery update
    boolean updateSuccessful = false; // Placeholder for actual update result
    // Replace with actual logic
    updateSuccessful = true; // Simulate a successful update for integration testing
    if (updateSuccessful) {
      record.setCurrentStep(Step.VERIFY_READ_REDIRECTION);
      record.resetAttempts();
      manager.scheduleNextStep(this, 0);
    } else {
      record.incrementAttempts();
      if (record.getAttempts() > manager.getMaxRetryAttempts()) {
        handleMigrationFailure();
      } else {
        manager.scheduleNextStep(this, 60); // Reschedule for 1 minute later
      }
    }
  }

  private void verifyReadRedirection() {
    // Implement read redirection verification
    boolean verified = false; // Placeholder for actual verification result

    // Replace with actual logic
    verified = true; // Simulate a successful update for integration testing
    if (verified) {
      record.setCurrentStep(Step.END_MIGRATION);
      record.resetAttempts();
      manager.scheduleNextStep(this, 0);
    } else if (record.getAttempts() > manager.getMaxRetryAttempts()) {
      handleMigrationFailure();
    } else {
      record.incrementAttempts();
      manager.scheduleNextStep(this, 60); // Reschedule for 1 minute later
    }
  }

  private void endMigration() {
    // Implement end store migration logic, including old store deletion
    record.setCurrentStep(Step.MIGRATION_SUCCEED); // Mark as store migration succeeded

    manager.cleanupMigrationRecord(record.getStoreName());
  }

  private boolean isTimeout() {
    return Duration.between(record.getStoreMigrationStartTime(), Instant.now()).compareTo(timeoutDuration) >= 0;
  }

  public void setTimeout(int hours) {
    this.timeoutDuration = Duration.ofHours(hours);
  }

  private void handleMigrationFailure() {
    manager.cleanupMigrationRecord(record.getStoreName());
    // Rollback the store migrated to the destination cluster
    if (record.getAbortOnFailure()) {
      LOGGER.info("Aborting migration for store: {}", record.getStoreName());
      // TODO: Implement logic to abort the migration process
      record.setIsAborted(true);
    }

  }
}
