package com.linkedin.venice.controller.multitaskscheduler;

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
      switch (record.getCurrentStep()) {
        case 0:
          checkDiskSpace();
          break;
        case 1:
          preCheckAndSubmitMigrationRequest();
          break;
        case 2:
          verifyMigrationStatus();
          break;
        case 3:
          updateClusterDiscovery();
          break;
        case 4:
          verifyReadRedirection();
          break;
        case 5:
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
        abortMigration();
      }
    }
  }

  private void checkDiskSpace() {
    // Implement disk space check logic
    record.setCurrentStep(1);
    manager.scheduleNextStep(this, 0);
  }

  private void preCheckAndSubmitMigrationRequest() {
    // Implement pre-check and submission logic
    record.setCurrentStep(2);
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
      record.setCurrentStep(3);
      manager.scheduleNextStep(this, 0);
    } else if (isTimeout()) {
      abortMigration();
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
      record.setCurrentStep(4);
      manager.scheduleNextStep(this, 0);
    } else {
      record.incrementAttempts();
      if (record.getAttempts() > manager.getMaxRetryAttempts()) {
        abortMigration();
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
      record.setCurrentStep(5);
      manager.scheduleNextStep(this, 0);
    } else if (record.getAttempts() > manager.getMaxRetryAttempts()) {
      abortMigration();
    } else {
      record.incrementAttempts();
      manager.scheduleNextStep(this, 60); // Reschedule for 1 minute later
    }
  }

  private void endMigration() {
    // Implement end store migration logic, including old store deletion
    record.setCurrentStep(6); // Mark as completed
    manager.cleanupMigrationRecord(record.getStoreName());
  }

  private boolean isTimeout() {
    return Duration.between(record.getStoreMigrationStartTime(), Instant.now()).compareTo(timeoutDuration) >= 0;
  }

  public void setTimeout(int hours) {
    this.timeoutDuration = Duration.ofHours(hours);
  }

  private void abortMigration() {
    manager.cleanupMigrationRecord(record.getStoreName());
    record.setIsAborted(true);
  }
}
