package com.linkedin.venice.controller.multitaskscheduler;

import static com.linkedin.venice.controller.multitaskscheduler.MigrationRecord.Step;
import static com.linkedin.venice.controller.multitaskscheduler.StoreMigrationUtils.applyPauseIfNeeded;
import static com.linkedin.venice.controller.multitaskscheduler.StoreMigrationUtils.isClonedStoreOnline;

import com.linkedin.venice.controllerapi.ChildAwareResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StoreInfo;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class StoreMigrationTask implements Runnable {
  private final MigrationRecord record;
  private final StoreMigrationManager manager;
  private Duration timeoutDuration = Duration.ofHours(24); // Default timeout duration
  private static final Logger LOGGER = LogManager.getLogger(StoreMigrationTask.class);
  private ControllerClient srcControllerClient;
  private ControllerClient destControllerClient;
  Map<String, ControllerClient> srcChildControllerClientMap;
  Map<String, ControllerClient> destChildControllerClientMap;
  private final Map<String, Boolean> fabricReadyMap = new HashMap<>();
  boolean statusVerified = false;

  public StoreMigrationTask(
      MigrationRecord record,
      StoreMigrationManager manager,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      Map<String, ControllerClient> srcChildControllerClientMap,
      Map<String, ControllerClient> destChildControllerClientMap,
      List<String> fabricList) {
    this.record = record;
    this.manager = manager;
    this.srcControllerClient = srcControllerClient;
    this.destControllerClient = destControllerClient;
    this.srcChildControllerClientMap = srcChildControllerClientMap;
    this.destChildControllerClientMap = destChildControllerClientMap;
    configureRegionReadyMap(fabricList);
  }

  private void configureRegionReadyMap(List<String> fabricList) {
    for (String fabric: fabricList) {
      this.fabricReadyMap.put(fabric, false);
    }
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
        manager.scheduleNextStep(this, this.manager.getDelayInSeconds());
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
    if (applyPauseIfNeeded(record)) {
      // If the migration is paused, do not proceed further and save it for resume migration
      manager.migrationTasks.put(record.getStoreName(), this);
      return;
    }
    LOGGER.info("Submit migration request of store {}...", record.getStoreName());
    // Implement pre-check
    StoreInfo srcStoreInfo = srcControllerClient.getStore(record.getStoreName()).getStore();
    if (!srcStoreInfo.isMigrating()) {
      StoreMigrationResponse storeMigrationResponse =
          srcControllerClient.migrateStore(record.getStoreName(), record.getDestinationCluster());
      if (storeMigrationResponse.isError()) {
        LOGGER.error(
            "Store migration failed for store: {} on step {} with error: {}",
            record.getStoreName(),
            record.getCurrentStepEnum(),
            storeMigrationResponse.getError());
        throw new VeniceException(
            "Store migration failed on submitting migrateStore request for store:" + record.getStoreName() + " on step "
                + record.getCurrentStepEnum() + " with error: " + storeMigrationResponse.getError());
      }
    }
    record.setCurrentStep(Step.VERIFY_MIGRATION_STATUS);
    record.resetAttempts();
    manager.scheduleNextStep(this, this.manager.getDelayInSeconds());
  }

  private void verifyMigrationStatus() {
    if (applyPauseIfNeeded(record)) {
      // If the migration is paused, do not proceed further
      manager.migrationTasks.put(record.getStoreName(), this);
      return;
    }
    LOGGER.info("Verifying migration status for migration of store {}...", record.getStoreName());
    if (Instant.ofEpochMilli(-1).equals(record.getStoreMigrationStartTime())) {
      // If the migration start time is not set yet, set it to the current time
      record.setStoreMigrationStartTime(Instant.now());
    }

    if (srcChildControllerClientMap.isEmpty() && destChildControllerClientMap.isEmpty()) {
      // This is a controller in single datacenter setup
      LOGGER.warn("Fabric option is ignored on child controller for store migration record : {}", record);
      if (isClonedStoreOnline(srcControllerClient, destControllerClient, record)) {
        statusVerified = true;
        LOGGER.info(
            "Store {} is ready in the destination cluster: {}",
            record.getStoreName(),
            record.getDestinationCluster());
      } else {
        LOGGER.debug(
            "Store {} is not ready in the destination cluster: {}",
            record.getStoreName(),
            record.getDestinationCluster());
      }
    } else {
      List<String> notReadyFabrics = findNotReadyFabrics();
      for (String fabric: notReadyFabrics) {
        if (!destChildControllerClientMap.containsKey(fabric)) {
          LOGGER.error("Parent controller does not know the controller url or d2 of {}.", fabric);
          throw new VeniceException("Parent controller does not know the controller url or d2 of " + fabric);
        } else {
          ControllerClient destChildController = destChildControllerClientMap.get(fabric);
          ControllerClient srcChildController = srcChildControllerClientMap.get(fabric);
          if (isClonedStoreOnline(srcChildController, destChildController, record)) {
            fabricReadyMap.put(fabric, true);
            LOGGER.info(
                "Store {} is ready in the destination cluster: {} on fabric: {}.",
                record.getStoreName(),
                record.getDestinationCluster(),
                fabric);
          } else {
            LOGGER.debug(
                "Store {} is not ready in the destination cluster: {} on fabric: {}.",
                record.getStoreName(),
                record.getDestinationCluster(),
                fabric);
          }
        }
      }
      if (findNotReadyFabrics().isEmpty()) {
        statusVerified = true;
      }
    }

    if (statusVerified) {
      record.setCurrentStep(Step.UPDATE_CLUSTER_DISCOVERY);
      record.resetAttempts();
      manager.scheduleNextStep(this, 1);
    } else if (isTimeout()) {
      handleMigrationFailure();
    } else {
      manager.scheduleNextStep(this, this.manager.getDelayInSeconds());
    }
  }

  private void updateClusterDiscovery() {
    if (applyPauseIfNeeded(record)) {
      // If the migration is paused, do not proceed further
      manager.migrationTasks.put(record.getStoreName(), this);
      return;
    }
    LOGGER.info("Updating cluster discovery for migration of store {}...", record.getStoreName());
    boolean updateSuccessful = false; // Placeholder for actual update result
    if (!statusVerified) {
      rescheduleStatusVerification();
      return;
    }

    // The fabricReadyMap acts as a safeguard to prevent completing migration in any non-ready region.
    List<String> readyFabrics = fabricReadyMap.entrySet()
        .stream()
        .filter(e -> Boolean.TRUE.equals(e.getValue())) // keep entries with false
        .map(Map.Entry::getKey) // take the key
        .collect(Collectors.toList());
    ChildAwareResponse response = destControllerClient.listChildControllers(record.getDestinationCluster());
    boolean isSingleDC = false;
    if (readyFabrics.isEmpty() && response.getChildDataCenterControllerUrlMap() == null
        && response.getChildDataCenterControllerD2Map() == null) {
      // This is a controller in single datacenter setup
      isSingleDC = true;
      srcControllerClient.completeMigration(record.getStoreName(), record.getDestinationCluster());
      LOGGER.info(
          "Complete migration on single datacenter setup for store: {} on destination cluster: {}",
          record.getStoreName(),
          record.getDestinationCluster());
    } else {
      for (String fabric: readyFabrics) {
        if (!destChildControllerClientMap.containsKey(fabric)) {
          LOGGER.error(
              "Can't complete migration for store {}, parent controller does not know the controller url or d2 of {}",
              record.getCurrentStep(),
              fabric);
          throw new VeniceException(
              "Can't complete migration for store " + record.getStoreName()
                  + " parent controller does not know the controller url or d2 of " + fabric);
        } else {
          ControllerClient destChildController = destChildControllerClientMap.get(fabric);
          ControllerClient srcChildController = srcChildControllerClientMap.get(fabric);
          if (destChildController.discoverCluster(record.getStoreName())
              .getCluster()
              .equals(record.getDestinationCluster())) {
            LOGGER.info(
                "Store {} is ready in the destination cluster: {}, fabric: {}",
                record.getStoreName(),
                record.getDestinationCluster(),
                fabric);
          } else {
            StoreMigrationResponse storeMigrationResponse =
                srcChildController.completeMigration(record.getStoreName(), record.getDestinationCluster());
            if (storeMigrationResponse.isError()) {
              LOGGER.error(
                  "Store migration failed for store: {} on complete migration step {} with error: {} on fabric: {}",
                  record.getStoreName(),
                  record.getCurrentStepEnum(),
                  storeMigrationResponse.getError(),
                  fabric);
              throw new VeniceException(
                  "Store migration failed on complete migration for store: " + record.getStoreName() + " on fabric: "
                      + fabric + " with error: " + storeMigrationResponse.getError());
            }
          }
        }
      }
    }
    updateSuccessful = true;
    if (!isSingleDC) {
      for (String fabric: readyFabrics) {
        ControllerClient destChildController = destChildControllerClientMap.get(fabric);
        if (!destChildController.discoverCluster(record.getStoreName())
            .getCluster()
            .equals(record.getDestinationCluster())) {
          LOGGER.info(
              "Store {} is not ready in the destination cluster: {}, fabric: {}, will retry later.",
              record.getStoreName(),
              record.getDestinationCluster(),
              fabric);
          updateSuccessful = false;
          break;
        }
      }
      if (updateSuccessful) {
        srcControllerClient.completeMigration(record.getStoreName(), record.getDestinationCluster());
      }
    }

    if (updateSuccessful && destControllerClient.discoverCluster(record.getStoreName())
        .getCluster()
        .equals(record.getDestinationCluster())) {
      LOGGER.info(
          "Store {} is discoverable in the destination cluster: {} and is ready for verify read redirection.",
          record.getStoreName(),
          record.getDestinationCluster());
    } else {
      LOGGER.info(
          "Store {} is not discoverable in the destination cluster: {}, will retry later.",
          record.getStoreName(),
          record.getDestinationCluster());
      updateSuccessful = false;
    }

    if (updateSuccessful) {
      record.setCurrentStep(Step.VERIFY_READ_REDIRECTION);
      record.resetAttempts();
      manager.scheduleNextStep(this, 1);
    } else {
      record.incrementAttempts();
      if (record.getAttempts() > manager.getMaxRetryAttempts()) {
        handleMigrationFailure();
      } else {
        manager.scheduleNextStep(this, this.manager.getDelayInSeconds());
      }
    }
  }

  private void verifyReadRedirection() {
    if (applyPauseIfNeeded(record)) {
      // If the migration is paused, do not proceed further
      manager.migrationTasks.put(record.getStoreName(), this);
      return;
    }
    LOGGER.info("Verifying read redirection for migration of store {}...", record.getStoreName());
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
      manager.scheduleNextStep(this, this.manager.getDelayInSeconds());
    }
  }

  private void endMigration() {
    if (applyPauseIfNeeded(record)) {
      // If the migration is paused, do not proceed further
      manager.migrationTasks.put(record.getStoreName(), this);
      return;
    }
    LOGGER.info("Ending migration for store {}...", record.getStoreName());
    String clusterDisc = destControllerClient.discoverCluster(record.getStoreName()).getCluster();
    if (!clusterDisc.equals(record.getDestinationCluster())) {
      LOGGER.error(
          "Store {} is not discoverable in the destination cluster: {} but in: {}, can't end migration.",
          record.getStoreName(),
          record.getDestinationCluster(),
          clusterDisc);
      throw new VeniceException(
          "Store " + record.getStoreName() + "is not discoverable in the destination cluster: "
              + record.getDestinationCluster() + " but in: " + clusterDisc + " on step " + record.getCurrentStepEnum());
    }
    // Verify that original store is deleted in the source cluster
    StoreResponse srcStoreResponse = srcControllerClient.getStore(record.getStoreName());
    if (srcStoreResponse.isError() && srcStoreResponse.getErrorType() != ErrorType.STORE_NOT_FOUND) {
      LOGGER.error(
          "Failed to check store {} existence in original cluster {} on step {} due to error: {} ",
          record.getStoreName(),
          record.getSourceCluster(),
          record.getCurrentStep(),
          srcStoreResponse.getError());
    }

    if (srcStoreResponse.getErrorType() != ErrorType.STORE_NOT_FOUND) {
      // Delete original store
      srcControllerClient.updateStore(
          record.getStoreName(),
          new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
      TrackableControllerResponse deleteResponse = srcControllerClient.deleteStore(record.getStoreName(), true);
      if (deleteResponse.isError()) {
        LOGGER.error(
            "Failed to delete store {} in the original cluster {} due to error: {}",
            record.getStoreName(),
            record.getSourceCluster(),
            deleteResponse.getError());
        throw new VeniceException(
            "Failed to delete store " + record.getStoreName() + " in the original cluster " + record.getSourceCluster()
                + " on step " + record.getCurrentStepEnum() + " due to error: " + deleteResponse.getError());
      }
    }
    // Verify that the original store is deleted in all child fabrics
    for (Map.Entry<String, ControllerClient> entry: srcChildControllerClientMap.entrySet()) {
      StoreResponse childSrcStoreResponse = entry.getValue().getStore(record.getStoreName());
      if (childSrcStoreResponse.isError() && childSrcStoreResponse.getErrorType() != ErrorType.STORE_NOT_FOUND) {
        LOGGER.error(
            "Failed to check store {} existence in original cluster {} in fabric {} due to error: {}",
            record.getStoreName(),
            record.getSourceCluster(),
            entry.getKey(),
            childSrcStoreResponse.getError());
        throw new VeniceException(
            "Failed to check store " + record.getStoreName() + " existence in original cluster "
                + record.getSourceCluster() + " in fabric " + entry.getKey() + " on step " + record.getCurrentStepEnum()
                + " due to error: " + childSrcStoreResponse.getError());
      }
      if (childSrcStoreResponse.getErrorType() != ErrorType.STORE_NOT_FOUND) {
        LOGGER.error(
            "Store {} still exists in source cluster {} in fabric {} after migration. Please try again later.",
            record.getStoreName(),
            record.getSourceCluster(),
            entry.getKey());
        throw new VeniceException(
            "Store " + record.getStoreName() + " still exists in source cluster " + record.getSourceCluster()
                + " in fabric " + entry.getKey() + " on step " + record.getCurrentStepEnum()
                + " after migration. StoreResponse on fabric  " + entry.getKey() + " of source cluster is "
                + childSrcStoreResponse + " . Please try again later.");
      }
    }

    // Reset the migration flags
    LOGGER.info(
        "Original store does not exist in source cluster {} . Resetting migration flags for store: {}",
        record.getSourceCluster(),
        record.getStoreName());
    ControllerResponse resetMigrationFlagResponse = destControllerClient.updateStore(
        record.getStoreName(),
        new UpdateStoreQueryParams().setStoreMigration(false).setMigrationDuplicateStore(false));
    if (resetMigrationFlagResponse.isError()) {
      LOGGER.error(
          "Failed to reset migration flags for store {} in destination cluster {} due to error: {}",
          record.getStoreName(),
          record.getDestinationCluster(),
          resetMigrationFlagResponse.getError());
      throw new VeniceException(
          "Failed to reset migration flags for store " + record.getStoreName() + " in destination cluster "
              + record.getDestinationCluster() + " on step " + record.getCurrentStepEnum() + " due to error: "
              + resetMigrationFlagResponse.getError());
    }
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
    if (record.getAbortOnFailure()) {
      if (record.getCurrentStepEnum().compareTo(Step.PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST) <= 0) {
        // If the migration is not started yet, just clean up the record
        LOGGER.info(
            "Migration for store {} is not started yet on step {}. Just cleaning up the record.",
            record.getStoreName(),
            record.getCurrentStepEnum());
        manager.cleanupMigrationRecord(record.getStoreName());
        return;
      }
      LOGGER.info("Start aborting migration for store: {}", record.getStoreName());

      if (record.getCurrentStepEnum().compareTo(Step.END_MIGRATION) >= 0) {
        LOGGER.error(
            "Cannot abort migration for store {} in step {}. Since the migration step is already end migration",
            record.getCurrentStepEnum(),
            record.getStoreName());
        throw new VeniceException(
            "Migration for store " + record.getStoreName() + " on step " + record.getCurrentStepEnum()
                + ". Cannot abort migration on or after end migration.");
      }
      if (srcControllerClient.getStore(record.getStoreName()).getStore() != null) {
        StoreInfo srcStoreInfo = srcControllerClient.getStore(record.getStoreName()).getStore();
        // If the store isMigrating flag is false, abort migrating can be risky
        if (!srcStoreInfo.isMigrating()) {
          LOGGER.info(
              "Store {} is not in migration state in source cluster {}. Cannot abort migration on step {}.",
              record.getStoreName(),
              record.getSourceCluster(),
              record.getCurrentStepEnum());
          throw new VeniceException(
              "Store " + record.getStoreName() + " is not in migration state on source cluster "
                  + record.getSourceCluster() + " on step " + record.getCurrentStepEnum()
                  + ". Cannot abort migration.");
        }
        D2ServiceDiscoveryResponse discoveryResponse = srcControllerClient.discoverCluster(record.getStoreName());
        if (!discoveryResponse.getCluster().equals(record.getSourceCluster())) {
          throw new VeniceException(
              "Store " + record.getStoreName() + " discovered on cluster " + discoveryResponse.getCluster()
                  + " instead of source cluster " + record.getSourceCluster()
                  + ". Either store migration has completed, or the internal states are messed up.");
        }
        // Reset migration flag and store config and force update cluster discovery to the source cluster.
        StoreMigrationResponse abortMigrationResponse =
            srcControllerClient.abortMigration(record.getStoreName(), record.getDestinationCluster());
        if (abortMigrationResponse.isError()) {
          LOGGER.error(
              "Failed to abort migration for store {} in destination cluster {} due to error: {}",
              record.getStoreName(),
              record.getDestinationCluster(),
              abortMigrationResponse.getError());
          throw new VeniceException(
              "Failed to abort migration for store " + record.getStoreName() + " in destination cluster "
                  + record.getDestinationCluster() + " on step " + record.getCurrentStepEnum() + " due to error: "
                  + abortMigrationResponse.getError());
        } else {
          LOGGER.info(
              "Successfully aborted migration for store {} on source cluster {}",
              record.getStoreName(),
              record.getSourceCluster());
        }

        discoveryResponse = srcControllerClient.discoverCluster(record.getStoreName());
        if (!discoveryResponse.getCluster().equals(record.getSourceCluster())) {
          LOGGER.error(
              "Aborted migration error, store {} is not discoverable in source cluster {} after abort migration. "
                  + "Incorrect cluster discovery: {} .",
              record.getStoreName(),
              record.getSourceCluster(),
              discoveryResponse.getCluster());
          throw new VeniceException(
              "Store" + record.getStoreName() + " is not discoverable in source cluster" + record.getSourceCluster()
                  + " after aborting migration. Please retry again later.");
        }
        // Delete store on destination cluster
        if (destControllerClient.getStore(record.getStoreName()) != null) {
          LOGGER.info(
              "Deleting cloned store {} in destination cluster {} after aborting migration in {} ",
              record.getStoreName(),
              record.getDestinationCluster(),
              destControllerClient.getLeaderControllerUrl());
          destControllerClient.updateStore(
              record.getStoreName(),
              new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
          TrackableControllerResponse deleteResponse = destControllerClient.deleteStore(record.getStoreName(), true);
          if (deleteResponse.isError()) {
            LOGGER.error(
                "Failed to delete store {} in the destination cluster {} after aborting migration due to error: {}",
                record.getStoreName(),
                record.getDestinationCluster(),
                deleteResponse.getError());
            throw new VeniceException(
                "Failed to delete store " + record.getStoreName() + " in the destination cluster "
                    + record.getDestinationCluster() + " on step " + record.getCurrentStepEnum() + " due to error: "
                    + deleteResponse.getError());
          }
        } else {
          LOGGER.info(
              "Store {} does not exist in destination cluster {} after aborting migration. No need to delete.",
              record.getStoreName(),
              record.getDestinationCluster());
        }
      } else {
        LOGGER.error(
            "Store {} does not exist in source cluster {}. Please verify store status use --migration-status.",
            record.getStoreName(),
            record.getSourceCluster());
      }
      record.setIsAborted(true);
    }
    manager.cleanupMigrationRecord(record.getStoreName());
  }

  private void rescheduleStatusVerification() {
    LOGGER.warn(
        "Store {} is not verified replicated in destination cluster {}. Skipping cluster discovery update and rescheduling verification.",
        record.getStoreName(),
        record.getDestinationCluster());
    record.setCurrentStep(Step.VERIFY_MIGRATION_STATUS);
    manager.scheduleNextStep(this, this.manager.getDelayInSeconds());
  }

  private List<String> findNotReadyFabrics() {
    return fabricReadyMap.entrySet()
        .stream()
        .filter(e -> Boolean.FALSE.equals(e.getValue())) // keep entries with false
        .map(Map.Entry::getKey) // take the key
        .collect(Collectors.toList());
  }
}
