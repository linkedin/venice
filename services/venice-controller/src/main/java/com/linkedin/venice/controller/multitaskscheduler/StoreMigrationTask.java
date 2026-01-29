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
  private static final int MAX_PROPAGATION_CHECK_INTERVAL_MS = 10000; // Max 10 seconds between checks
  private static final int MIN_PROPAGATION_CHECK_INTERVAL_MS = 500; // Min 0.5 seconds for fast tests
  private ControllerClient srcControllerClient, destControllerClient;
  Map<String, ControllerClient> srcChildControllerClientMap, destChildControllerClientMap;
  private final Map<String, Boolean> fabricReadyMap = new HashMap<>();
  boolean statusVerified = false;
  // Tracks if source store is in fully operational state (both reads AND writes enabled)
  // Used to optimize updates by skipping redundant operations
  private boolean srcStoreFullyReadWriteEnabled = true;

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
    fabricList.forEach(fabric -> fabricReadyMap.put(fabric, false));
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
      LOGGER.error(
          "Error during migration for store: {} on step: {}",
          record.getStoreName(),
          record.getCurrentStepEnum(),
          e);
      if (record.getAttempts() < manager.getMaxRetryAttempts()) {
        record.incrementAttempts();
        LOGGER.info(
            "Retry {} < {} for store {}, rescheduling",
            record.getAttempts(),
            manager.getMaxRetryAttempts(),
            record.getStoreName());
        manager.scheduleNextStep(this, manager.getDelayInSeconds());
      } else {
        LOGGER.warn(
            "Max retries {} reached for store {}, aborting",
            manager.getMaxRetryAttempts(),
            record.getStoreName());
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
    if (handlePauseIfNeeded())
      return;
    LOGGER.info("Submit migration request of store {}...", record.getStoreName());
    StoreInfo srcStoreInfo = srcControllerClient.getStore(record.getStoreName()).getStore();
    if (!srcStoreInfo.isMigrating()) { // Implement pre-check
      StoreMigrationResponse response =
          srcControllerClient.migrateStore(record.getStoreName(), record.getDestinationCluster());
      if (response.isError()) {
        throw new VeniceException(
            "Store migration failed for " + record.getStoreName() + " on step " + record.getCurrentStepEnum() + ": "
                + response.getError());
      }
    }
    advanceToNextStep(Step.VERIFY_MIGRATION_STATUS, manager.getDelayInSeconds());
  }

  private void verifyMigrationStatus() {
    if (handlePauseIfNeeded())
      return;
    LOGGER.info("Verifying migration status for store {}...", record.getStoreName());
    if (Instant.ofEpochMilli(-1).equals(record.getStoreMigrationStartTime())) {
      record.setStoreMigrationStartTime(Instant.now()); // Migration start time is unset yet, use current time
    }

    // This is a controller in single datacenter setup
    if (srcChildControllerClientMap.isEmpty() && destChildControllerClientMap.isEmpty()) {
      statusVerified = isClonedStoreOnline(srcControllerClient, destControllerClient, record);
      if (statusVerified) {
        LOGGER.info(
            "Store {} is ready in destination cluster {} in the single datacenter setup.",
            record.getStoreName(),
            record.getDestinationCluster());
      } else {
        String logKey = record.getStoreName() + "_" + record.getDestinationCluster();
        if (!manager.filter.isRedundantException(logKey)) {
          LOGGER.info(
              "Store {} is not ready in destination cluster {} in the single datacenter setup, retry later.",
              record.getStoreName(),
              record.getDestinationCluster());
        }
      }
    } else {
      verifyMultiDatacenterStatus();
    }

    if (statusVerified) {
      advanceToNextStep(Step.UPDATE_CLUSTER_DISCOVERY, 1);
    } else if (isTimeout()) {
      handleMigrationFailure();
    } else {
      manager.scheduleNextStep(this, manager.getDelayInSeconds());
    }
  }

  private void verifyMultiDatacenterStatus() {
    List<String> notReadyFabrics = findNotReadyFabrics();
    for (String fabric: notReadyFabrics) {
      if (!destChildControllerClientMap.containsKey(fabric)) {
        throw new VeniceException("Parent controller missing URL/D2 for fabric " + fabric);
      }
      if (isClonedStoreOnline(
          srcChildControllerClientMap.get(fabric),
          destChildControllerClientMap.get(fabric),
          record)) {
        fabricReadyMap.put(fabric, true);
        LOGGER.info(
            "Store {} is ready in cluster {} fabric {}",
            record.getStoreName(),
            record.getDestinationCluster(),
            fabric);
      } else {
        String logKey = record.getStoreName() + "_" + record.getDestinationCluster();
        if (!manager.filter.isRedundantException(logKey)) {
          LOGGER.info(
              "Store {} is not ready in destination cluster {} fabric {}, retry later",
              record.getStoreName(),
              record.getDestinationCluster());
        }
      }
    }
    statusVerified = findNotReadyFabrics().isEmpty();
  }

  private void updateClusterDiscovery() {
    if (handlePauseIfNeeded())
      return;
    if (!statusVerified) {
      rescheduleStatusVerification();
      return;
    }
    LOGGER.info("Updating cluster discovery for store {}...", record.getStoreName());

    // The fabricReadyMap acts as a safeguard to prevent completing migration in any non-ready region.
    // It is guaranteed to include all regions specified in the STORE_MIGRATION_FABRIC_LIST configuration.
    List<String> readyFabrics = fabricReadyMap.entrySet()
        .stream()
        .filter(e -> Boolean.TRUE.equals(e.getValue()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    ChildAwareResponse childAwareResponse = destControllerClient.listChildControllers(record.getDestinationCluster());
    boolean isSingleDC = readyFabrics.isEmpty() && childAwareResponse.getChildDataCenterControllerUrlMap() == null
        && childAwareResponse.getChildDataCenterControllerD2Map() == null;

    boolean updateSuccessful = true;
    if (isSingleDC) {
      srcControllerClient.completeMigration(record.getStoreName(), record.getDestinationCluster());
      LOGGER.info(
          "Completed single DC migration for store {} to cluster {}",
          record.getStoreName(),
          record.getDestinationCluster());
    } else {
      updateSuccessful = completeMultiDatacenterMigration(readyFabrics);
    }

    updateSuccessful = updateSuccessful && destControllerClient.discoverCluster(record.getStoreName())
        .getCluster()
        .equals(record.getDestinationCluster());

    if (updateSuccessful) {
      LOGGER.info(
          "Store {} discoverable in destination cluster {}",
          record.getStoreName(),
          record.getDestinationCluster());
      advanceToNextStep(Step.VERIFY_READ_REDIRECTION, 1);
    } else {
      LOGGER.info(
          "Store {} not discoverable in destination cluster {}, retry later",
          record.getStoreName(),
          record.getDestinationCluster());
      retryOrFail();
    }
  }

  private boolean completeMultiDatacenterMigration(List<String> readyFabrics) {
    // Complete migration for each fabric
    for (String fabric: readyFabrics) {
      if (!destChildControllerClientMap.containsKey(fabric)) {
        throw new VeniceException("Missing url/d2 for fabric " + fabric + " for store " + record.getStoreName());
      }
      ControllerClient destChild = destChildControllerClientMap.get(fabric);
      if (!destChild.discoverCluster(record.getStoreName()).getCluster().equals(record.getDestinationCluster())) {
        StoreMigrationResponse response = srcChildControllerClientMap.get(fabric)
            .completeMigration(record.getStoreName(), record.getDestinationCluster());
        if (response.isError()) {
          throw new VeniceException(
              "Migration completion failed for store " + record.getStoreName() + " on fabric " + fabric + ": "
                  + response.getError());
        }
        LOGGER.info(
            "Completed migration for store {} to cluster {} on fabric {}",
            record.getStoreName(),
            record.getDestinationCluster(),
            fabric);
      }
    }
    // Verify all fabrics are ready
    for (String fabric: readyFabrics) {
      if (!destChildControllerClientMap.get(fabric)
          .discoverCluster(record.getStoreName())
          .getCluster()
          .equals(record.getDestinationCluster())) {
        return false;
      }
    }
    srcControllerClient.completeMigration(record.getStoreName(), record.getDestinationCluster());
    return true;
  }

  private void verifyReadRedirection() {
    if (handlePauseIfNeeded())
      return;
    LOGGER.info("Verifying read redirection for store {}...", record.getStoreName());
    boolean verified = false; // Placeholder - implement actual verification logic

    // Replace with actual logic
    verified = true; // Simulate a successful update for integration testing
    if (verified) {
      advanceToNextStep(Step.END_MIGRATION, manager.getDelayInSeconds());
    } else {
      retryOrFail();
    }
  }

  private void retryOrFail() {
    record.incrementAttempts();
    if (record.getAttempts() > manager.getMaxRetryAttempts()) {
      handleMigrationFailure();
    } else {
      manager.scheduleNextStep(this, manager.getDelayInSeconds());
    }
  }

  private void endMigration() {
    if (handlePauseIfNeeded())
      return;
    LOGGER.info("Ending migration for store {}...", record.getStoreName());

    verifyStoreInDestinationCluster();
    deleteSourceStoreIfExists();
    verifyChildFabricStoresDeleted();
    resetMigrationFlags();

    record.setCurrentStep(Step.MIGRATION_SUCCEED);
    manager.cleanupMigrationRecord(record.getStoreName());
  }

  private void verifyStoreInDestinationCluster() {
    String actualCluster = destControllerClient.discoverCluster(record.getStoreName()).getCluster();
    if (!actualCluster.equals(record.getDestinationCluster())) {
      throw new VeniceException(
          "Store " + record.getStoreName() + " not in destination cluster " + record.getDestinationCluster()
              + " but in " + actualCluster);
    }
  }

  private void deleteSourceStoreIfExists() {
    StoreResponse srcStoreResponse = srcControllerClient.getStore(record.getStoreName());
    if (srcStoreResponse.isError() && srcStoreResponse.getErrorType() != ErrorType.STORE_NOT_FOUND) {
      LOGGER.error(
          "Failed to check store {} in source cluster {}: {}",
          record.getStoreName(),
          record.getSourceCluster(),
          srcStoreResponse.getError());
    }

    if (srcStoreResponse.getErrorType() != ErrorType.STORE_NOT_FOUND) {
      StoreInfo store = srcStoreResponse.getStore();
      boolean needsDisabling = store.isEnableStoreReads() || store.isEnableStoreWrites();
      if (needsDisabling && !disableSrcStoreReadWrite()) {
        throw new VeniceException(
            "Failed to disable reads/writes for store " + record.getStoreName() + " in cluster "
                + record.getSourceCluster());
      }

      TrackableControllerResponse deleteResponse = srcControllerClient.deleteStore(record.getStoreName(), true);
      if (deleteResponse.isError()) {
        throw new VeniceException(
            "Failed to delete store " + record.getStoreName() + " from source cluster " + record.getSourceCluster()
                + ": " + deleteResponse.getError());
      }
    }
  }

  private void verifyChildFabricStoresDeleted() {
    for (Map.Entry<String, ControllerClient> entry: srcChildControllerClientMap.entrySet()) {
      String fabric = entry.getKey();
      StoreResponse response = entry.getValue().getStore(record.getStoreName());

      if (response.isError() && response.getErrorType() != ErrorType.STORE_NOT_FOUND) {
        throw new VeniceException(
            "Failed to check store " + record.getStoreName() + " in fabric " + fabric + ": " + response.getError());
      }
      if (response.getErrorType() != ErrorType.STORE_NOT_FOUND) {
        throw new VeniceException(
            "Store " + record.getStoreName() + " still exists in source cluster fabric " + fabric + " after migration");
      }
    }
  }

  private void resetMigrationFlags() {
    LOGGER.info("Resetting migration flags for store {}", record.getStoreName());
    ControllerResponse response = destControllerClient.updateStore(
        record.getStoreName(),
        new UpdateStoreQueryParams().setStoreMigration(false).setMigrationDuplicateStore(false));
    if (response.isError()) {
      throw new VeniceException(
          "Failed to reset migration flags for store " + record.getStoreName() + " in destination cluster: "
              + response.getError());
    }
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
        LOGGER.info("Migration not started for store {}, cleaning up migration record", record.getStoreName());
        manager.cleanupMigrationRecord(record.getStoreName());
        return;
      }
      if (record.getCurrentStepEnum().compareTo(Step.END_MIGRATION) >= 0) {
        throw new VeniceException(
            "Cannot abort migration for store " + record.getStoreName() + " at step " + record.getCurrentStepEnum()
                + " - too late to abort");
      }

      abortMigrationProcess();
      record.setIsAborted(true);
    }
    manager.cleanupMigrationRecord(record.getStoreName());
  }

  private void abortMigrationProcess() {
    LOGGER.info("Aborting migration for store {}", record.getStoreName());
    StoreInfo srcStoreInfo = srcControllerClient.getStore(record.getStoreName()).getStore();
    if (srcStoreInfo == null) {
      LOGGER.error("Store {} missing from source cluster {}", record.getStoreName(), record.getSourceCluster());
      return;
    }

    if (!srcStoreInfo.isMigrating()) {
      throw new VeniceException(
          "Store " + record.getStoreName() + " not in migrating state, cannot abort at step "
              + record.getCurrentStepEnum());
    }

    D2ServiceDiscoveryResponse discovery = srcControllerClient.discoverCluster(record.getStoreName());
    if (!discovery.getCluster().equals(record.getSourceCluster())) {
      throw new VeniceException(
          "Store " + record.getStoreName() + " discovered in cluster " + discovery.getCluster() + " instead of source "
              + record.getSourceCluster() + ". Either migration has completed, or internal states are messed up.");
    }

    StoreMigrationResponse abortResponse =
        srcControllerClient.abortMigration(record.getStoreName(), record.getDestinationCluster());
    if (abortResponse.isError()) {
      throw new VeniceException(
          "Failed to abort migration for store " + record.getStoreName() + ": " + abortResponse.getError());
    }
    LOGGER.info("Abort migration request submitted successfully for store {}", record.getStoreName());

    // Verify abort succeeded
    discovery = srcControllerClient.discoverCluster(record.getStoreName());
    if (!discovery.getCluster().equals(record.getSourceCluster())) {
      throw new VeniceException("Store " + record.getStoreName() + " not restored to source cluster after abort");
    }

    deleteDestinationStoreAfterAbort();
    LOGGER.info("Successfully aborted migration for store {}", record.getStoreName());
  }

  private void deleteDestinationStoreAfterAbort() {
    if (destControllerClient.getStore(record.getStoreName()) != null) {
      LOGGER.info(
          "Deleting cloned store {} from destination cluster {} after abort.",
          record.getStoreName(),
          record.getDestinationCluster());
      destControllerClient.updateStore(
          record.getStoreName(),
          new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
      TrackableControllerResponse deleteResponse = destControllerClient.deleteStore(record.getStoreName(), true);
      if (deleteResponse.isError()) {
        throw new VeniceException(
            "Failed to delete store " + record.getStoreName() + " from destination cluster after abort: "
                + deleteResponse.getError());
      }
    }
  }

  private void rescheduleStatusVerification() {
    LOGGER.warn(
        "Store {} not verified in destination cluster {}, rescheduling verification",
        record.getStoreName(),
        record.getDestinationCluster());
    advanceToNextStep(Step.VERIFY_MIGRATION_STATUS, manager.getDelayInSeconds());
  }

  private boolean handlePauseIfNeeded() {
    if (applyPauseIfNeeded(record)) {
      manager.migrationTasks.put(record.getStoreName(), this);
      return true;
    }
    return false;
  }

  private void advanceToNextStep(Step nextStep, int delaySeconds) {
    record.setCurrentStep(nextStep);
    record.resetAttempts();
    manager.scheduleNextStep(this, delaySeconds);
  }

  private List<String> findNotReadyFabrics() {
    if (fabricReadyMap.isEmpty()) {
      throw new IllegalArgumentException("fabricReadyMap must not be empty for non-single datacenter setup.");
    }
    return fabricReadyMap.entrySet()
        .stream()
        .filter(e -> Boolean.FALSE.equals(e.getValue())) // keep entries with false
        .map(Map.Entry::getKey) // take the key
        .collect(Collectors.toList());
  }

  private boolean disableSrcStoreReadWrite() {
    if (!srcStoreFullyReadWriteEnabled) { // Skip update if store is already disabled
      return true;
    }
    LOGGER.info(
        "Disabling reads and writes for store {} in cluster {}",
        record.getStoreName(),
        record.getSourceCluster());
    ControllerResponse response = srcControllerClient
        .updateStore(record.getStoreName(), new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
    if (response.isError()) {
      LOGGER.error(
          "Failed to disable reads/writes for store {} in cluster {}: {}",
          record.getStoreName(),
          record.getSourceCluster(),
          response.getError());
      return false;
    }

    // Wait for configuration to propagate to store metadata
    for (int i = 0; i < 10; i++) {
      StoreInfo storeInfo = srcControllerClient.getStore(record.getStoreName()).getStore();
      // Check if both reads and writes are now disabled
      if (!storeInfo.isEnableStoreReads() && !storeInfo.isEnableStoreWrites()) {
        srcStoreFullyReadWriteEnabled = false;
        LOGGER.info(
            "Successfully disabled reads and writes for store {} in cluster {}",
            record.getStoreName(),
            record.getSourceCluster());
        return true;
      }

      try {
        int sleepMs = Math.max(
            MIN_PROPAGATION_CHECK_INTERVAL_MS,
            Math.min(MAX_PROPAGATION_CHECK_INTERVAL_MS, manager.getDelayInSeconds() * 1000 / 3));
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    LOGGER.error(
        "Timeout waiting for reads/writes to be disabled on store {} cluster {}",
        record.getStoreName(),
        record.getSourceCluster());
    return false;
  }
}
