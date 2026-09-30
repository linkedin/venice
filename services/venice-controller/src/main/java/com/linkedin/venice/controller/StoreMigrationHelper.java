package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.schema.SchemaEntry;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.Logger;


/**
 * Cross-cluster store-migration plumbing: RPCs against the destination controller to create
 * the cloned store, register its value schemas, and replicate per-region store-config overrides.
 */
final class StoreMigrationHelper {
  private StoreMigrationHelper() {
  }

  static void validateEncryptionClusterMigration(
      boolean srcEncryptionCluster,
      boolean destEncryptionCluster,
      String srcClusterName,
      String destClusterName,
      String storeName) {
    if (srcEncryptionCluster != destEncryptionCluster) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Cannot migrate store " + storeName + " from cluster " + srcClusterName + " to cluster " + destClusterName
              + " because migrating between an encryption cluster and a non-encryption cluster is not allowed.",
          ErrorType.BAD_REQUEST);
    }
  }

  /**
   * The destination store is created with its own {@code encryptionEnabled} flag derived from the destination
   * cluster's current policy, independent of the source store's (possibly stale) flag. So a store created before
   * its source cluster became an encryption cluster can have no pubSubEncryptionKeyUrn, yet still pass
   * {@link #validateEncryptionClusterMigration} once both clusters are encryption clusters. Migrating it as-is
   * would create a destination store that is encryption-enabled with no key, which only fails later when a new
   * version is requested. Fail fast here instead, before any migration state changes.
   *
   * <p>{@code encryptionEnabled} is only ever set at store-creation time and cannot be changed afterward.
   * {@code update-store} can set an initial key for a store where it is already true (a valid remedy), but not
   * for a legacy store where it is false; the message is conditioned on that so it never prescribes a fix that
   * would be rejected.
   */
  static void validateSourceStoreEncryptionKeyForMigration(
      boolean destEncryptionCluster,
      boolean srcStoreEncryptionEnabled,
      String srcStorePubSubEncryptionKeyUrn,
      String srcClusterName,
      String destClusterName,
      String storeName) {
    if (destEncryptionCluster && StringUtils.isBlank(srcStorePubSubEncryptionKeyUrn)) {
      String remedy = srcStoreEncryptionEnabled
          ? "The store is encryption-enabled but has no pubSubEncryptionKeyUrn configured yet; set one via "
              + "update-store, then retry the migration."
          : "Its encryptionEnabled flag is set once at store creation and cannot be changed afterward, so "
              + "update-store cannot set a key for it; this store cannot be migrated into an encryption cluster.";
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Cannot migrate store " + storeName + " from cluster " + srcClusterName + " to cluster " + destClusterName
              + " because the store does not have a pubSubEncryptionKeyUrn configured. " + remedy,
          ErrorType.BAD_REQUEST);
    }
  }

  static void cloneDestinationStoreAndSyncConfigs(
      ControllerClient destControllerClient,
      StoreInfo srcStore,
      String keySchema,
      List<SchemaEntry> valueSchemaEntries,
      Map<String, Map<String, StoreInfo>> srcStoresInChildColos,
      String destClusterName,
      String storeName,
      String localRegion,
      Logger logger) {
    NewStoreResponse newStoreResponse = destControllerClient
        .createNewStore(storeName, srcStore.getOwner(), keySchema, valueSchemaEntries.get(0).getSchema().toString());
    if (newStoreResponse.isError()) {
      throw new VeniceException(
          "Failed to create store " + storeName + " in dest cluster " + destClusterName + ". Error "
              + newStoreResponse.getError());
    }

    for (SchemaEntry schemaEntry: valueSchemaEntries) {
      SchemaResponse schemaResponse =
          destControllerClient.addValueSchema(storeName, schemaEntry.getSchema().toString());
      if (schemaResponse.isError()) {
        throw new VeniceException(
            "Failed to add value schema " + schemaEntry.getId() + " into store " + storeName + " in dest cluster "
                + destClusterName + ". Error " + schemaResponse.getError());
      }
    }

    UpdateStoreQueryParams params = new UpdateStoreQueryParams(srcStore, true);
    Set<String> remainingRegions = new HashSet<>();
    remainingRegions.add(localRegion);
    for (Map.Entry<String, StoreInfo> entry: srcStoresInChildColos.get(storeName).entrySet()) {
      UpdateStoreQueryParams paramsInChildColo = new UpdateStoreQueryParams(entry.getValue(), true);
      if (params.isDifferent(paramsInChildColo)) {
        paramsInChildColo.setRegionsFilter(entry.getKey());
        logger.info("Sending update-store request {} to store {} in {}", paramsInChildColo, storeName, entry.getKey());
        ControllerResponse updateStoreResponse = destControllerClient.updateStore(storeName, paramsInChildColo);
        if (updateStoreResponse.isError()) {
          throw new VeniceException(
              "Failed to update store " + storeName + " in dest cluster " + destClusterName + " in region "
                  + paramsInChildColo + ". Error " + updateStoreResponse.getError());
        }
      } else {
        remainingRegions.add(entry.getKey());
      }
    }

    params.setRegionsFilter(String.join(",", remainingRegions));
    logger.info("Sending update-store request {} to store {} in {}", params, storeName, remainingRegions);
    ControllerResponse updateStoreResponse = destControllerClient.updateStore(storeName, params);
    if (updateStoreResponse.isError()) {
      throw new VeniceException(
          "Failed to update store " + storeName + " in dest cluster " + destClusterName + " in regions "
              + remainingRegions + ". Error " + updateStoreResponse.getError());
    }
  }
}
