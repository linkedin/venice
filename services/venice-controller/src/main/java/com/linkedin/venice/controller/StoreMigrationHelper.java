package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.schema.SchemaEntry;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.Logger;


/**
 * Cross-cluster store-migration plumbing: RPCs against the destination controller to create
 * the cloned store, register its value schemas, and replicate per-region store-config overrides.
 */
final class StoreMigrationHelper {
  private StoreMigrationHelper() {
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
