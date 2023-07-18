package com.linkedin.venice.system.store;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Utils;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ControllerClientBackedSystemSchemaInitializer {
  private static final Logger LOGGER = LogManager.getLogger(ControllerClientBackedSystemSchemaInitializer.class);
  private static final String DEFAULT_KEY_SCHEMA_STR = "\"int\"";
  /**
   * Current leader controller might transit from leader to standby, clear cached store repository, and fail to handle
   * schema requests. Leverage 20 retries (38 seconds) to cover the leader->standby period so that a later retry will be
   * sent to the new leader controller, who can handle schema requests successfully.
   */
  private static final int DEFAULT_RETRY_TIMES = 20;

  private final AvroProtocolDefinition protocolDefinition;
  private final String clusterName;
  private final Schema keySchema;
  private final UpdateStoreQueryParams storeMetadataUpdate;
  private final boolean autoRegisterPartialUpdateSchema;
  private final Optional<SSLFactory> sslFactory;
  private final String controllerUrl;
  private final String controllerD2ServiceName;
  private final String d2ZkHost;
  private final boolean enforceSslOnly;
  private ControllerClient controllerClient;

  public ControllerClientBackedSystemSchemaInitializer(
      AvroProtocolDefinition protocolDefinition,
      String systemStoreCluster,
      Schema keySchema,
      UpdateStoreQueryParams storeMetadataUpdate,
      boolean autoRegisterPartialUpdateSchema,
      Optional<SSLFactory> sslFactory,
      String controllerUrl,
      String controllerD2ServiceName,
      String d2ZkHost,
      boolean enforceSslOnly) {
    this.protocolDefinition = protocolDefinition;
    this.clusterName = systemStoreCluster;
    this.keySchema = keySchema;
    this.storeMetadataUpdate = storeMetadataUpdate;
    this.autoRegisterPartialUpdateSchema = autoRegisterPartialUpdateSchema;
    this.sslFactory = sslFactory;
    this.controllerUrl = controllerUrl;
    this.controllerD2ServiceName = controllerD2ServiceName;
    this.d2ZkHost = d2ZkHost;
    this.enforceSslOnly = enforceSslOnly;
  }

  public void execute() {
    if (controllerClient == null) {
      if (!controllerUrl.isEmpty()) {
        controllerClient = ControllerClient.constructClusterControllerClient(clusterName, controllerUrl, sslFactory);
      } else if (!controllerD2ServiceName.isEmpty() && !d2ZkHost.isEmpty()) {
        controllerClient = new D2ControllerClient(
            controllerD2ServiceName,
            clusterName,
            d2ZkHost,
            enforceSslOnly ? sslFactory : Optional.empty());
      } else {
        throw new VeniceException(
            "System schema initialization is enabled but neither controller url nor d2 config is provided.");
      }
    }

    if (!hasLeaderController()) {
      LOGGER.warn(
          "Could not find leader controller after retries. It's very likely that the region does not have any live "
              + "controller yet. Skip system schema registration via controller client.");
      return;
    }

    String storeName = protocolDefinition.getSystemStoreName();
    Map<Integer, Schema> schemasInLocalResources = Utils.getAllSchemasFromResources(protocolDefinition);
    D2ServiceDiscoveryResponse discoveryResponse = controllerClient.retryableRequest(
        DEFAULT_RETRY_TIMES,
        c -> c.discoverCluster(storeName),
        r -> ErrorType.STORE_NOT_FOUND.equals(r.getErrorType()));
    if (discoveryResponse.isError()) {
      if (ErrorType.STORE_NOT_FOUND.equals(discoveryResponse.getErrorType())) {
        checkAndMayCreateSystemStore(storeName, schemasInLocalResources.get(1));
      } else {
        throw new VeniceException(
            "Error when discovering system store " + storeName + " after retries. Error: "
                + discoveryResponse.getError());
      }
    } else {
      String currSystemStoreCluster = discoveryResponse.getCluster();
      if (!currSystemStoreCluster.equals(clusterName)) {
        throw new VeniceException(
            "The system store for " + protocolDefinition.name() + " already exists in cluster " + currSystemStoreCluster
                + ", which is inconsistent with the config " + CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME
                + " which specifies that it should be in cluster " + clusterName
                + ". Cannot continue the initialization.");
      }
    }

    // Only verify the key schema if it is explicitly specified by the caller. We don't care about the dummy key schema.
    if (keySchema != null) {
      checkIfKeySchemaMatches(storeName);
    }

    MultiSchemaResponse multiSchemaResponse =
        controllerClient.retryableRequest(DEFAULT_RETRY_TIMES, c -> c.getAllValueSchema(storeName));
    if (multiSchemaResponse.isError()) {
      throw new VeniceException(
          "Error when getting all value schemas from system store " + storeName + " in cluster " + clusterName
              + " after retries. Error: " + multiSchemaResponse.getError());
    }
    Map<Integer, Schema> schemasInZk = new HashMap<>();
    Arrays.stream(multiSchemaResponse.getSchemas())
        .forEach(schema -> schemasInZk.put(schema.getId(), AvroCompatibilityHelper.parse(schema.getSchemaStr())));
    for (int version = 1; version <= protocolDefinition.getCurrentProtocolVersion(); version++) {
      Schema schemaInLocalResources = schemasInLocalResources.get(version);
      if (schemaInLocalResources == null) {
        throw new VeniceException(
            "Invalid protocol definition: " + protocolDefinition.name() + " does not have a version " + version
                + " even though it is less than or equal to the current version ("
                + protocolDefinition.getCurrentProtocolVersion() + ").");
      }
      checkAndMayRegisterValueSchema(storeName, version, schemasInZk.get(version), schemaInLocalResources);

      if (autoRegisterPartialUpdateSchema) {
        checkAndMayRegisterPartialUpdateSchema(storeName, version, schemaInLocalResources);
      }
    }
  }

  private boolean hasLeaderController() {
    try {
      RetryUtils.executeWithMaxAttempt(
          () -> controllerClient.getLeaderControllerUrl(),
          3,
          Duration.ofSeconds(2),
          Collections.singletonList(Exception.class));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void checkAndMayCreateSystemStore(String storeName, Schema firstValueSchema) {
    StoreResponse storeResponse = controllerClient.retryableRequest(
        DEFAULT_RETRY_TIMES,
        c -> c.getStore(storeName),
        r -> ErrorType.STORE_NOT_FOUND.equals(r.getErrorType()));
    if (storeResponse.isError()) {
      if (ErrorType.STORE_NOT_FOUND.equals(storeResponse.getErrorType())) {
        if (firstValueSchema == null) {
          throw new VeniceException("Protocol definition: " + protocolDefinition.name() + " does not have version 1");
        }
        String firstKeySchemaStr = keySchema == null ? DEFAULT_KEY_SCHEMA_STR : keySchema.toString();
        String firstValueSchemaStr = firstValueSchema.toString();
        NewStoreResponse newStoreResponse = controllerClient.retryableRequest(
            DEFAULT_RETRY_TIMES,
            c -> c.createNewSystemStore(
                storeName,
                VeniceConstants.SYSTEM_STORE_OWNER,
                firstKeySchemaStr,
                firstValueSchemaStr),
            r -> r.getError().contains("already exists"));
        if (newStoreResponse.isError() && !newStoreResponse.getError().contains("already exists")) {
          throw new VeniceException(
              "Error when creating system store " + storeName + " in cluster " + clusterName + " after retries. Error: "
                  + newStoreResponse.getError());
        }

        if (storeMetadataUpdate != null) {
          ControllerResponse updateStoreResponse = controllerClient
              .retryableRequest(DEFAULT_RETRY_TIMES, c -> c.updateStore(storeName, storeMetadataUpdate));
          if (updateStoreResponse.isError()) {
            throw new VeniceException(
                "Error when updating system store " + storeName + " in cluster " + clusterName
                    + " after retries. Error: " + updateStoreResponse.getError());
          }
          LOGGER.info("System store {} has been created.", storeName);
        }
      } else {
        throw new VeniceException(
            "Error when getting system store " + storeName + " from cluster " + clusterName + " after retries. Error: "
                + storeResponse.getError());
      }
    }
  }

  private void checkIfKeySchemaMatches(String storeName) {
    SchemaResponse keySchemaResponse =
        controllerClient.retryableRequest(DEFAULT_RETRY_TIMES, c -> c.getKeySchema(storeName));
    if (keySchemaResponse.isError()) {
      throw new VeniceException(
          "Error when getting key schema from system store " + storeName + " in cluster " + clusterName
              + " after retries. Error: " + keySchemaResponse.getError());
    }
    Schema curKeySchema = AvroCompatibilityHelper.parse(keySchemaResponse.getSchemaStr());
    if (!curKeySchema.equals(keySchema)) {
      LOGGER.error(
          "Key Schema of {} in cluster {} is already registered but it is INCONSISTENT with the local definition.\n"
              + "Already registered: {}\n" + "Local definition: {}",
          storeName,
          clusterName,
          curKeySchema.toString(true),
          keySchema.toString(true));
    }
  }

  private void checkAndMayRegisterValueSchema(
      String storeName,
      int valueSchemaId,
      Schema schemaInZk,
      Schema schemaInLocalResources) {
    if (schemaInZk == null) {
      SchemaResponse addValueSchemaResponse = controllerClient.retryableRequest(
          DEFAULT_RETRY_TIMES,
          c -> c.addValueSchema(storeName, schemaInLocalResources.toString(), valueSchemaId));
      if (addValueSchemaResponse.isError()) {
        throw new VeniceException(
            "Error when adding value schema " + valueSchemaId + " to system store " + storeName + " in cluster "
                + clusterName + " after retries. Error: " + addValueSchemaResponse.getError());
      }
      LOGGER.info("Added new schema v{} to system store {}.", valueSchemaId, storeName);
    } else {
      if (schemaInZk.equals(schemaInLocalResources)) {
        LOGGER.info(
            "Schema v{} in system store {} is already registered and consistent with the local definition.",
            valueSchemaId,
            storeName);
      } else {
        LOGGER.warn(
            "Schema v{} in system store {} is already registered but it is INCONSISTENT with the local definition.\n"
                + "Already registered: {}\n" + "Local definition: {}",
            valueSchemaId,
            storeName,
            schemaInZk.toString(true),
            schemaInLocalResources.toString(true));
      }
    }
  }

  private void checkAndMayRegisterPartialUpdateSchema(
      String storeName,
      int valueSchemaId,
      Schema schemaInLocalResources) {
    String partialUpdateSchema =
        WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(schemaInLocalResources).toString();
    SchemaResponse getSchemaResponse = controllerClient.retryableRequest(
        DEFAULT_RETRY_TIMES,
        c -> c.getValueOrDerivedSchemaId(storeName, partialUpdateSchema),
        r -> r.getError().contains("Can not find any registered value schema nor derived schema"));
    if (getSchemaResponse.isError()) {
      if (getSchemaResponse.getError().contains("Can not find any registered value schema nor derived schema")) {
        // The derived schema doesn't exist right now, try to register it.
        SchemaResponse addDerivedSchemaResponse = controllerClient.retryableRequest(
            DEFAULT_RETRY_TIMES,
            c -> c.addDerivedSchema(storeName, valueSchemaId, partialUpdateSchema));
        if (addDerivedSchemaResponse.isError()) {
          throw new VeniceException(
              "Error when adding derived schema for value schema v" + valueSchemaId + " to system store " + storeName
                  + " in cluster " + clusterName + " after retries. Error: " + addDerivedSchemaResponse.getError());
        }
        LOGGER.info(
            "Added derived schema v{} for value schema v{} to system store {}.",
            addDerivedSchemaResponse.getDerivedSchemaId(),
            valueSchemaId,
            storeName);
      } else {
        throw new VeniceException(
            "Error when getting derived schema from system store " + storeName + " in cluster " + clusterName
                + " after retries. Error: " + getSchemaResponse.getError());
      }
    }
  }

  // For testing
  void setControllerClient(ControllerClient controllerClient) {
    this.controllerClient = controllerClient;
  }
}
