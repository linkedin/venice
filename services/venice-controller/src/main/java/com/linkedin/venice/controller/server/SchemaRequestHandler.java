package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Request handler for schema-related operations.
 * Handles schema retrieval and management for Venice stores.
 */
public class SchemaRequestHandler {
  private static final Logger LOGGER = LogManager.getLogger(SchemaRequestHandler.class);

  private final Admin admin;

  public SchemaRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
  }

  /**
   * Retrieves the value schema for a store by schema ID.
   * No ACL check required - reading schema metadata is public.
   *
   * @param clusterName the cluster name
   * @param storeName the store name
   * @param schemaId the schema ID
   * @return SchemaResponse containing the value schema
   */
  public SchemaResponse getValueSchema(String clusterName, String storeName, int schemaId) {
    LOGGER
        .info("Getting value schema for store: {} in cluster: {} with schema id: {}", storeName, clusterName, schemaId);

    SchemaEntry valueSchemaEntry;
    try {
      valueSchemaEntry = admin.getValueSchema(clusterName, storeName, schemaId);
    } catch (VeniceException e) {
      // Convert VeniceException to IllegalArgumentException for consistent error handling
      LOGGER.warn("Failed to get value schema for store: {} schema id: {}", storeName, schemaId, e);
      throw new IllegalArgumentException(
          "Value schema for schema id: " + schemaId + " of store: " + storeName + " doesn't exist: " + e.getMessage(),
          e);
    }

    if (valueSchemaEntry == null) {
      throw new IllegalArgumentException(
          "Value schema for schema id: " + schemaId + " of store: " + storeName + " doesn't exist");
    }

    SchemaResponse response = new SchemaResponse();
    response.setCluster(clusterName);
    response.setName(storeName);
    response.setId(valueSchemaEntry.getId());
    response.setSchemaStr(valueSchemaEntry.getSchema().toString());
    return response;
  }

  /**
   * Retrieves the key schema for a store.
   * No ACL check required - reading schema metadata is public.
   *
   * @param clusterName the cluster name
   * @param storeName the store name
   * @return SchemaResponse containing the key schema
   */
  public SchemaResponse getKeySchema(String clusterName, String storeName) {
    LOGGER.info("Getting key schema for store: {} in cluster: {}", storeName, clusterName);

    SchemaEntry keySchemaEntry;
    try {
      keySchemaEntry = admin.getKeySchema(clusterName, storeName);
    } catch (VeniceException e) {
      // Convert VeniceException to IllegalArgumentException for consistent error handling
      LOGGER.warn("Failed to get key schema for store: {}", storeName, e);
      throw new IllegalArgumentException("Key schema doesn't exist for store: " + storeName + ": " + e.getMessage(), e);
    }

    if (keySchemaEntry == null) {
      throw new IllegalArgumentException("Key schema doesn't exist for store: " + storeName);
    }

    SchemaResponse response = new SchemaResponse();
    response.setCluster(clusterName);
    response.setName(storeName);
    response.setId(keySchemaEntry.getId());
    response.setSchemaStr(keySchemaEntry.getSchema().toString());
    return response;
  }
}
