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
 * Follows v2 architecture: takes primitives + ControllerRequestContext, returns POJOs.
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
   * @param context the request context (contains client identity)
   * @return SchemaResponse containing the value schema
   */
  public SchemaResponse getValueSchema(
      String clusterName,
      String storeName,
      int schemaId,
      ControllerRequestContext context) {
    LOGGER
        .info("Getting value schema for store: {} in cluster: {} with schema id: {}", storeName, clusterName, schemaId);

    SchemaEntry valueSchemaEntry = admin.getValueSchema(clusterName, storeName, schemaId);
    if (valueSchemaEntry == null) {
      throw new VeniceException(
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
   * @param context the request context (contains client identity)
   * @return SchemaResponse containing the key schema
   */
  public SchemaResponse getKeySchema(String clusterName, String storeName, ControllerRequestContext context) {
    LOGGER.info("Getting key schema for store: {} in cluster: {}", storeName, clusterName);

    SchemaEntry keySchemaEntry = admin.getKeySchema(clusterName, storeName);
    if (keySchemaEntry == null) {
      throw new VeniceException("Key schema doesn't exist for store: " + storeName);
    }

    SchemaResponse response = new SchemaResponse();
    response.setCluster(clusterName);
    response.setName(storeName);
    response.setId(keySchemaEntry.getId());
    response.setSchemaStr(keySchemaEntry.getSchema().toString());
    return response;
  }
}
