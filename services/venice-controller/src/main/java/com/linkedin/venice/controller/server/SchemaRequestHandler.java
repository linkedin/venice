package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controllerapi.AllValueSchemaResponse;
import com.linkedin.venice.exceptions.VeniceUnauthorizedAccessException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetAllValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAllValueSchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.SchemaGrpcInfo;
import com.linkedin.venice.schema.SchemaEntry;
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Request handler for schema-related operations.
 * Handles schema retrieval and management for Venice stores.
 */
public class SchemaRequestHandler {
  private static final Logger LOGGER = LogManager.getLogger(SchemaRequestHandler.class);

  private final Admin admin;
  private final VeniceControllerAccessManager accessManager;

  public SchemaRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
    this.accessManager = dependencies.getControllerAccessManager();
  }

  /**
   * Retrieves the value schema for a store by schema ID.
   * @param request the request containing cluster name, store name, and schema ID
   * @return response containing the value schema string
   */
  public GetValueSchemaGrpcResponse getValueSchema(GetValueSchemaGrpcRequest request) {
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    ControllerRequestParamValidator.validateClusterStoreInfo(storeInfo);
    String clusterName = storeInfo.getClusterName();
    String storeName = storeInfo.getStoreName();
    int schemaId = request.getSchemaId();
    LOGGER
        .info("Getting value schema for store: {} in cluster: {} with schema id: {}", storeName, clusterName, schemaId);
    SchemaEntry valueSchemaEntry = admin.getValueSchema(clusterName, storeName, schemaId);
    if (valueSchemaEntry == null) {
      throw new IllegalArgumentException(
          "Value schema for schema id: " + schemaId + " of store: " + storeName + " doesn't exist");
    }
    return GetValueSchemaGrpcResponse.newBuilder()
        .setStoreInfo(storeInfo)
        .setSchemaId(valueSchemaEntry.getId())
        .setSchemaStr(valueSchemaEntry.getSchema().toString())
        .build();
  }

  /**
   * Retrieves all value schemas for a store (v2 architecture).
   * @param clusterName the cluster name
   * @param storeName the store name
   * @param context the request context (contains client identity for ACL check)
   * @return AllValueSchemaResponse with all schemas and super set schema ID
   */
  public AllValueSchemaResponse getAllValueSchema(
      String clusterName,
      String storeName,
      ControllerRequestContext context) {
    // Validate parameters
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required");
    }
    if (StringUtils.isBlank(storeName)) {
      throw new IllegalArgumentException("Store name is required");
    }

    LOGGER.info("Getting all value schemas for store: {} in cluster: {}", storeName, clusterName);

    Collection<SchemaEntry> valueSchemaEntries = admin.getValueSchemas(clusterName, storeName)
        .stream()
        .sorted(Comparator.comparingInt(SchemaEntry::getId))
        .collect(Collectors.toList());

    Store store = admin.getStore(clusterName, storeName);

    // Build POJO response
    AllValueSchemaResponse response = new AllValueSchemaResponse();
    response.setCluster(clusterName);
    response.setName(storeName);
    response.setSchemas(
        valueSchemaEntries.stream()
            .map(entry -> new AllValueSchemaResponse.SchemaInfo(entry.getId(), entry.getSchema().toString()))
            .collect(Collectors.toList()));
    response.setSuperSetSchemaId(store.getLatestSuperSetValueSchemaId());
    return response;
  }

  /**
   * Retrieves all value schemas for a store (v1 architecture - deprecated).
   * @param request the request containing cluster name and store name
   * @return response containing all value schemas and the super set schema ID
   * @deprecated Use {@link #getAllValueSchema(String, String, ControllerRequestContext)} instead
   */
  @Deprecated
  public GetAllValueSchemaGrpcResponse getAllValueSchema(GetAllValueSchemaGrpcRequest request) {
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    ControllerRequestParamValidator.validateClusterStoreInfo(storeInfo);
    String clusterName = storeInfo.getClusterName();
    String storeName = storeInfo.getStoreName();

    LOGGER.info("Getting all value schemas for store: {} in cluster: {}", storeName, clusterName);

    Collection<SchemaEntry> valueSchemaEntries = admin.getValueSchemas(clusterName, storeName)
        .stream()
        .sorted(Comparator.comparingInt(SchemaEntry::getId))
        .collect(Collectors.toList());

    Store store = admin.getStore(clusterName, storeName);

    GetAllValueSchemaGrpcResponse.Builder responseBuilder =
        GetAllValueSchemaGrpcResponse.newBuilder().setStoreInfo(storeInfo);

    for (SchemaEntry entry: valueSchemaEntries) {
      SchemaGrpcInfo schemaInfo =
          SchemaGrpcInfo.newBuilder().setId(entry.getId()).setSchemaStr(entry.getSchema().toString()).build();
      responseBuilder.addSchemas(schemaInfo);
    }

    responseBuilder.setSuperSetSchemaId(store.getLatestSuperSetValueSchemaId());

    return responseBuilder.build();
  }

  /**
   * Centralized ACL check - throws if user is not authorized.
   * @param resourceName the resource name (typically store name)
   * @param context the request context containing client identity
   * @param operation the operation name for error messages
   */
  private void requireAllowListUser(String resourceName, ControllerRequestContext context, String operation) {
    if (accessManager != null
        && !accessManager.isAllowListUser(resourceName, context.getClientCertificate().orElse(null))) {
      throw new VeniceUnauthorizedAccessException(
          "Only admin users are allowed to run " + operation + " on resource: " + resourceName);
    }
  }
}
