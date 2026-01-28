package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetAllValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAllValueSchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.SchemaGrpcInfo;
import com.linkedin.venice.schema.SchemaEntry;
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;
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
   * Retrieves all value schemas for a store.
   * @param request the request containing cluster name and store name
   * @return response containing all value schemas and the super set schema ID
   */
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
}
