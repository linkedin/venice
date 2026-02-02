package com.linkedin.venice.controller.grpc.server;

import com.linkedin.venice.controller.server.SchemaRequestHandler;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetKeySchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetKeySchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.SchemaGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.SchemaGrpcServiceGrpc.SchemaGrpcServiceImplBase;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * gRPC service implementation for schema operations.
 * Extracts primitives from protobuf, calls handler, and converts POJO response back to protobuf.
 */
public class SchemaGrpcServiceImpl extends SchemaGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(SchemaGrpcServiceImpl.class);
  private final SchemaRequestHandler schemaRequestHandler;

  public SchemaGrpcServiceImpl(SchemaRequestHandler schemaRequestHandler) {
    this.schemaRequestHandler = schemaRequestHandler;
  }

  /**
   * Retrieves the value schema for a store by schema ID.
   * No ACL check is required for this operation as it only reads store metadata.
   */
  @Override
  public void getValueSchema(
      GetValueSchemaGrpcRequest request,
      StreamObserver<GetValueSchemaGrpcResponse> responseObserver) {
    LOGGER.debug("Received getValueSchema with args: {}", request);

    ControllerGrpcServerUtils.handleRequest(SchemaGrpcServiceGrpc.getGetValueSchemaMethod(), () -> {
      // Extract primitives from protobuf
      ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
      String clusterName = storeInfo.getClusterName();
      String storeName = storeInfo.getStoreName();
      int schemaId = request.getSchemaId();

      // Call handler - returns POJO
      SchemaResponse result = schemaRequestHandler.getValueSchema(clusterName, storeName, schemaId);

      // Convert POJO to protobuf response
      return GetValueSchemaGrpcResponse.newBuilder()
          .setStoreInfo(storeInfo)
          .setSchemaId(result.getId())
          .setSchemaStr(result.getSchemaStr())
          .build();
    }, responseObserver, request.getStoreInfo());
  }

  /**
   * Retrieves the key schema for a store.
   * No ACL check is required for this operation as it only reads store metadata.
   */
  @Override
  public void getKeySchema(GetKeySchemaGrpcRequest request, StreamObserver<GetKeySchemaGrpcResponse> responseObserver) {
    LOGGER.debug("Received getKeySchema with args: {}", request);

    ControllerGrpcServerUtils.handleRequest(SchemaGrpcServiceGrpc.getGetKeySchemaMethod(), () -> {
      // Extract primitives from protobuf
      ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
      String clusterName = storeInfo.getClusterName();
      String storeName = storeInfo.getStoreName();

      // Call handler - returns POJO
      SchemaResponse result = schemaRequestHandler.getKeySchema(clusterName, storeName);

      // Convert POJO to protobuf response
      return GetKeySchemaGrpcResponse.newBuilder()
          .setStoreInfo(storeInfo)
          .setSchemaId(result.getId())
          .setSchemaStr(result.getSchemaStr())
          .build();
    }, responseObserver, request.getStoreInfo());
  }

}
