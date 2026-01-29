package com.linkedin.venice.controller.grpc.server;

import com.linkedin.venice.controller.server.SchemaRequestHandler;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.SchemaGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.SchemaGrpcServiceGrpc.SchemaGrpcServiceImplBase;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SchemaGrpcServiceImpl extends SchemaGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(SchemaGrpcServiceImpl.class);
  private final SchemaRequestHandler schemaRequestHandler;

  public SchemaGrpcServiceImpl(SchemaRequestHandler schemaRequestHandler) {
    this.schemaRequestHandler = schemaRequestHandler;
  }

  @Override
  public void getValueSchema(
      GetValueSchemaGrpcRequest request,
      StreamObserver<GetValueSchemaGrpcResponse> responseObserver) {
    LOGGER.debug("Received getValueSchema with args: {}", request);
    // No ACL check on getting store metadata (same as HTTP endpoint)
    ControllerGrpcServerUtils.handleRequest(
        SchemaGrpcServiceGrpc.getGetValueSchemaMethod(),
        () -> schemaRequestHandler.getValueSchema(request),
        responseObserver,
        request.getStoreInfo());
  }
}
