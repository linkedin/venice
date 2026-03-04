package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcRequestProcessor;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceReadServiceImpl.class);

  private final VeniceServerGrpcRequestProcessor requestProcessor;

  public VeniceReadServiceImpl(VeniceServerGrpcRequestProcessor requestProcessor) {
    this.requestProcessor = requestProcessor;
  }

  @Override
  public void get(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    handleRequest(request, responseObserver);
  }

  @Override
  public void batchGet(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    handleRequest(request, responseObserver);
  }

  private void handleRequest(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    VeniceServerResponse.Builder responseBuilder =
        VeniceServerResponse.newBuilder().setErrorCode(VeniceReadResponseStatus.OK.getCode());
    GrpcRequestContext ctx = new GrpcRequestContext(request, responseBuilder, responseObserver);
    requestProcessor.process(ctx);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
