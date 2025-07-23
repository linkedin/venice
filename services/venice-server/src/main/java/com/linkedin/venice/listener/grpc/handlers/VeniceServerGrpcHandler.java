package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;


public abstract class VeniceServerGrpcHandler {
  protected VeniceServerGrpcHandler next;

  public VeniceServerGrpcHandler() {
    next = null;
  }

  public VeniceServerGrpcHandler getNext() {
    return next;
  }

  public void addNextHandler(VeniceServerGrpcHandler next) {
    this.next = next;
  }

  public abstract void processRequest(GrpcRequestContext ctx);

  public void invokeNextHandler(GrpcRequestContext ctx) {
    if (next != null) {
      next.processRequest(ctx);
    } else {
      writeResponse(ctx);
    }
  }

  public void writeResponse(GrpcRequestContext ctx) {
    StreamObserver<VeniceServerResponse> responseObserver = ctx.getResponseObserver();
    VeniceServerResponse.Builder responseBuilder = ctx.getVeniceServerResponseBuilder();

    // Ensure response has a valid error code
    if (responseBuilder.getErrorCode() == 0) {
      // Set default error code based on whether there's an error
      if (ctx.hasError()) {
        responseBuilder.setErrorCode(com.linkedin.venice.response.VeniceReadResponseStatus.INTERNAL_ERROR);
        if (responseBuilder.getErrorMessage().isEmpty()) {
          responseBuilder.setErrorMessage("Undefined error code");
        }
      } else {
        responseBuilder.setErrorCode(com.linkedin.venice.response.VeniceReadResponseStatus.OK);
      }
    }

    VeniceServerResponse response = responseBuilder.build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

}
