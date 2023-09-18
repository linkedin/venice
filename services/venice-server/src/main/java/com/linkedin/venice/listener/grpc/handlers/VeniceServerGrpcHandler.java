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
    VeniceServerResponse response = ctx.getVeniceServerResponseBuilder().build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

}
