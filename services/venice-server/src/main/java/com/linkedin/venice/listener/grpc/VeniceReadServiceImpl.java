package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.listener.HttpChannelInitializer;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceReadServiceImpl.class);

  private final GrpcHandlerPipeline handlerPipeline;

  public VeniceReadServiceImpl(HttpChannelInitializer channelInitializer) {
    this.handlerPipeline = channelInitializer.initGrpcHandlers();
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
    VeniceServerResponse.Builder responseBuilder = VeniceServerResponse.newBuilder().setErrorCode(200);
    GrpcHandlerContext ctx = new GrpcHandlerContext(request, responseBuilder, responseObserver);
    GrpcHandlerPipeline requestPipeline = handlerPipeline.getNewPipeline();

    requestPipeline.processRequest(ctx);
    requestPipeline.processResponse(ctx);

    responseObserver.onNext(ctx.getVeniceServerResponseBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
