package com.linkedin.venice.listener.grpc;

public class GrpcHandlerDelegate implements VeniceGrpcHandler {
  private VeniceGrpcHandler nextInboundHandler;
  private VeniceGrpcHandler nextOutboundHandler;

  @Override
  public void setNextInboundHandler(VeniceGrpcHandler nextInboundHandler) {
    this.nextInboundHandler = nextInboundHandler;
  }

  @Override
  public void setNextOutboundHandler(VeniceGrpcHandler nextOutboundHandler) {
    this.nextOutboundHandler = nextOutboundHandler;
  }

  @Override
  public void grpcRead(GrpcHandlerContext ctx) {
    nextOutboundHandler.grpcWrite(ctx);
  }

  @Override
  public void grpcWrite(GrpcHandlerContext ctx) {

  }
}
