package com.linkedin.venice.listener.grpc;

public interface VeniceGrpcHandler {
  void setNextInboundHandler(VeniceGrpcHandler nextInboundHandler);

  void setNextOutboundHandler(VeniceGrpcHandler nextOutboundHandler);

  // used for gRPC replacement for ChannelRead
  void grpcRead(GrpcHandlerContext ctx);

  // used for gRPC replacement for ChannelWrite
  void grpcWrite(GrpcHandlerContext ctx);
}
