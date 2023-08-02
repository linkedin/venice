package com.linkedin.venice.listener.grpc;

public interface VeniceGrpcHandler {
  // used for gRPC replacement for ChannelRead
  void grpcRead(GrpcHandlerContext ctx, GrpcHandlerPipeline pipeline);

  // used for gRPC replacement for ChannelWrite
  void grpcWrite(GrpcHandlerContext ctx, GrpcHandlerPipeline pipeline);
}
