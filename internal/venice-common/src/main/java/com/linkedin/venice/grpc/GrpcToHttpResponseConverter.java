package com.linkedin.venice.grpc;

@FunctionalInterface
public interface GrpcToHttpResponseConverter<S, D> {
  D convert(S grpcResponse);
}
