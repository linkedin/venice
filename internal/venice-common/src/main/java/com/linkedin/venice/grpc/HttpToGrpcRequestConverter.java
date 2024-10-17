package com.linkedin.venice.grpc;

import com.linkedin.venice.controllerapi.QueryParams;


@FunctionalInterface
public interface HttpToGrpcRequestConverter<D> {
  D convert(QueryParams requestParams);
}
