package com.linkedin.venice.grpc;

import com.google.protobuf.GeneratedMessageV3;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.GetStoresInClusterGrpcRequest;
import com.linkedin.venice.protocols.GetStoresInClusterGrpcResponse;
import com.linkedin.venice.protocols.QueryJobStatusGrpcRequest;
import com.linkedin.venice.protocols.QueryJobStatusGrpcResponse;


public enum GrpcControllerRoute {
  CREATE_STORE(CreateStoreGrpcRequest.class, CreateStoreGrpcResponse.class),
  GET_STORES_IN_CLUSTER(GetStoresInClusterGrpcRequest.class, GetStoresInClusterGrpcResponse.class),
  QUERY_JOB_STATUS(QueryJobStatusGrpcRequest.class, QueryJobStatusGrpcResponse.class);

  private final Class<? extends GeneratedMessageV3> requestType;
  private final Class<? extends GeneratedMessageV3> responseType;

  GrpcControllerRoute(Class<? extends GeneratedMessageV3> reqT, Class<? extends GeneratedMessageV3> resT) {
    this.requestType = reqT;
    this.responseType = resT;
  }

  public Class<? extends GeneratedMessageV3> getRequestType() {
    return this.requestType;
  }

  public Class<? extends GeneratedMessageV3> getResponseType() {
    return this.responseType;
  }
}
