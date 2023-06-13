package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.grpc.VeniceGrpcServer;
import com.linkedin.venice.grpc.VeniceGrpcServerConfig;
import com.linkedin.venice.listener.grpc.interceptors.AuthorizationInterceptor;
import io.grpc.InsecureServerCredentials;


public class VeniceReadServiceServer extends VeniceGrpcServer {
  public VeniceReadServiceServer(int port) {
    super(
        new VeniceGrpcServerConfig.Builder().setPort(port)
            .setCredentials(InsecureServerCredentials.create())
            .setService(new VeniceReadServiceImpl())
            .setInterceptor(new AuthorizationInterceptor())
            .build());
  }
}
