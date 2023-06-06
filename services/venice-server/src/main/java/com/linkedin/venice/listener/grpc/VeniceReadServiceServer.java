package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.listener.grpc.interceptors.AuthorizationInterceptor;
import io.grpc.*;


public class VeniceReadServiceServer {
  public static void main(String[] args) throws Exception {

    int port = 8080;

    Server server = Grpc.newServerBuilderForPort(8080, InsecureServerCredentials.create())
        .addService(ServerInterceptors.intercept(new VeniceReadServiceImpl(), new AuthorizationInterceptor()))
        .build()
        .start();

    System.out.println("[server] Server started");
    server.awaitTermination();
  }
}
