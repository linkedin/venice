package com.linkedin.venice.listener.grpc;

import io.grpc.*;


public class VeniceReadServiceServer {
  public static void main(String[] args) throws Exception {

    int port = 8080;

    Server server = ServerBuilder.forPort(port).addService(new VeniceReadServiceImpl()).build();

    server.start();
    System.out.println("[server] Server started");
    server.awaitTermination();
  }
}
