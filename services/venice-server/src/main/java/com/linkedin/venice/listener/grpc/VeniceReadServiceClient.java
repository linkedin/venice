package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.listener.grpc.interceptors.AuthTokenProvideInterceptor;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.*;


public class VeniceReadServiceClient {
  public static void main(String[] args) {
    final ManagedChannel originChannel = ManagedChannelBuilder.forTarget("localhost:8080").usePlaintext().build();
    ClientInterceptor interceptor = new AuthTokenProvideInterceptor();

    Channel newChannel = ClientInterceptors.intercept(originChannel, interceptor);
    VeniceReadServiceGrpc.VeniceReadServiceBlockingStub stub = VeniceReadServiceGrpc.newBlockingStub(newChannel);
    VeniceClientRequest request = VeniceClientRequest.newBuilder().setKey("this is a key :)").build();

    VeniceServerResponse response = stub.get(request);

    System.out.println("[client] key: " + request.getKey());
    System.out.println("[client] returned val: " + response.getValue());

    originChannel.shutdown();
  }
}
