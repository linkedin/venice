package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.grpc.interceptors.AuthTokenProvideInterceptor;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceReadServiceClient {
  private final ManagedChannel originChannel;
  private final static Logger LOGGER = LogManager.getLogger(VeniceReadServiceClient.class);

  private final VeniceReadServiceGrpc.VeniceReadServiceBlockingStub stub;

  public VeniceReadServiceClient(String address) {
    originChannel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    ClientInterceptor interceptor = new AuthTokenProvideInterceptor();
    Channel newChannel = ClientInterceptors.intercept(originChannel, interceptor);
    stub = VeniceReadServiceGrpc.newBlockingStub(newChannel);
  }

  public String get(String key) {
    VeniceClientRequest request = VeniceClientRequest.newBuilder().setKey(key).build();
    VeniceServerResponse response;

    try {
      response = stub.get(request);
    } catch (Exception e) {
      LOGGER.error("Fail to get value for key: {}", key, e);
      throw new VeniceException(e);
    }

    return response.getValue();
  }

  public void shutdown() {
    originChannel.shutdown();
  }
}
