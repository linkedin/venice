package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;


public class VeniceReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  @Override
  public void get(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    System.out.println(request);

    String key = request.getKey();
    String value = key + key;

    VeniceServerResponse response = VeniceServerResponse.newBuilder().setKey(key).setValue(value).build();

    System.out.println("[service] Key: " + key);
    System.out.println("[service] Value: " + value);

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
